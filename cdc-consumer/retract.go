package main

// Retract-by-model: withdraw everything one model version wrote, and prove it by a full scan.
//
// # Why this is the question provenance exists to answer
//
// A model version is found to have been getting something wrong — a price field, a category, a
// unit. The operational question is not "which rows are wrong" (nobody knows) but "which rows did
// *that* model write", and it has to be answerable at the destination, days later, by somebody who
// does not have the source database in front of them and cannot replay a log that has been
// checkpointed since.
//
// It is answerable here because the sink lands the writer with the row. `_model_version` is a
// column of the destination table, so the retraction is one indexed predicate and the ground truth
// is a `SELECT`.
//
// # The two halves, and why both are asserted
//
//   - **100% of the named model's rows.** A retraction that misses rows leaves the bad data in
//     place while reporting success, which is worse than not running it.
//   - **0% of any other model's rows.** A retraction that over-reaches withdraws correct data, and
//     an operator who cannot trust the blast radius will not run it at all.
//
// `scan` exists so a caller can check both from outside this program: it prints one line per row
// with its model version and its retracted flag, and the caller does the arithmetic. A retraction
// that reported its own row count would be grading its own work.
//
// # What is refused rather than reported as success
//
//   - An **empty** `-model-version`. Rows written by no run land a NULL model version, so an empty
//     string would either match nothing or — with a different SQL comparison — match every
//     unattributed row in the table. Neither is what anyone asked for.
//   - A destination with **no `_model_version` column**: landed by a sink that predates attribution.
//     Retracting nothing from it and printing a zero would read exactly like a clean run.
//   - An **engine** this program does not know, the same refusal `openDestination` makes: a typo in
//     `-engine` that quietly opened the wrong file is worse than an error, because the destination
//     the operator is watching stays untouched.
//
// # Both engines, through SQL neither one owns
//
// SQLite and DuckDB both land attribution, so both can be retracted from. The queries here are
// deliberately plain — `SELECT … FROM t LIMIT 0` to ask whether a table or a column exists, rather
// than SQLite's `PRAGMA table_info` or DuckDB's `duckdb_columns()` — so there is one code path
// instead of two that can drift. The two places the engines genuinely differ are named explicitly:
// the driver, and whether `_retracted` is an integer or a real BOOLEAN.
//   - A model version that matches **no rows**. A retraction that touched nothing has not passed:
//     the overwhelmingly likely cause is a typo in the version string, and the error names the
//     versions that are actually present so the next attempt is informed.

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// engine names a destination's driver and the two dialect facts this file needs.
type engine struct {
	driver string
	// The literal that means "retracted" in this engine. SQLite has no boolean type and stores 1;
	// DuckDB has a real BOOLEAN and refuses the integer.
	trueLit string
}

func engineFor(name string) (engine, error) {
	switch name {
	case "sqlite":
		return engine{driver: "sqlite", trueLit: "1"}, nil
	case "duckdb":
		return engine{driver: "duckdb", trueLit: "TRUE"}, nil
	default:
		return engine{}, fmt.Errorf("unknown -engine %q; known engines are sqlite and duckdb", name)
	}
}

// truthy reads a retraction/tombstone flag from either engine.
//
// SQLite hands back an int64 and DuckDB a bool for the same logical column, and a `Scan` into either
// concrete type fails against the other. Scanning into `any` and deciding here is what keeps one
// query working on both.
func truthy(v any) bool {
	switch t := v.(type) {
	case bool:
		return t
	case int64:
		return t != 0
	case int:
		return t != 0
	case []byte:
		return len(t) > 0 && t[0] != '0'
	case string:
		return t != "" && t != "0" && !strings.EqualFold(t, "false")
	case nil:
		return false
	default:
		return false
	}
}

// retractMode decides what a retraction does to the rows it matches.
type retractMode string

const (
	// quarantine marks the rows and leaves them queryable: `_retracted = 1`.
	quarantine retractMode = "quarantine"
	// remove marks them AND tombstones them, so a consumer reading the table the way it reads a
	// soft-deleted row no longer sees them: `_retracted = 1, _deleted = 1`.
	remove retractMode = "delete"
)

// hasColumn reports whether a table has a column of this name, and errors if the TABLE is missing.
//
// Asked with `SELECT … LIMIT 0` rather than with either engine's catalog, so there is one query
// instead of two. The table is probed separately first, because "no such table" and "no such column"
// are different facts and a single failing query cannot tell them apart — and the whole point of
// this file is not misdiagnosing which.
func hasColumn(db *sql.DB, table, column string) (bool, error) {
	// **The column reference is TABLE-QUALIFIED, and that is not style.**
	//
	// SQLite's double-quoted-string misfeature makes a bare `"_model_version"` that resolves to no
	// column fall back to being a string LITERAL — so `SELECT "_model_version" FROM inv` succeeds
	// against a table without the column, returning the text. Measured: the probe reported the
	// column present, and the UPDATE then failed with `no such column: _retracted` — the wrong
	// error, from the wrong place, naming a column nobody had asked about. `inv."_model_version"` is
	// unambiguously a column reference and cannot be reinterpreted, in either engine.
	//
	// `Query`, not `Exec`, for the same reason: `Exec` on a bad SELECT returned nil.
	probe := func(sel string) error {
		rows, err := db.Query(fmt.Sprintf("SELECT %s FROM %s LIMIT 0", sel, quoteIdent(table)))
		if err != nil {
			return err
		}
		err = rows.Err()
		rows.Close()
		return err
	}
	if err := probe("1"); err != nil {
		return false, fmt.Errorf("table %q cannot be read in this destination: %w", table, err)
	}
	if probe(quoteIdent(table)+"."+quoteIdent(column)) != nil {
		return false, nil
	}
	return true, nil
}

// presentModelVersions lists the distinct model versions in a table, for an error message that
// tells the operator what they could have meant.
func presentModelVersions(db *sql.DB, table string) []string {
	rows, err := db.Query(fmt.Sprintf(
		`SELECT DISTINCT "_model_version" FROM %s`, quoteIdent(table)))
	if err != nil {
		return nil
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var v sql.NullString
		if err := rows.Scan(&v); err != nil {
			return out
		}
		if v.Valid {
			out = append(out, v.String)
		} else {
			out = append(out, "<unattributed>")
		}
	}
	sort.Strings(out)
	return out
}

// runRetract withdraws every row one model version wrote.
func runRetract(dbPath, table, modelVersion string, mode retractMode, engineName string) error {
	if strings.TrimSpace(modelVersion) == "" {
		return fmt.Errorf("-model-version is empty. Rows written by no agent run carry a NULL " +
			"model version, so an empty string names either nothing or every unattributed row " +
			"depending on how SQL compares it; refusing rather than picking one")
	}
	if mode != quarantine && mode != remove {
		return fmt.Errorf("unknown -mode %q; known modes are %q and %q", mode, quarantine, remove)
	}
	eng, err := engineFor(engineName)
	if err != nil {
		return err
	}
	db, err := sql.Open(eng.driver, dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	ok, err := hasColumn(db, table, "_model_version")
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("table %q has no _model_version column: it was landed by a sink that "+
			"predates attribution, so which model wrote each row was never recorded and cannot be "+
			"reconstructed from the destination. Re-land the feed with this build", table)
	}

	var total int64
	if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", quoteIdent(table))).Scan(&total); err != nil {
		return err
	}

	set := fmt.Sprintf(`"_retracted" = %s`, eng.trueLit)
	if mode == remove {
		set = fmt.Sprintf(`"_retracted" = %s, "_deleted" = %s`, eng.trueLit, eng.trueLit)
	}
	// The predicate is on the column, parameterised. `_model_version` is compared with `=`, which
	// NULL never satisfies — so unattributed rows are never swept up by a retraction, whatever
	// string is passed.
	res, err := db.Exec(fmt.Sprintf(
		`UPDATE %s SET %s WHERE "_model_version" = ?`, quoteIdent(table), set), modelVersion)
	if err != nil {
		return fmt.Errorf("retract %s from %s: %w", modelVersion, table, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("no row in %q was written by model_version %q, so nothing was retracted. "+
			"A retraction that touched nothing has not succeeded — the versions present are %v",
			table, modelVersion, presentModelVersions(db, table))
	}
	fmt.Printf("RETRACTED %d OF %d table=%s model_version=%s mode=%s engine=%s\n",
		n, total, table, modelVersion, mode, engineName)
	return nil
}

// runScan prints every row's key, model version and retraction flag: the ground truth a caller
// checks a retraction against, produced by a full table scan and not by the code that did the
// retracting.
func runScan(dbPath, table, key, engineName string) error {
	eng, err := engineFor(engineName)
	if err != nil {
		return err
	}
	db, err := sql.Open(eng.driver, dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	ok, err := hasColumn(db, table, "_model_version")
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("table %q has no _model_version column; there is no attribution in this "+
			"destination to scan", table)
	}

	rows, err := db.Query(fmt.Sprintf(
		`SELECT %s, "_prov_id", "_agent", "_model_version", "_retracted", "_deleted" FROM %s ORDER BY %s`,
		quoteIdent(key), quoteIdent(table), quoteIdent(key)))
	if err != nil {
		return err
	}
	defer rows.Close()

	n := 0
	for rows.Next() {
		var k any
		var provID int64
		var agent, mv sql.NullString
		// `any` rather than a concrete type: SQLite returns an integer here and DuckDB a bool, and
		// scanning into either fails against the other.
		var retracted, deleted any
		if err := rows.Scan(&k, &provID, &agent, &mv, &retracted, &deleted); err != nil {
			return err
		}
		version := "<unattributed>"
		if mv.Valid {
			version = mv.String
		}
		who := "<none>"
		if agent.Valid && agent.String != "" {
			who = agent.String
		}
		fmt.Printf("ROW %s=%v prov_id=%d agent=%s model_version=%s retracted=%d deleted=%d\n",
			key, k, provID, who, version, b2i(truthy(retracted)), b2i(truthy(deleted)))
		n++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	if n == 0 {
		// A scan that collected nothing has not passed: an empty destination and a scan pointed at
		// the wrong table look identical from a zero.
		return fmt.Errorf("table %q holds no rows; a scan that returned nothing proves nothing "+
			"about a retraction", table)
	}
	fmt.Printf("SCANNED %d table=%s engine=%s\n", n, table, engineName)
	return nil
}

// b2i renders a flag as 0/1 so the output is one shape whatever the engine stored.
func b2i(b bool) int {
	if b {
		return 1
	}
	return 0
}
