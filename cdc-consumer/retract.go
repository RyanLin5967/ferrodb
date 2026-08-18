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
//   - A model version that matches **no rows**. A retraction that touched nothing has not passed:
//     the overwhelmingly likely cause is a typo in the version string, and the error names the
//     versions that are actually present so the next attempt is informed.

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

// retractMode decides what a retraction does to the rows it matches.
type retractMode string

const (
	// quarantine marks the rows and leaves them queryable: `_retracted = 1`.
	quarantine retractMode = "quarantine"
	// remove marks them AND tombstones them, so a consumer reading the table the way it reads a
	// soft-deleted row no longer sees them: `_retracted = 1, _deleted = 1`.
	remove retractMode = "delete"
)

// hasColumn reports whether a table has a column of this name.
func hasColumn(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(fmt.Sprintf("PRAGMA table_info(%s)", quoteIdent(table)))
	if err != nil {
		return false, err
	}
	defer rows.Close()
	found := false
	any := false
	for rows.Next() {
		var cid int
		var name, typ string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &typ, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		any = true
		if name == column {
			found = true
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	if !any {
		return false, fmt.Errorf("table %q does not exist in this destination", table)
	}
	return found, nil
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
func runRetract(dbPath, table, modelVersion string, mode retractMode) error {
	if strings.TrimSpace(modelVersion) == "" {
		return fmt.Errorf("-model-version is empty. Rows written by no agent run carry a NULL " +
			"model version, so an empty string names either nothing or every unattributed row " +
			"depending on how SQL compares it; refusing rather than picking one")
	}
	if mode != quarantine && mode != remove {
		return fmt.Errorf("unknown -mode %q; known modes are %q and %q", mode, quarantine, remove)
	}
	db, err := sql.Open("sqlite", dbPath)
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

	set := `"_retracted" = 1`
	if mode == remove {
		set = `"_retracted" = 1, "_deleted" = 1`
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
	fmt.Printf("RETRACTED %d OF %d table=%s model_version=%s mode=%s\n",
		n, total, table, modelVersion, mode)
	return nil
}

// runScan prints every row's key, model version and retraction flag: the ground truth a caller
// checks a retraction against, produced by a full table scan and not by the code that did the
// retracting.
func runScan(dbPath, table, key string) error {
	db, err := sql.Open("sqlite", dbPath)
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
		var retracted, deleted int
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
			key, k, provID, who, version, retracted, deleted)
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
	fmt.Printf("SCANNED %d table=%s\n", n, table)
	return nil
}
