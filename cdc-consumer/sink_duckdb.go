package main

// A CDC sink: land the change feed in DuckDB.
//
// This is the same sink as `sink.go`, aimed at an analytical destination instead of SQLite. It is
// not a port for its own sake: SQLite is where an operational replica goes, DuckDB is where the
// analysts' copy goes, and a change feed that can only reach the first of those is half a pipeline.
//
// # What carries over unchanged, and why it has to
//
// The feed is at-least-once. A sink WILL be handed the same event twice, and can be handed a stale
// one after a newer one. So all four properties from the SQLite sink are load-bearing here too:
//
//  1. Every destination row carries `_commit_lsn` — the commit that last wrote it.
//  2. An event applies ONLY if its `commit_lsn` is strictly greater. That test lives in the
//     `ON CONFLICT ... DO UPDATE ... WHERE` clause, not in Go control flow, so every write path
//     inherits it — including one added later by someone who did not read this comment.
//  3. Deletes are SOFT (`_deleted = true`). A hard delete throws away the LSN, and with it the only
//     evidence that would reject a stale re-insert arriving afterwards. The row is gone from the
//     caller's point of view either way; the tombstone is what makes "gone" stick.
//  4. CREATE_TABLE events drive the destination DDL, learned in band and in log order.
//
// # Where DuckDB genuinely differs from SQLite, rather than just spelling things differently
//
//   - **Types are real.** SQLite has storage classes and will put a string in an INTEGER column
//     without complaint; DuckDB will refuse. So the type map below produces DuckDB types, and the
//     inference path used when a CREATE_TABLE has been truncated away infers from the JSON *value*
//     rather than defaulting everything to text — text would be accepted by SQLite and rejected by
//     DuckDB the moment a number arrived.
//   - **The catalog can be asked.** On restart with no CREATE_TABLE in the feed segment, this sink
//     reads the destination's own column list instead of guessing from the first row it sees. A
//     guess that disagrees with the table produces an INSERT naming a column that does not exist.
//   - **cgo.** github.com/marcboeker/go-duckdb links DuckDB statically and requires CGO_ENABLED=1.
//     That is a build-level cost for the whole module, recorded here rather than discovered later.
//
// # Reading the result
//
// `Close` issues an explicit CHECKPOINT so the single database file is self-contained for an
// outside reader — the `duckdb` CLI, say — rather than leaving state in a `.wal` sidecar that a
// second process would have to replay. Verifying a writer with its own reader proves only that it
// agrees with itself.

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb/v2"
)

// DuckSink writes change events into a DuckDB database file.
type DuckSink struct {
	db  *sql.DB
	key string
	// Columns known per table, learned from CREATE_TABLE events, from the destination catalog, or
	// inferred from the first row — in that order of preference.
	columns map[string][]string
}

// bookkeeping names the columns this sink adds to every destination table. They are prefixed so
// they cannot collide with a source column unless the source deliberately chose a leading
// underscore, and they are listed here so the catalog reader can tell them from real ones.
var bookkeeping = map[string]bool{"_commit_lsn": true, "_deleted": true}

func openDuckSink(path, key string) (*DuckSink, error) {
	if key == "" {
		return nil, fmt.Errorf("a sink with no key column cannot upsert; pass -key")
	}
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return nil, err
	}
	// One connection. DuckDB allows several, but the sink is a single serial applier and a pool
	// would let two events for one row race — the SQL guard makes that safe rather than correct,
	// and "safe rather than correct" is not a property worth relying on.
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("open duckdb %s: %w", path, err)
	}
	// The checkpoint travels with the data, in the same database, so a restored backup of the
	// destination resumes from where that backup actually was rather than from wherever a separate
	// state file happens to have got to.
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS _cdc_checkpoint (
		table_name VARCHAR PRIMARY KEY,
		"cursor"   BIGINT NOT NULL
	)`); err != nil {
		db.Close()
		return nil, err
	}
	return &DuckSink{db: db, key: key, columns: map[string][]string{}}, nil
}

// duckTypes is the closed set of types this sink will ever put into DDL.
//
// It is an ALLOWLIST, and that is the point rather than tidiness: a type string travels from the
// feed into a CREATE TABLE, where it cannot be parameterised. A denylist of things that look like
// SQL only catches what someone already thought of; a fixed set of five cannot be talked out of.
// `ensureDuckTable` refuses anything not in here, so the only way to widen it is to widen it here.
var duckTypes = map[string]bool{
	"BIGINT": true, "DOUBLE": true, "VARCHAR": true, "BOOLEAN": true, "TIMESTAMP": true,
}

// duckType maps a feed type onto a DuckDB type.
//
// INTEGER becomes BIGINT deliberately: the feed's integers are JSON numbers with no declared width,
// and narrowing them at the destination would turn a value the source accepted into an error here.
// VARCHAR(n) loses its n, which DuckDB ignores anyway.
func duckType(t string) string {
	switch {
	case t == "INTEGER":
		return "BIGINT"
	case t == "BOOLEAN":
		return "BOOLEAN"
	case t == "FLOAT" || t == "DOUBLE" || t == "REAL":
		return "DOUBLE"
	case strings.HasPrefix(t, "VARCHAR") || t == "TEXT":
		return "VARCHAR"
	default:
		// An unrecognised type must land somewhere storable rather than produce invalid DDL. VARCHAR
		// accepts anything the JSON decoder can hand over as a string.
		return "VARCHAR"
	}
}

// duckTypeOf infers a DuckDB type from a decoded JSON value, for the path where the feed's
// CREATE_TABLE has been truncated away and there is nothing to be told by.
//
// Inference is worse than being told and is recorded as such — but inferring from the value beats
// SQLite's habit of calling everything TEXT, because DuckDB will reject a number written into a
// VARCHAR column rather than quietly storing it.
func duckTypeOf(v any) string {
	switch n := v.(type) {
	case json.Number:
		if _, err := n.Int64(); err == nil {
			return "BIGINT"
		}
		return "DOUBLE"
	case bool:
		return "BOOLEAN"
	case string:
		return "VARCHAR"
	case float64:
		return "DOUBLE"
	case int, int32, int64:
		return "BIGINT"
	default:
		// Including nil: a NULL in the first row says nothing about the column's type.
		return "VARCHAR"
	}
}

// ensureDuckTable creates the destination table from a schema event.
//
// IF NOT EXISTS is load-bearing, not defensive habit: a CREATE_TABLE is re-emitted at every
// checkpoint of the source, because a checkpoint truncates the log and has to re-establish the
// schema at the new base. A sink that treated each one as "a new table appeared" would fail on the
// second checkpoint of every table's life.
//
// The key column is declared PRIMARY KEY, and that is not cosmetic in DuckDB: ON CONFLICT needs an
// index on the conflict target, so without it the ordering guard below has nothing to attach to and
// every write would raise a binder error rather than silently losing the guard.
func (s *DuckSink) ensureDuckTable(table string, cols []map[string]any) error {
	if len(cols) == 0 {
		return fmt.Errorf("cannot create %s with no columns", table)
	}
	names := make([]string, 0, len(cols))
	types := make([]string, 0, len(cols))
	defs := make([]string, 0, len(cols)+2)
	sawKey := false
	for _, c := range cols {
		name := fmt.Sprint(c["name"])
		typ := fmt.Sprint(c["type"])
		if !duckTypes[typ] {
			// Refuse; do not fall through to a default. A type this function cannot vouch for is
			// about to be concatenated into DDL, and guessing is exactly the wrong move there.
			return fmt.Errorf("column %s.%s: type %q is not one this sink will emit", table, name, typ)
		}
		names = append(names, name)
		types = append(types, typ)
		def := quoteIdent(name) + " " + typ
		if name == s.key {
			def += " PRIMARY KEY"
			sawKey = true
		}
		defs = append(defs, def)
	}
	if !sawKey {
		// Refuse rather than warn. A table without the key column has no conflict target, so every
		// upsert against it would be a plain INSERT — the ordering guard would be absent, not
		// degraded, and the destination would corrupt silently on the first replay.
		return fmt.Errorf("table %s has no key column %q; the ordering guard needs one", table, s.key)
	}
	defs = append(defs, `"_commit_lsn" BIGINT NOT NULL`, `"_deleted" BOOLEAN NOT NULL DEFAULT false`)

	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", quoteIdent(table), strings.Join(defs, ", "))
	if _, err := s.db.Exec(stmt); err != nil {
		return fmt.Errorf("create %s: %w", table, err)
	}
	// The table may already have existed with a different shape — an older run's inference, say. Ask
	// the catalog what is actually there rather than assuming the DDL just issued is what took.
	//
	// IF NOT EXISTS makes a disagreement SILENT, and that is the dangerous part. A table first
	// created by inference from a row whose `qty` was null gets VARCHAR; the authoritative
	// CREATE_TABLE saying BIGINT arrives later and does nothing at all, and from then on `qty: 42`
	// is stored as the string "42" — which is precisely the corruption DuckDB's typing is supposed
	// to catch, arriving through the one door that bypasses it. So the declared shape is compared
	// against the catalog and a disagreement is REFUSED rather than warned about: continuing would
	// write wrongly typed data that reads back looking fine.
	actualCols, actualTypes, err := s.catalogSchema(table)
	if err != nil {
		return err
	}
	if len(actualCols) == 0 {
		// The table does not exist even after CREATE — nothing to reconcile against.
		s.columns[table] = names
		return nil
	}
	if err := s.checkSchemaAgrees(table, names, types, actualCols, actualTypes); err != nil {
		return err
	}
	s.columns[table] = actualCols
	return nil
}

// checkSchemaAgrees refuses when the destination table is not the table the event describes.
//
// Re-emission is the COMMON case, not the exception — a CREATE_TABLE is re-sent at every checkpoint
// of the source — so agreement has to stay a silent no-op. Only a genuine difference is an error,
// and the message names the column and both types, because "schema mismatch" alone sends the reader
// to diff two schemas by hand.
func (s *DuckSink) checkSchemaAgrees(table string, want, wantTypes, got, gotTypes []string) error {
	if len(want) != len(got) {
		return fmt.Errorf(
			"table %s already exists with %d column(s) %v but the CREATE_TABLE event declares %d %v; "+
				"refusing to write through a schema that does not match the source",
			table, len(got), got, len(want), want)
	}
	for i := range want {
		if want[i] != got[i] {
			return fmt.Errorf(
				"table %s column %d is %q in the destination but %q in the CREATE_TABLE event; "+
					"refusing to write through a schema that does not match the source",
				table, i, got[i], want[i])
		}
		if !strings.EqualFold(wantTypes[i], gotTypes[i]) {
			return fmt.Errorf(
				"table %s column %q is %s in the destination but the CREATE_TABLE event declares %s; "+
					"a value written through the wrong type reads back looking fine, so this is refused "+
					"rather than warned about",
				table, got[i], gotTypes[i], wantTypes[i])
		}
	}
	return nil
}

// catalogSchema reads a destination table's data columns and their types, excluding this sink's
// bookkeeping ones. An empty result means the table does not exist; that is not an error here, it is
// the question being asked.
func (s *DuckSink) catalogSchema(table string) (names, types []string, err error) {
	rows, err := s.db.Query(
		`SELECT column_name, data_type FROM duckdb_columns() WHERE table_name = ? AND schema_name = 'main'
		 ORDER BY column_index`, table)
	if err != nil {
		return nil, nil, fmt.Errorf("read catalog for %s: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var n, t string
		if err := rows.Scan(&n, &t); err != nil {
			return nil, nil, err
		}
		if !bookkeeping[n] {
			names = append(names, n)
			types = append(types, t)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, err
	}
	return names, types, nil
}

// catalogColumns is catalogSchema when only the names are wanted.
func (s *DuckSink) catalogColumns(table string) ([]string, error) {
	names, _, err := s.catalogSchema(table)
	return names, err
}

// ensureDuckFromRow settles a table's columns when no CREATE_TABLE has been seen this run.
//
// Order of preference, and each step is strictly better evidence than the next: what a schema event
// said this run; what the destination catalog actually holds; what the first row looks like.
func (s *DuckSink) ensureDuckFromRow(table string, row map[string]any) error {
	if _, ok := s.columns[table]; ok {
		return nil
	}
	actual, err := s.catalogColumns(table)
	if err != nil {
		return err
	}
	if len(actual) > 0 {
		s.columns[table] = actual
		return nil
	}
	names := make([]string, 0, len(row))
	for k := range row {
		names = append(names, k)
	}
	sort.Strings(names)
	cols := make([]map[string]any, 0, len(names))
	for _, n := range names {
		cols = append(cols, map[string]any{"name": n, "type": duckTypeOf(row[n])})
	}
	return s.ensureDuckTable(table, cols)
}

// apply writes one event, ignoring it if the destination already holds a newer one.
func (s *DuckSink) apply(e *Event) error {
	switch e.Op {
	case "CREATE_TABLE":
		list, _ := e.After["columns"].([]any)
		cols := make([]map[string]any, 0, len(list))
		for _, c := range list {
			if m, ok := c.(map[string]any); ok {
				cols = append(cols, map[string]any{
					"name": fmt.Sprint(m["name"]),
					"type": duckType(fmt.Sprint(m["type"])),
				})
			}
		}
		return s.ensureDuckTable(e.Table, cols)

	case "DROP_TABLE":
		if _, err := s.db.Exec("DROP TABLE IF EXISTS " + quoteIdent(e.Table)); err != nil {
			return err
		}
		delete(s.columns, e.Table)
		return nil
	}

	row := e.After
	deleted := false
	if e.Op == "DELETE" {
		row = e.Before
		deleted = true
	}
	if row == nil {
		return fmt.Errorf("%s event for %s carries no row", e.Op, e.Table)
	}
	if err := s.ensureDuckFromRow(e.Table, row); err != nil {
		return err
	}
	if _, ok := row[s.key]; !ok {
		// Without the key there is no conflict target, so the row would land as a fresh insert every
		// time it was re-delivered. Say so rather than duplicating it.
		return fmt.Errorf("%s event for %s has no key column %q", e.Op, e.Table, s.key)
	}

	cols := s.columns[e.Table]
	names := make([]string, 0, len(cols)+2)
	placeholders := make([]string, 0, len(cols)+2)
	values := make([]any, 0, len(cols)+2)
	for _, c := range cols {
		names = append(names, quoteIdent(c))
		placeholders = append(placeholders, "?")
		values = append(values, normalise(row[c]))
	}
	names = append(names, `"_commit_lsn"`, `"_deleted"`)
	placeholders = append(placeholders, "?", "?")
	values = append(values, int64(e.CommitLSN), deleted)

	// The ordering guard lives HERE, in the statement, not in Go control flow above it. Every write
	// path that goes through this function inherits it, and there is no path that does not.
	sets := make([]string, 0, len(cols)+2)
	for _, c := range cols {
		if c == s.key {
			// Left out because it is a no-op by construction: the row matched on this column, so it
			// already holds this value. (DuckDB 1.4.1 accepts the assignment — measured, not
			// assumed — so this is about the statement saying what it means, not about being
			// allowed to.) Keeping the SET list to columns that can actually change is what makes
			// the guard readable.
			continue
		}
		sets = append(sets, fmt.Sprintf("%s=excluded.%s", quoteIdent(c), quoteIdent(c)))
	}
	sets = append(sets, `"_commit_lsn"=excluded."_commit_lsn"`, `"_deleted"=excluded."_deleted"`)

	stmt := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)
		 ON CONFLICT(%s) DO UPDATE SET %s
		 WHERE excluded."_commit_lsn" > %s."_commit_lsn"`,
		quoteIdent(e.Table),
		strings.Join(names, ", "),
		strings.Join(placeholders, ", "),
		quoteIdent(s.key),
		strings.Join(sets, ", "),
		quoteIdent(e.Table),
	)
	if _, err := s.db.Exec(stmt, values...); err != nil {
		return fmt.Errorf("apply %s to %s: %w", e.Op, e.Table, err)
	}
	return nil
}

// saveCursor advances a table's resume point, and never moves it backwards — the same strictly-
// greater test as the row guard, in SQL, for the same reason.
func (s *DuckSink) saveCursor(table string, cursor uint64) error {
	_, err := s.db.Exec(
		`INSERT INTO _cdc_checkpoint (table_name, "cursor") VALUES (?, ?)
		 ON CONFLICT(table_name) DO UPDATE SET "cursor"=excluded."cursor"
		 WHERE excluded."cursor" > _cdc_checkpoint."cursor"`,
		table, int64(cursor))
	return err
}

func (s *DuckSink) cursor(table string) uint64 {
	var c int64
	if err := s.db.QueryRow(`SELECT "cursor" FROM _cdc_checkpoint WHERE table_name = ?`, table).
		Scan(&c); err != nil {
		return 0
	}
	return uint64(c)
}

// Close checkpoints before closing, so the database file is complete on its own and an outside
// reader does not have to replay a `.wal` sidecar to see what landed. A checkpoint failure is
// reported rather than swallowed, but the handle is closed either way.
func (s *DuckSink) Close() error {
	_, cerr := s.db.Exec("CHECKPOINT")
	err := s.db.Close()
	if cerr != nil {
		return fmt.Errorf("checkpoint: %w", cerr)
	}
	return err
}

// renderCell formats one value the way the `duckdb` CLI does in `-list` mode.
//
// Byte-for-byte agreement with the CLI is the whole point, and it is not free — three cases differ
// from what Go's default formatting produces, and each was measured against `duckdb -noheader -list`
// (v1.5.5) rather than assumed:
//
//   - NULL prints as the four characters `NULL`, not as the empty string. Getting this wrong is
//     worse than cosmetic: an assertion written against the CLI on one machine then fails on a
//     machine that fell back to this reader, and reports as a DATA disagreement rather than a
//     rendering one — sending the reader after a corruption that is not there.
//   - A DOUBLE holding an integral value prints as `2.0`; `fmt.Sprint(float64(2))` gives `2`.
//   - A TIMESTAMP prints as `2024-01-02 03:04:05`, with fractional seconds only when it has them;
//     Go's `time.Time` stringer appends a zone (`+0000 UTC`) the CLI never shows.
func renderCell(c any) string {
	switch v := c.(type) {
	case nil:
		return "NULL"
	case []byte:
		return string(v)
	case bool:
		if v {
			return "true"
		}
		return "false"
	case float64:
		s := strconv.FormatFloat(v, 'g', -1, 64)
		// `g` drops the point on an integral value; the CLI keeps it. Inf and NaN carry neither a
		// point nor an exponent either, so they are excluded explicitly rather than by luck.
		if !strings.ContainsAny(s, ".eE") && !math.IsInf(v, 0) && !math.IsNaN(v) {
			s += ".0"
		}
		return s
	case time.Time:
		if v.Nanosecond() == 0 {
			return v.Format("2006-01-02 15:04:05")
		}
		// The trailing nines trim insignificant zeros, matching the CLI's `.123` over `.123000`.
		return v.Format("2006-01-02 15:04:05.999999999")
	default:
		return fmt.Sprint(v)
	}
}

// duckSQL runs one statement against a DuckDB file from a separate process and renders any rows the
// way the `duckdb` and `sqlite3` CLIs do in list mode — columns joined by `|`, one row per line,
// NULL as the empty string, booleans as true/false. Byte-for-byte comparable with the CLI, which is
// the point: a test can run both and check they agree.
//
// This exists so a destination can be inspected on a machine with no `duckdb` CLI installed. It is
// a STRICTLY WEAKER check than the CLI and callers must not pretend otherwise — it is the same
// driver and the same linked DuckDB that did the writing, so it shares any misreading either has. A
// writer checked with its own reader agrees with itself.
//
// Read-write, not read-only, and deliberately: it stands in for the CLI, which can also write, and
// a stand-in that silently cannot do half of what it replaces is a worse trap than an obvious gap.
func duckSQL(path, query string) (string, error) {
	db, err := sql.Open("duckdb", path)
	if err != nil {
		return "", err
	}
	db.SetMaxOpenConns(1)
	defer db.Close()
	rows, err := db.Query(query)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	var out []string
	for rows.Next() {
		cells := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return "", err
		}
		parts := make([]string, len(cols))
		for i, c := range cells {
			parts[i] = renderCell(c)
		}
		out = append(out, strings.Join(parts, "|"))
	}
	if err := rows.Err(); err != nil {
		return "", err
	}
	return strings.Join(out, "\n"), nil
}
