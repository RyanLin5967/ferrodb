package main

// A CDC sink: land the change feed in SQLite.
//
// A change feed nobody lands anywhere is a demo. This is the shape a real pipeline has — source
// database, change feed, destination table — and it is where the interesting correctness problem
// lives, because writing rows is easy and writing them *idempotently* is not.
//
// # The guarantee the feed gives, and what it forces on a sink
//
// The feed is at-least-once, and a sink cannot assume otherwise even though one of the two sources
// of duplication has since been closed:
//
//   - A consumer that acts on an event and dies before recording its position sees that event
//     again on restart. Nothing on the source side can prevent this — it is a property of the
//     consumer's own checkpointing.
//   - A consumer resuming from a snapshot handoff used to see changes the snapshot already
//     contained. The exact cutover (`snapshot_table_exact` with
//     `FeedStreamer::resuming_after_snapshot`) removes that one: the stream skips exactly the
//     transactions the snapshot held. A consumer that cuts over the older way
//     (`snapshot_table`, which brackets the read with LSNs and cannot know) still sees them.
//
// So a sink WILL be handed the same event twice, and can be handed a stale one after a newer one.
// Applying either naively is not a small bug:
//
//   - Re-applying an old UPDATE overwrites current data with a previous value.
//   - Re-applying an INSERT after a DELETE resurrects a row the source no longer has.
//
// Both leave the destination silently wrong and self-consistent, which is the worst failure a
// pipeline can have — nothing downstream can tell.
//
// # The guard, and why it lives in SQL
//
// Every destination row carries `_commit_lsn`: the commit that last wrote it. An event is applied
// only if its `commit_lsn` is strictly greater. That test is written into the `ON CONFLICT ... DO
// UPDATE ... WHERE` clause rather than into this program's control flow, so it holds for every
// path that can ever write the table — including one added later by someone who did not read this
// comment. A guard the caller has to remember to call is a guard that eventually is not called.
//
// Deletes are **soft** (`_deleted = 1`) for the same reason. A hard delete throws away the LSN,
// and with it the only evidence that would let the sink reject a stale re-insert arriving
// afterwards. The row is gone from the caller's point of view either way; keeping the tombstone is
// what makes "gone" stick.

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"

	_ "modernc.org/sqlite"
)

// Sink writes change events into a SQLite database.
type Sink struct {
	db  *sql.DB
	key string
	// Columns known per table, learned from CREATE_TABLE events or inferred from the first row.
	columns map[string][]string
}

func openSink(path, key string) (*Sink, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	// The checkpoint travels with the data, in the same database, so a restored backup of the
	// destination resumes from where that backup actually was rather than from wherever a separate
	// state file happens to have got to.
	// `record_lsn` is the second half of the composite resume point. A commit_lsn identifies a
	// transaction and a transaction carries many rows, so it cannot order siblings within one commit.
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS _cdc_checkpoint (
		table_name TEXT PRIMARY KEY,
		cursor     INTEGER NOT NULL,
		record_lsn INTEGER NOT NULL DEFAULT 0
	)`); err != nil {
		return nil, err
	}
	// Upgrade a destination written before the column existed, rather than failing on it. A sink that
	// refused to open an older destination would strand exactly the backups this checkpoint design
	// exists to make resumable.
	if _, err := db.Exec(`ALTER TABLE _cdc_checkpoint ADD COLUMN record_lsn INTEGER NOT NULL DEFAULT 0`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column name") {
			return nil, err
		}
	}
	return &Sink{db: db, key: key, columns: map[string][]string{}}, nil
}

// sqlType maps a feed type onto a SQLite storage class.
//
// Every type the feed can name is handled explicitly, because the `default` is a fallback for
// types this consumer has never heard of and not a place to quietly park ones it has. BIGINT and
// TIMESTAMP were reaching it and being stored as TEXT, which SQLite compares lexicographically —
// `WHERE big > 5` would sort "10" below "5". SQLite's INTEGER is 8 bytes and holds every i64
// exactly, and INTEGER affinity converts the feed's digit-string losslessly on the way in.
//
// DECIMAL deliberately stays TEXT: it is the one type with no bound on its digits, so REAL would
// round it and INTEGER would refuse its fraction.
func sqlType(t string) string {
	switch {
	case t == "INTEGER" || t == "BOOLEAN" || t == "BIGINT" || t == "TIMESTAMP":
		return "INTEGER"
	case t == "FLOAT":
		return "REAL"
	case t == "DECIMAL":
		return "TEXT"
	case strings.HasPrefix(t, "VARCHAR"):
		return "TEXT"
	default:
		// A type added to the feed that nothing here knows. TEXT stores the bytes unchanged, which
		// is the only choice that cannot corrupt a value it does not understand.
		return "TEXT"
	}
}

// quoteIdent quotes an identifier for SQLite. Doubling embedded quotes is the whole of the escape,
// and it is applied to every identifier rather than only to ones that look suspicious — a column
// named `"; DROP TABLE` is a column name, not an attack, and it should round-trip.
func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

// ensureTable creates the destination table from a schema event.
//
// IF NOT EXISTS is load-bearing, not defensive habit: a CREATE_TABLE is re-emitted at every
// checkpoint of the source, because a checkpoint truncates the log and has to re-establish the
// schema at the new base. A sink that treated each one as "a new table appeared" would fail on the
// second checkpoint of every table's life.
func (s *Sink) ensureTable(table string, cols []map[string]any) error {
	names, ddl := s.tableDDL(table, cols)
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", quoteIdent(table), ddl)
	if _, err := s.db.Exec(stmt); err != nil {
		return fmt.Errorf("create %s: %w", table, err)
	}
	// **A destination BEHIND the declared shape is caught up — B11.** See the DuckDB sink's
	// `catchUpToDeclaredShape` for why this case exists and why only a strict prefix qualifies: a
	// consumer that missed the `ADD_COLUMN` window and resumed after the source truncated its log
	// gets the evolved declaration and nothing else, and `CREATE TABLE IF NOT EXISTS` is a no-op on
	// the table it already has.
	//
	// SQLite has no equivalent of `checkSchemaAgrees`, so a mismatch here has always surfaced later
	// as "no such column" at INSERT time. That is unchanged for every case except this one.
	got, err := s.tableColumns(table)
	if err != nil {
		return err
	}
	if len(got) < len(names) {
		prefix := true
		for i := range got {
			if got[i] != names[i] {
				prefix = false
				break
			}
		}
		if prefix {
			for i := len(got); i < len(names); i++ {
				def := quoteIdent(names[i]) + " " + sqlType(fmt.Sprint(cols[i]["type"]))
				add := "ALTER TABLE " + quoteIdent(table) + " ADD COLUMN " + def
				if _, err := s.db.Exec(add); err != nil {
					if !strings.Contains(err.Error(), "duplicate column name") {
						return fmt.Errorf("catch %s up to the declared shape (%s): %w",
							table, names[i], err)
					}
				}
			}
		}
	}
	s.columns[table] = names
	return nil
}

// tableDDL renders a destination table's column definitions, and the data column names alongside.
//
// One renderer, two callers: `ensureTable` and the retype rebuild in `apply`. SQLite cannot change
// a column's declared type in place, so a retype has to build the table again — and building it
// from a second copy of these definitions is how the rebuilt table quietly loses the PRIMARY KEY or
// a bookkeeping column.
func (s *Sink) tableDDL(table string, cols []map[string]any) (names []string, defs string) {
	out := make([]string, 0, len(cols)+3)
	for _, c := range cols {
		name := fmt.Sprint(c["name"])
		names = append(names, name)
		def := quoteIdent(name) + " " + sqlType(fmt.Sprint(c["type"]))
		if name == s.key {
			def += " PRIMARY KEY"
		}
		out = append(out, def)
	}
	// Bookkeeping columns, prefixed so they cannot collide with a source column of the same name
	// without the source having chosen a leading underscore deliberately.
	out = append(out, `"_commit_lsn" INTEGER NOT NULL`, `"_lsn" INTEGER NOT NULL DEFAULT 0`,
		`"_deleted" INTEGER NOT NULL DEFAULT 0`)
	return names, strings.Join(out, ", ")
}

// eventColumns pulls the full post-change shape out of a schema event.
func eventColumns(e *Event) []map[string]any {
	list, _ := e.After["columns"].([]any)
	cols := make([]map[string]any, 0, len(list))
	for _, c := range list {
		if m, ok := c.(map[string]any); ok {
			cols = append(cols, m)
		}
	}
	return cols
}

// applySchemaChange evolves the destination for a column-level change — B11.
//
// **The destination is altered rather than re-declared.** A sink that reacted to a shape change by
// dropping and recreating the table would land a correct-looking schema and an empty table, which
// is the worst outcome available: self-consistent and wrong, with nothing downstream able to tell.
// So each case issues the narrowest statement that produces the declared shape while keeping every
// row.
func (s *Sink) applySchemaChange(e *Event) error {
	cols := eventColumns(e)
	if len(cols) == 0 {
		return fmt.Errorf("%s for %s carries no shape", e.Op, e.Table)
	}
	// A destination that has never seen this table has nothing to alter; the event's shape is the
	// whole truth, so create it. This is the resume case: a consumer starting mid-feed after the
	// source truncated its log gets the CREATE_TABLE re-declaration with the evolved shape, but a
	// consumer whose first event is the ALTER itself must not fail.
	if _, err := s.db.Exec("SELECT 1 FROM " + quoteIdent(e.Table) + " LIMIT 0"); err != nil {
		return s.ensureTable(e.Table, cols)
	}

	switch e.Op {
	case "ADD_COLUMN":
		name, _ := alterField(e, "column")
		var typ string
		for _, c := range cols {
			if fmt.Sprint(c["name"]) == name {
				typ = sqlType(fmt.Sprint(c["type"]))
			}
		}
		if name == "" || typ == "" {
			return fmt.Errorf("ADD_COLUMN for %s names no column present in its shape", e.Table)
		}
		stmt := "ALTER TABLE " + quoteIdent(e.Table) + " ADD COLUMN " + quoteIdent(name) + " " + typ
		if _, err := s.db.Exec(stmt); err != nil {
			// Already there: the same event re-delivered, or a destination created from the
			// evolved declaration. Not an error — but only this one, so a genuine DDL failure is
			// still a failure.
			if !strings.Contains(err.Error(), "duplicate column name") {
				return fmt.Errorf("add %s.%s: %w", e.Table, name, err)
			}
		}
	case "RENAME_COLUMN":
		from, _ := alterField(e, "from")
		to, _ := alterField(e, "to")
		if from == "" || to == "" {
			return fmt.Errorf("RENAME_COLUMN for %s names no columns", e.Table)
		}
		stmt := "ALTER TABLE " + quoteIdent(e.Table) + " RENAME COLUMN " + quoteIdent(from) +
			" TO " + quoteIdent(to)
		if _, err := s.db.Exec(stmt); err != nil {
			if !strings.Contains(err.Error(), "no such column") {
				return fmt.Errorf("rename %s.%s: %w", e.Table, from, err)
			}
		}
	case "ALTER_COLUMN_TYPE":
		// SQLite has no `ALTER COLUMN ... TYPE`, and this is not a case where "it is dynamically
		// typed so it does not matter" holds: the declared type sets the column's **affinity**, and
		// `sqlType` maps DECIMAL to TEXT while INTEGER and BIGINT both map to INTEGER. A column that
		// should have become TEXT but kept INTEGER affinity coerces its digit strings back to
		// integers on the way in, silently losing every digit past an i64 — the exact loss the
		// source ships decimals as text to prevent.
		//
		// The rebuild is SQLite's own documented recipe, and it is unconditional rather than
		// conditional on the affinity actually moving: a conditional would need a second copy of
		// the type mapping to decide, and two copies of a mapping is how they diverge.
		if err := s.rebuildTable(e.Table, cols); err != nil {
			return err
		}
	}
	// Re-read the shape from the destination rather than trusting the statement above to have
	// produced it: the column list drives every subsequent INSERT, and one built from what was
	// asked for rather than from what is there names a column that may not exist.
	actual, err := s.tableColumns(e.Table)
	if err != nil {
		return err
	}
	s.columns[e.Table] = actual
	return nil
}

// rebuildTable recreates a table with new column definitions, carrying every row across.
//
// SQLite's documented procedure for a change `ALTER TABLE` cannot express. Columns are copied BY
// NAME, so a rebuild for a retype moves the data and a column absent from the new shape would be
// dropped rather than mis-assigned — but this source has no DROP COLUMN, so the name sets are equal
// in practice and a difference would be a bug worth failing on rather than absorbing.
func (s *Sink) rebuildTable(table string, cols []map[string]any) error {
	existing, err := s.tableColumns(table)
	if err != nil {
		return err
	}
	have := map[string]bool{}
	for _, c := range existing {
		have[c] = true
	}
	names, defs := s.tableDDL(table, cols)
	carried := make([]string, 0, len(names)+3)
	for _, n := range names {
		if !have[n] {
			return fmt.Errorf("rebuild %s: the new shape names column %q, which the destination "+
				"does not have; a retype must not invent a column", table, n)
		}
		carried = append(carried, quoteIdent(n))
	}
	carried = append(carried, `"_commit_lsn"`, `"_lsn"`, `"_deleted"`)
	tmp := table + "_cdc_rebuild"

	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	steps := []string{
		fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(tmp)),
		fmt.Sprintf("CREATE TABLE %s (%s)", quoteIdent(tmp), defs),
		fmt.Sprintf("INSERT INTO %s (%s) SELECT %s FROM %s", quoteIdent(tmp),
			strings.Join(carried, ", "), strings.Join(carried, ", "), quoteIdent(table)),
		fmt.Sprintf("DROP TABLE %s", quoteIdent(table)),
		fmt.Sprintf("ALTER TABLE %s RENAME TO %s", quoteIdent(tmp), quoteIdent(table)),
	}
	for _, stmt := range steps {
		if _, err := tx.Exec(stmt); err != nil {
			return fmt.Errorf("rebuild %s: %w", table, err)
		}
	}
	return tx.Commit()
}

// tableColumns asks the destination what a table's data columns actually are, in order.
func (s *Sink) tableColumns(table string) ([]string, error) {
	rows, err := s.db.Query("SELECT name FROM pragma_table_info(?) ORDER BY cid", table)
	if err != nil {
		return nil, fmt.Errorf("read %s columns: %w", table, err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return nil, err
		}
		if n != "_commit_lsn" && n != "_lsn" && n != "_deleted" {
			out = append(out, n)
		}
	}
	return out, rows.Err()
}

// ensureFromRow creates a table from a data row, for a feed whose CREATE_TABLE has been truncated
// away. Types are inferred, which is worse than being told — recorded here so the difference is
// visible rather than silently equivalent.
func (s *Sink) ensureFromRow(table string, row map[string]any) error {
	if _, ok := s.columns[table]; ok {
		return nil
	}
	names := make([]string, 0, len(row))
	for k := range row {
		names = append(names, k)
	}
	sort.Strings(names)
	cols := make([]map[string]any, 0, len(names))
	for _, n := range names {
		cols = append(cols, map[string]any{"name": n, "type": "TEXT"})
	}
	return s.ensureTable(table, cols)
}

// apply writes one event, ignoring it if the destination already holds a newer one.
func (s *Sink) apply(e *Event) error {
	switch e.Op {
	case "CREATE_TABLE":
		return s.ensureTable(e.Table, eventColumns(e))

	case "ADD_COLUMN", "RENAME_COLUMN", "ALTER_COLUMN_TYPE":
		return s.applySchemaChange(e)

	case "DROP_TABLE":
		if _, err := s.db.Exec("DROP TABLE IF EXISTS " + quoteIdent(e.Table)); err != nil {
			return err
		}
		delete(s.columns, e.Table)
		return nil
	}

	row := e.After
	deleted := 0
	if e.Op == "DELETE" {
		row = e.Before
		deleted = 1
	}
	if row == nil {
		return fmt.Errorf("%s event for %s carries no row", e.Op, e.Table)
	}
	if err := s.ensureFromRow(e.Table, row); err != nil {
		return err
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
	names = append(names, `"_commit_lsn"`, `"_lsn"`, `"_deleted"`)
	placeholders = append(placeholders, "?", "?", "?")
	values = append(values, int64(e.CommitLSN), int64(e.LSN), deleted)

	// The ordering guard lives HERE, in the statement, not in Go control flow above it.
	sets := make([]string, 0, len(cols)+2)
	for _, c := range cols {
		sets = append(sets, fmt.Sprintf("%s=excluded.%s", quoteIdent(c), quoteIdent(c)))
	}
	sets = append(sets, `"_commit_lsn"=excluded."_commit_lsn"`, `"_lsn"=excluded."_lsn"`,
		`"_deleted"=excluded."_deleted"`)

	stmt := fmt.Sprintf(
		`INSERT INTO %s (%s) VALUES (%s)
		 ON CONFLICT(%s) DO UPDATE SET %s
		 WHERE excluded."_commit_lsn" > %s."_commit_lsn"
		    OR (excluded."_commit_lsn" = %s."_commit_lsn" AND excluded."_lsn" > %s."_lsn")`,
		quoteIdent(e.Table),
		strings.Join(names, ", "),
		strings.Join(placeholders, ", "),
		quoteIdent(s.key),
		strings.Join(sets, ", "),
		quoteIdent(e.Table),
		quoteIdent(e.Table),
		quoteIdent(e.Table),
	)
	if _, err := s.db.Exec(stmt, values...); err != nil {
		return fmt.Errorf("apply %s to %s: %w", e.Op, e.Table, err)
	}
	return nil
}

// normalise turns json.Number into something SQLite stores as a number rather than as text.
func normalise(v any) any {
	type numberish interface{ Int64() (int64, error) }
	if n, ok := v.(numberish); ok {
		if i, err := n.Int64(); err == nil {
			return i
		}
	}
	if n, ok := v.(interface{ Float64() (float64, error) }); ok {
		if f, err := n.Float64(); err == nil {
			return f
		}
	}
	return v
}

// saveCursor advances a table's resume point and never moves it backwards, comparing the composite
// (commit_lsn, record_lsn) lexicographically in the statement rather than in Go.
func (s *Sink) saveCursor(table string, commitLSN, recordLSN uint64) error {
	_, err := s.db.Exec(
		`INSERT INTO _cdc_checkpoint (table_name, cursor, record_lsn) VALUES (?, ?, ?)
		 ON CONFLICT(table_name) DO UPDATE SET cursor=excluded.cursor, record_lsn=excluded.record_lsn
		 WHERE excluded.cursor > _cdc_checkpoint.cursor
		    OR (excluded.cursor = _cdc_checkpoint.cursor
		        AND excluded.record_lsn > _cdc_checkpoint.record_lsn)`,
		table, int64(commitLSN), int64(recordLSN))
	return err
}

func (s *Sink) cursor(table string) (uint64, uint64) {
	var c, r int64
	if err := s.db.QueryRow(
		`SELECT cursor, record_lsn FROM _cdc_checkpoint WHERE table_name = ?`, table).
		Scan(&c, &r); err != nil {
		return 0, 0
	}
	return uint64(c), uint64(r)
}

func (s *Sink) Close() error { return s.db.Close() }
