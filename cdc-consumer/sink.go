package main

// A CDC sink: land the change feed in SQLite.
//
// A change feed nobody lands anywhere is a demo. This is the shape a real pipeline has — source
// database, change feed, destination table — and it is where the interesting correctness problem
// lives, because writing rows is easy and writing them *idempotently* is not.
//
// # The guarantee the feed gives, and what it forces on a sink
//
// The feed is at-least-once. A consumer that acts on an event and dies before recording its
// position sees that event again; a consumer resuming from a snapshot handoff sees changes the
// snapshot already contained. So a sink WILL be handed the same event twice, and can be handed a
// stale one after a newer one. Applying either naively is not a small bug:
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
	db      *sql.DB
	key     string
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
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS _cdc_checkpoint (
		table_name TEXT PRIMARY KEY,
		cursor     INTEGER NOT NULL
	)`); err != nil {
		return nil, err
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
	names := make([]string, 0, len(cols))
	defs := make([]string, 0, len(cols)+2)
	for _, c := range cols {
		name := fmt.Sprint(c["name"])
		names = append(names, name)
		def := quoteIdent(name) + " " + sqlType(fmt.Sprint(c["type"]))
		if name == s.key {
			def += " PRIMARY KEY"
		}
		defs = append(defs, def)
	}
	// Bookkeeping columns, prefixed so they cannot collide with a source column of the same name
	// without the source having chosen a leading underscore deliberately.
	defs = append(defs, `"_commit_lsn" INTEGER NOT NULL`, `"_deleted" INTEGER NOT NULL DEFAULT 0`)

	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", quoteIdent(table), strings.Join(defs, ", "))
	if _, err := s.db.Exec(stmt); err != nil {
		return fmt.Errorf("create %s: %w", table, err)
	}
	s.columns[table] = names
	return nil
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
		list, _ := e.After["columns"].([]any)
		cols := make([]map[string]any, 0, len(list))
		for _, c := range list {
			if m, ok := c.(map[string]any); ok {
				cols = append(cols, m)
			}
		}
		return s.ensureTable(e.Table, cols)

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
	names = append(names, `"_commit_lsn"`, `"_deleted"`)
	placeholders = append(placeholders, "?", "?")
	values = append(values, int64(e.CommitLSN), deleted)

	// The ordering guard lives HERE, in the statement, not in Go control flow above it.
	sets := make([]string, 0, len(cols)+2)
	for _, c := range cols {
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

func (s *Sink) saveCursor(table string, cursor uint64) error {
	_, err := s.db.Exec(
		`INSERT INTO _cdc_checkpoint (table_name, cursor) VALUES (?, ?)
		 ON CONFLICT(table_name) DO UPDATE SET cursor=excluded.cursor
		 WHERE excluded.cursor > _cdc_checkpoint.cursor`,
		table, int64(cursor))
	return err
}

func (s *Sink) cursor(table string) uint64 {
	var c int64
	if err := s.db.QueryRow(`SELECT cursor FROM _cdc_checkpoint WHERE table_name = ?`, table).
		Scan(&c); err != nil {
		return 0
	}
	return uint64(c)
}

func (s *Sink) Close() error { return s.db.Close() }
