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
//
// # Why the writer is landed with the row
//
// Every destination row also carries the run that wrote it: `_prov_id`, `_agent`, `_run`,
// `_model`, `_model_version`. That is what makes `retract` possible at all — "withdraw everything
// model_version 2026-07 wrote" is answerable by a scan of the destination, with no access to the
// source database, no replay of the feed, and no join against a log a checkpoint may have truncated
// away. A pipeline that has to go back to the producer to answer it cannot answer it after the
// producer is gone, which is exactly when the question gets asked.
//
// The prompt is landed as a digest (`_prompt_sha256`) and never as text; see `writerKeys` in
// main.go for the allowlist that enforces that upstream of here.

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
	defs = append(defs, `"_commit_lsn" INTEGER NOT NULL`, `"_lsn" INTEGER NOT NULL DEFAULT 0`,
		`"_deleted" INTEGER NOT NULL DEFAULT 0`)
	for _, w := range writerColumns {
		defs = append(defs, quoteIdent(w.name)+" "+w.decl)
	}

	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)", quoteIdent(table), strings.Join(defs, ", "))
	if _, err := s.db.Exec(stmt); err != nil {
		return fmt.Errorf("create %s: %w", table, err)
	}
	if err := s.ensureWriterColumns(table); err != nil {
		return err
	}
	s.columns[table] = names
	return nil
}

// writerColumns are the attribution columns every destination table carries.
//
// Declared in one place because they are added by two paths — CREATE TABLE for a new destination,
// ALTER TABLE for one an older sink already made — and two hand-written lists is one place for them
// to drift apart, which would show up as a retraction that silently matches nothing.
var writerColumns = []struct{ name, decl string }{
	{"_prov_id", "INTEGER NOT NULL DEFAULT 0"},
	{"_agent", "TEXT"},
	{"_run", "TEXT"},
	{"_model", "TEXT"},
	{"_model_version", "TEXT"},
	{"_prompt_sha256", "TEXT"},
	// Set by `retract`, never by the feed. Kept separate from `_deleted` so an operator can tell a
	// row the SOURCE deleted from one this consumer withdrew.
	{"_retracted", "INTEGER NOT NULL DEFAULT 0"},
}

// ensureWriterColumns upgrades a destination written before attribution existed.
//
// Refusing to open an older destination would strand exactly the backups the checkpoint design
// exists to make resumable, so the columns are added in place. SQLite has no `ADD COLUMN IF NOT
// EXISTS`, so a duplicate is the expected outcome on every run after the first and is the only
// error swallowed here.
func (s *Sink) ensureWriterColumns(table string) error {
	for _, w := range writerColumns {
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
			quoteIdent(table), quoteIdent(w.name), w.decl)
		if _, err := s.db.Exec(stmt); err != nil && !strings.Contains(err.Error(), "duplicate column name") {
			return fmt.Errorf("add %s to %s: %w", w.name, table, err)
		}
	}
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
	names = append(names, `"_commit_lsn"`, `"_lsn"`, `"_deleted"`)
	placeholders = append(placeholders, "?", "?", "?")
	values = append(values, int64(e.CommitLSN), int64(e.LSN), deleted)

	// The writer, landed with the row. A change no run produced lands NULLs and prov_id 0, which is
	// the unattributed slot — distinct from a run whose model_version is the empty string, and
	// `retract` refuses an empty model version precisely so the two cannot be conflated.
	var provID int64
	var agent, run, model, modelVersion, promptSHA any
	if e.Writer != nil {
		provID = int64(e.Writer.ProvID)
		agent, run = e.Writer.Agent, e.Writer.Run
		model, modelVersion = e.Writer.Model, e.Writer.ModelVersion
		promptSHA = e.Writer.PromptSHA256
	}
	// A newly applied event replaces the row AND its attribution, so `_retracted` returns to 0: the
	// mark described the row as it stood, and this is a different row now. A re-delivery of an
	// already-retracted event does not reach here — the ordering guard below rejects it — so a
	// retraction is not undone by a replay.
	names = append(names, `"_prov_id"`, `"_agent"`, `"_run"`, `"_model"`, `"_model_version"`,
		`"_prompt_sha256"`, `"_retracted"`)
	placeholders = append(placeholders, "?", "?", "?", "?", "?", "?", "?")
	values = append(values, provID, agent, run, model, modelVersion, promptSHA, 0)

	// The ordering guard lives HERE, in the statement, not in Go control flow above it.
	sets := make([]string, 0, len(cols)+3+len(writerColumns))
	for _, c := range cols {
		sets = append(sets, fmt.Sprintf("%s=excluded.%s", quoteIdent(c), quoteIdent(c)))
	}
	sets = append(sets, `"_commit_lsn"=excluded."_commit_lsn"`, `"_lsn"=excluded."_lsn"`,
		`"_deleted"=excluded."_deleted"`)
	for _, w := range writerColumns {
		sets = append(sets, fmt.Sprintf("%s=excluded.%s", quoteIdent(w.name), quoteIdent(w.name)))
	}

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
