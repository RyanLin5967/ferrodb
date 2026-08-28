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
	// The key column's CURRENT name per table, read from the destination. See `keyFor`.
	keys map[string]string
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
	return &Sink{db: db, key: key, columns: map[string][]string{}, keys: map[string]string{}}, nil
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

// sqliteAffinity is the storage class a DECLARED type resolves to, by SQLite's own rules.
//
// **Affinity, not spelling — I20.** A first cut of the type check compared the declared strings and
// refused a destination declared `VARCHAR(20)` against an event mapping to `TEXT`, which are the
// same column: both have TEXT affinity, and nothing is converted going in or out. The declared
// string is a label; the affinity is the behaviour, and the behaviour is what finding 4 is about.
//
// The rules are SQLite's documented five, in order, and the order matters: `VARCHAR` contains
// neither "INT" nor "BLOB" but does contain "CHAR", and a rule set applied in any other sequence
// gets it wrong.
func sqliteAffinity(declared string) string {
	d := strings.ToUpper(strings.TrimSpace(declared))
	switch {
	case strings.Contains(d, "INT"):
		return "INTEGER"
	case strings.Contains(d, "CHAR"), strings.Contains(d, "CLOB"), strings.Contains(d, "TEXT"):
		return "TEXT"
	case d == "" || strings.Contains(d, "BLOB"):
		return "BLOB"
	case strings.Contains(d, "REAL"), strings.Contains(d, "FLOA"), strings.Contains(d, "DOUB"):
		return "REAL"
	default:
		return "NUMERIC"
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
//
// `declared` says where `cols` came from, and it decides whether the type-agreement check below
// runs at all. A CREATE_TABLE or an ALTER carries the source's own declaration and is authoritative;
// `ensureFromRow` INFERS types from a row's JSON and marks every column TEXT. Checking a guess
// against a destination built from a declaration would refuse a correct destination for disagreeing
// with a fallback — measured: `TestARetractionSurvivesAReplayButNotAFreshWrite` reopens a sink and
// applies a bare UPDATE, which takes the inference path over an INTEGER destination.
func (s *Sink) ensureTable(table string, cols []map[string]any, declared bool) error {
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
	got, gotTypes, err := s.tableShape(table)
	if err != nil {
		return err
	}
	// The declared shape in SQLite's own spelling, so the comparison below is like for like: the
	// feed says DECIMAL, the destination says TEXT, and only `sqlType` knows those are the same
	// column. Built once and reused by the catch-up and the agreement check.
	wantTypes := make([]string, len(names))
	for i := range names {
		wantTypes[i] = sqlType(fmt.Sprint(cols[i]["type"]))
	}
	if grown, err := s.catchUpToDeclaredShape(table, names, wantTypes, got, gotTypes, declared); err != nil {
		return err
	} else if grown {
		got, gotTypes, err = s.tableShape(table)
		if err != nil {
			return err
		}
	}
	if declared {
		if err := s.checkSchemaAgrees(table, names, wantTypes, got, gotTypes); err != nil {
			return err
		}
	}
	// B5's attribution columns, on the path an OLDER destination takes. The merge dropped this call
	// once: `ensureWriterColumns` was defined and never invoked, which compiles, passes every test
	// that does not read an attribution column, and silently ships a destination with no writer
	// columns at all.
	if err := s.ensureWriterColumns(table); err != nil {
		return err
	}
	s.columns[table] = names
	return nil
}

// catchUpToDeclaredShape adds columns a declaration has and the destination does not, and reports
// whether it added any.
//
// The SQLite half of what `DuckSink.catchUpToDeclaredShape` does, and now to the same rule. It was
// written a second time inside `ensureTable` as `if got[i] != names[i]` — **names only** — which is
// review finding 4: the prefix test the safety argument rests on was only half performed on this
// side, so a retype rode in under a matching name and the column kept an affinity that silently
// converted its values.
//
// Refuses to do anything unless the destination is a strict PREFIX of the declaration: every column
// it already has must be the declared one at that ordinal, **by name and by declared type**.
// Anything else is left for `checkSchemaAgrees` to refuse. A shape diff cannot tell a rename from a
// drop-plus-add, and guessing there loses the column's data.
func (s *Sink) catchUpToDeclaredShape(table string, want, wantTypes, got, gotTypes []string, declared bool) (bool, error) {
	if len(got) >= len(want) {
		return false, nil
	}
	for i := range got {
		if got[i] != want[i] {
			return false, nil
		}
		// **Only a DECLARATION's types are worth comparing.** `ensureFromRow` infers every column
		// as TEXT from a row's JSON, and comparing a guess against a destination built from the
		// source's own CREATE_TABLE refuses on every column — which would strand exactly the
		// consumer the catch-up exists for: one that resumed after its CREATE_TABLE was truncated
		// away and now sees a row carrying a column the source added. Measured before this
		// exemption: `table inv has no column named zz`.
		if declared && sqliteAffinity(gotTypes[i]) != sqliteAffinity(wantTypes[i]) {
			return false, nil
		}
	}
	for i := len(got); i < len(want); i++ {
		add := "ALTER TABLE " + quoteIdent(table) + " ADD COLUMN " +
			quoteIdent(want[i]) + " " + wantTypes[i]
		if _, err := s.db.Exec(add); err != nil {
			if !strings.Contains(err.Error(), "duplicate column name") {
				return false, fmt.Errorf("catch %s up to the declared shape (%s): %w",
					table, want[i], err)
			}
		}
	}
	return true, nil
}

// checkSchemaAgrees refuses when the destination table is not the table the event describes.
//
// SQLite had no equivalent of the DuckDB sink's check, so a disagreement surfaced later as
// "no such column" at INSERT time — or, for a type, as nothing at all, because SQLite will happily
// store a value through the wrong affinity and read it back looking fine. That is exactly the loss
// `applySchemaChange`'s rebuild recipe exists to prevent, arriving through the one door that
// bypassed it.
//
// Re-emission is the COMMON case, not the exception — a CREATE_TABLE is re-sent at every checkpoint
// of the source — so agreement stays a silent no-op. Only a genuine difference is an error, and the
// message names the column and both types, because "schema mismatch" alone sends the reader to diff
// two schemas by hand.
func (s *Sink) checkSchemaAgrees(table string, want, wantTypes, got, gotTypes []string) error {
	// **Deliberately narrower than the DuckDB sink's check of the same name, and each exclusion is
	// a case the suite proved legitimate rather than a case nobody thought about.**
	//
	// NOT a column-count check. A destination AHEAD of the declaration is the ordinary outcome of
	// replaying any feed that contains an ADD_COLUMN: the second pass re-delivers the original
	// CREATE_TABLE, which declares fewer columns than the destination now has, and refusing there
	// would break every replay (`TestReplayingAFeedWithASchemaChangeIsANoOp`). A destination BEHIND
	// the declaration has already been offered the catch-up above; if it declined, the missing
	// column surfaces at INSERT time, loudly.
	//
	// NOT a name check. A name differing at an ordinal is a RENAME the source performed and this
	// consumer's log was truncated past — review finding 8, still open, still surfacing as
	// "no such column" at INSERT. Refusing here would change that failure's shape without giving it
	// a way forward, which is not this fix's job.
	//
	// A TYPE difference at an ordinal whose NAME agrees is the one case with no other detector, and
	// it is review finding 4: the value is stored through the wrong affinity, converted on the way
	// in, and reads back looking fine.
	n := len(want)
	if len(got) < n {
		n = len(got)
	}
	for i := 0; i < n; i++ {
		if want[i] != got[i] {
			// **A diagnosis, not a repair — I20, review finding 8.**
			//
			// Equal column counts with a name differing at one ordinal is the signature of a
			// RENAME the source performed and this consumer's log was truncated past: the alter
			// record is destroyed by the next whole-table DDL anywhere in the database, and what
			// arrives is the evolved declaration with no way to tell it from a different table of
			// the same name (review finding 9, still open — which is exactly why this refuses
			// rather than renaming the column itself).
			//
			// The outcome is the same stall it has always been. What changes is that the operator
			// is told the cause instead of meeting `table inv has no column named quantity` at
			// INSERT time, which points at the feed rather than at the destination.
			if len(want) == len(got) {
				return fmt.Errorf(
					"table %s column %d is %q in the destination but %q in the event's shape, and the "+
						"shapes are otherwise the same size. That is what a RENAME COLUMN on the source "+
						"looks like once a checkpoint has truncated the alter record away. This sink will "+
						"not rename the column on a shape diff alone: a rename and a drop-plus-recreate of "+
						"a table of the same name are indistinguishable here, and guessing wrong keeps a "+
						"dead table's rows and presents them as live. Rename %q to %q in the destination "+
						"by hand, or drop the table and let the feed rebuild it",
					table, i, got[i], want[i], got[i], want[i])
			}
			continue
		}
		// **One direction only, and the set of retypes this source can perform is what makes that
		// exact rather than a guess.** `Widening::of` in `catalog/alter.rs` is a closed allowlist:
		// Integer->BigInt, Integer->Decimal, BigInt->Decimal, Varchar(n)->Varchar(m>=n). Mapped
		// through `sqlType`, the first and last change no affinity at all; the only affinity a
		// retype can move is INTEGER -> TEXT.
		//
		// So a destination with INTEGER affinity under a declaration with TEXT affinity is a retype
		// the destination has NOT applied — finding 4's silent conversion, refused. Every other
		// difference is the destination being AHEAD of the declaration or a shape that never came
		// from a declaration at all, and refusing those strands a consumer over nothing:
		//   * a destination already retyped to TEXT, re-handed the ORIGINAL CREATE_TABLE on a
		//     replay. `bypassesCursor` makes a declaration re-run on every replay, so this is the
		//     ordinary case, not an edge one.
		//   * a destination built by `ensureFromRow` inference (all TEXT), later handed the
		//     source's real declaration.
		// Both were refused by a first cut of this check and are now allowed, deliberately.
		if sqliteAffinity(gotTypes[i]) == "INTEGER" && sqliteAffinity(wantTypes[i]) == "TEXT" {
			return fmt.Errorf(
				"table %s column %q is declared %s in the destination but the event's shape makes it "+
					"%s; SQLite's declared type sets the column's affinity, so a value written through "+
					"the wrong one is converted on the way in and reads back looking fine — a DECIMAL "+
					"landing in an INTEGER column loses every digit past an i64. That is a retype this "+
					"destination has not applied. Refused rather than warned about: rebuild the table, "+
					"or drop it and let the feed recreate it",
				table, got[i], gotTypes[i], wantTypes[i])
		}
	}
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
		// `keyFor`, not the flag — I20. A rebuild (the retype recipe) re-declares the table from
		// these definitions, and one built from the flag after the source renamed the key column
		// would quietly drop the PRIMARY KEY and leave every later upsert without a conflict target.
		if name == s.keyFor(table) {
			def += " PRIMARY KEY"
		}
		out = append(out, def)
	}
	// Bookkeeping columns, prefixed so they cannot collide with a source column of the same name
	// without the source having chosen a leading underscore deliberately.
	out = append(out, `"_commit_lsn" INTEGER NOT NULL`, `"_lsn" INTEGER NOT NULL DEFAULT 0`,
		`"_deleted" INTEGER NOT NULL DEFAULT 0`)
	for _, w := range writerColumns {
		out = append(out, quoteIdent(w.name)+" "+w.decl)
	}
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
		return s.ensureTable(e.Table, cols, true)
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
	//
	// I20: the cached key name goes with it. A RENAME_COLUMN may have moved the key, and a retype
	// rebuild re-declares the table; either way what is cached describes the table as it was.
	s.forgetKey(e.Table)
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
	names, _, err := s.tableShape(table)
	return names, err
}

// tableShape asks the destination for its data columns AND their declared types, in order.
//
// **The types are the point — I20, review finding 4.** `tableColumns` read `SELECT name` and the
// catch-up below compared names only, while the DuckDB sink's `catchUpToDeclaredShape` compared
// both. The comment on that function and this consumer's own report both said the catch-up fires
// on "same names, SAME TYPES, in order"; on this side the second half was never written. A source
// that retyped `qty` to DECIMAL and added `note`, then checkpointed both ALTERs away, handed the
// destination a declaration whose NAMES still prefix-matched — so `note` was appended, `qty` kept
// its INTEGER affinity, and a 39-digit decimal arriving as a JSON string was coerced to a float:
// 1.7014118346046923e+38, storage class `real`, from a run printing `applied 2, skipped 0` and
// exiting 0.
//
// The declared type is what sets a SQLite column's affinity, so it is the thing that decides
// whether a value is stored as given or converted. Reading it is the whole fix.
func (s *Sink) tableShape(table string) (names []string, types []string, err error) {
	rows, err := s.db.Query("SELECT name, type FROM pragma_table_info(?) ORDER BY cid", table)
	if err != nil {
		return nil, nil, fmt.Errorf("read %s columns: %w", table, err)
	}
	defer rows.Close()
	for rows.Next() {
		var n, ty string
		if err := rows.Scan(&n, &ty); err != nil {
			return nil, nil, err
		}
		// **Bookkeeping columns are not data columns**, and the list has to include B5's
		// attribution columns as well as the three original ones. This filter was written when
		// `_commit_lsn`, `_lsn` and `_deleted` were the only ones there; B5 then added seven
		// attribution columns to every destination, and B11's catch-up path re-reads the shape
		// FROM the destination - so those seven came back as data columns and the upsert named each
		// of them twice, once with a NULL. Measured: `NOT NULL constraint failed: inv._prov_id`,
		// from a statement listing `"_prov_id"` in both halves.
		//
		// Driven off `writerColumns` rather than a second hand-written list, which is the same
		// reason that declaration exists: two copies are one place to drift, and the drift shows up
		// as a retraction that silently matches nothing.
		if n == "_commit_lsn" || n == "_lsn" || n == "_deleted" {
			continue
		}
		bookkeeping := false
		for _, w := range writerColumns {
			if n == w.name {
				bookkeeping = true
				break
			}
		}
		if !bookkeeping {
			names = append(names, n)
			types = append(types, ty)
		}
	}
	return names, types, rows.Err()
}

// keyFor is the name the key column carries in `table` RIGHT NOW — I20, review finding 10.
//
// The `-key` flag names the key column as it is called when the consumer STARTS. It can move: the
// source permits renaming the primary key (only RETYPING it is refused, `catalog/alter.rs`), and a
// rename keeps the column at ordinal 0, which is what still makes it the primary key. Both sinks
// pinned `s.key` to the flag for ever, so after such a rename every upsert died on
// `ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint` — and `runSink` returns
// on the first error, so every restart died at the same record.
//
// Read from the DESTINATION rather than tracked in memory, which is what makes it survive a
// restart: the RENAME_COLUMN event is ordinary positioned traffic, so a resume whose cursor is past
// it never sees it again. SQLite carries the PRIMARY KEY onto the new name across
// `ALTER TABLE ... RENAME COLUMN` — measured, not assumed: `pragma_table_info` reports `pk=1` on
// the renamed column and an upsert against it succeeds. So the destination's own catalog is the
// truth about what `ON CONFLICT` will accept, and the flag is only the fallback for a table that
// does not exist yet.
func (s *Sink) keyFor(table string) string {
	if n, ok := s.keys[table]; ok {
		return n
	}
	rows, err := s.db.Query("SELECT name FROM pragma_table_info(?) WHERE pk != 0 ORDER BY pk", table)
	if err != nil {
		return s.key
	}
	defer rows.Close()
	for rows.Next() {
		var n string
		if err := rows.Scan(&n); err != nil {
			return s.key
		}
		// The first PK column. This source's primary key is always a single column at ordinal 0
		// (`catalog.rs`: "first column = primary key"), so there is no composite case to get wrong.
		s.keys[table] = n
		return n
	}
	return s.key
}

// forgetKey drops a cached key name. Called wherever the destination's shape changes underneath
// the cache — a rename moves it, a rebuild re-declares it, and a DROP takes the table with it.
func (s *Sink) forgetKey(table string) { delete(s.keys, table) }

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
	// `false`: these types are guesses. See `ensureTable`.
	return s.ensureTable(table, cols, false)
}

// apply writes one event, ignoring it if the destination already holds a newer one.
func (s *Sink) apply(e *Event) error {
	switch e.Op {
	case "CREATE_TABLE":
		return s.ensureTable(e.Table, eventColumns(e), true)

	case "ADD_COLUMN", "RENAME_COLUMN", "ALTER_COLUMN_TYPE":
		return s.applySchemaChange(e)

	case "DROP_TABLE":
		if _, err := s.db.Exec("DROP TABLE IF EXISTS " + quoteIdent(e.Table)); err != nil {
			return err
		}
		delete(s.columns, e.Table)
		s.forgetKey(e.Table)
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
		quoteIdent(s.keyFor(e.Table)),
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
