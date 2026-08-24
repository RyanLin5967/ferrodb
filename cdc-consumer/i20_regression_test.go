package main

// I20 — regression pins from the adversarial pass over the SQLite/DuckDB sink fixes.
//
// These began as attacks on the claims of the finding-4 and finding-10 commits, written to make
// those fixes fail. FOUR OF THEM LANDED: the first cut of the type check compared declared type
// STRINGS where SQLite semantics are about AFFINITY, and it ran on paths where the incoming shape
// is a guess (`ensureFromRow`) or is stale (a declaration replayed after the destination has
// already applied a retype). Each of those refused a consumer that had done nothing wrong.
//
// They are kept as regression tests because each pins a case the original fix got wrong, and the
// narrowed rule — refuse only when the destination has INTEGER affinity under a TEXT declaration,
// which the closed `Widening` allowlist makes the ONLY affinity a retype can move — is easy to
// widen again by accident.

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// A — a destination built by `ensureFromRow` inference, then handed the source's own
// CREATE_TABLE re-declaration.
//
// `ensureTable(declared=false)` was introduced so a GUESSED shape is not checked against a DECLARED
// destination. The mirror case is not covered: a DECLARED shape checked against a GUESSED
// destination. Both halves of the scenario are ones this code base says are ordinary — a consumer
// that starts after its CREATE_TABLE was truncated away takes the inference path (that is why
// `ensureFromRow` exists), and "a CREATE_TABLE is re-sent at every checkpoint of the source" is
// `checkSchemaAgrees`'s own comment.
func TestI20RegressionInferredDestinationThenDeclaredCreateTable(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "out.sqlite")

	// The consumer joins mid-feed: no CREATE_TABLE, just a row. Types are inferred as TEXT.
	first := filepath.Join(dir, "first.jsonl")
	writeLines(t, first, []string{insertLine(1, 10, 10, "")})
	if err := runSink(first, db, "id", "sqlite"); err != nil {
		t.Fatalf("first pass (inference path): %v", err)
	}
	shape := sqliteRows(t, db, `SELECT name, type FROM pragma_table_info('inv') WHERE name NOT LIKE '\_%' ESCAPE '\'`)
	t.Logf("destination shape after the inference path: %v", shape)

	// The source checkpoints and re-declares the table. Same names, same order, real types.
	second := filepath.Join(dir, "second.jsonl")
	writeLines(t, second, []string{createInv, insertLine(2, 20, 30, "")})
	err := runSink(second, db, "id", "sqlite")
	if err != nil {
		t.Fatalf("REGRESSION: the source's own re-declaration was refused over a destination "+
			"this same sink built one run earlier: %v", err)
	}
	rows := sqliteRows(t, db, `SELECT id, qty FROM inv ORDER BY id`)
	if len(rows) != 2 {
		t.Fatalf("rows lost: %v", rows)
	}
}

// B — the catch-up now compares types, and `declared=false` does NOT switch that off. So a
// column added by the source, arriving on the inference path over a destination that was built from
// a declaration, can no longer be appended.
//
// Before I20 the catch-up compared names only, so the destination grew and the row landed.
func TestI20RegressionInferredCatchUpOverADeclaredDestination(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "out.sqlite")

	first := filepath.Join(dir, "first.jsonl")
	writeLines(t, first, []string{createInv, insertLine(1, 10, 10, "")})
	if err := runSink(first, db, "id", "sqlite"); err != nil {
		t.Fatalf("first pass: %v", err)
	}

	// A restart. The ADD_COLUMN and the CREATE_TABLE re-declaration are both behind this consumer's
	// cursor, so all it has is a row that now carries a third column. `zz` sorts last, so the names
	// still form a strict prefix — which is exactly the case the catch-up exists for.
	second := filepath.Join(dir, "second.jsonl")
	writeLines(t, second, []string{
		`{"table":"inv","op":"INSERT","txn":2,"lsn":30,"commit_lsn":30,"commit_end_lsn":31,` +
			`"before":null,"after":{"id":2,"qty":20,"zz":"new"}}`})
	err := runSink(second, db, "id", "sqlite")
	if err != nil {
		t.Fatalf("REGRESSION: a column the source added could not be caught up on the "+
			"inference path, because the guessed TEXT does not equal the destination's INTEGER: %v", err)
	}
	rows := sqliteRows(t, db, `SELECT id, qty, zz FROM inv ORDER BY id`)
	t.Logf("rows: %v", rows)
	if len(rows) != 2 {
		t.Fatalf("rows lost: %v", rows)
	}
}

// C — two tables in one feed with different key columns. The `-key` flag is one global name.
func TestI20RegressionTwoTablesDifferentKeys(t *testing.T) {
	const createOrders = `{"table":"orders","op":"CREATE_TABLE","txn":0,"lsn":1,"commit_lsn":1,"commit_end_lsn":2,` +
		`"before":null,"after":{"columns":[` +
		`{"name":"sku","type":"INTEGER","nullable":false},` +
		`{"name":"n","type":"INTEGER","nullable":true}]}}`
	orderRow := func(sku, n, lsn int) string {
		return fmt.Sprintf(
			`{"table":"orders","op":"INSERT","txn":%d,"lsn":%d,"commit_lsn":%d,"commit_end_lsn":%d,`+
				`"before":null,"after":{"sku":%d,"n":%d}}`, sku, lsn, lsn, lsn+1, sku, n)
	}
	for _, engine := range []string{"sqlite", "duckdb"} {
		t.Run(engine, func(t *testing.T) {
			dir := t.TempDir()
			db := filepath.Join(dir, "out."+engine)
			feed := filepath.Join(dir, "feed.jsonl")
			writeLines(t, feed, []string{
				createInv, insertLine(1, 10, 10, ""),
				createOrders, orderRow(7, 1, 20), orderRow(7, 2, 30),
			})
			err := runSink(feed, db, "id", engine)
			if err != nil {
				t.Logf("%s: refused a second table whose key is not the flag: %v", engine, err)
				return
			}
			t.Logf("%s: accepted. checking whether the upsert had a conflict target", engine)
			if engine == "sqlite" {
				rows := sqliteRows(t, db, `SELECT sku, n FROM orders ORDER BY sku, n`)
				t.Logf("orders rows: %v", rows)
				if len(rows) != 1 {
					t.Fatalf("REGRESSION: the second table's upsert had no conflict target; "+
						"two deliveries of one key produced %d rows: %v", len(rows), rows)
				}
			}
		})
	}
}

// D — a table with NO primary key at all in the destination. `keyFor` falls back to the flag
// without caching. Does the fallback produce a wrong conflict target or a stall?
func TestI20RegressionKeyForOnAPrimaryKeylessDestination(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "out.sqlite")
	s, err := openSink(db, "id")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if _, err := s.db.Exec(`CREATE TABLE inv (id INTEGER, qty INTEGER)`); err != nil {
		t.Fatal(err)
	}
	got := s.keyFor("inv")
	t.Logf("keyFor on a PK-less destination: %q (flag is %q)", got, s.key)
	if got != "id" {
		t.Fatalf("keyFor invented a key: %q", got)
	}
	if _, cached := s.keys["inv"]; cached {
		t.Fatalf("the fallback was cached, so a later CREATE of a real key would not be seen")
	}
}

// E — `keyFor`'s cache going stale and producing a WRONG conflict target.
//
// The cache is dropped by `forgetKey` on DROP_TABLE and at the end of `applySchemaChange`. The
// question is whether any path caches a name and then changes the destination without dropping it.
// A DROP followed by a re-CREATE whose key column is a different name is the sharpest version.
func TestI20RegressionKeyCacheAcrossDropAndRecreate(t *testing.T) {
	const dropInv = `{"table":"inv","op":"DROP_TABLE","txn":0,"lsn":40,"commit_lsn":40,"commit_end_lsn":41,` +
		`"before":null,"after":null}`
	const recreateInvOtherKey = `{"table":"inv","op":"CREATE_TABLE","txn":0,"lsn":50,"commit_lsn":50,"commit_end_lsn":51,` +
		`"before":null,"after":{"columns":[` +
		`{"name":"id","type":"INTEGER","nullable":false},` +
		`{"name":"qty","type":"INTEGER","nullable":true}]}}`
	dir := t.TempDir()
	db := filepath.Join(dir, "out.sqlite")
	feed := filepath.Join(dir, "feed.jsonl")
	writeLines(t, feed, []string{
		createInv,
		insertLine(1, 10, 10, ""),
		renameKeyLine(20),
		keyedInsertLine(1, 99, 30),
		dropInv,
		recreateInvOtherKey,
		insertLine(1, 5, 60, ""),
		insertLine(1, 6, 70, ""),
	})
	if err := runSink(feed, db, "id", "sqlite"); err != nil {
		t.Fatalf("ATTACK LANDED (stall): %v", err)
	}
	rows := sqliteRows(t, db, `SELECT id, qty FROM inv ORDER BY id`)
	t.Logf("after drop+recreate: %v", rows)
	if len(rows) != 1 || fmt.Sprint(rows[0][1]) != "6" {
		t.Fatalf("REGRESSION: the conflict target after a drop+recreate is wrong: %v", rows)
	}
	pk := sqliteRows(t, db, `SELECT name FROM pragma_table_info('inv') WHERE pk != 0`)
	if len(pk) != 1 || fmt.Sprint(pk[0][0]) != "id" {
		t.Fatalf("the re-created table's key is not the declared one: %v", pk)
	}
}

// F — `checkSchemaAgrees` refusing something that legitimately worked before: a destination
// that already carries the EVOLVED shape being re-handed the ORIGINAL CREATE_TABLE on a replay.
// The comment claims this is covered by "NOT a column-count check". Verify it with a TYPE that also
// moved, which is the case the count exemption does not reach.
func TestI20RegressionReplayOfAnOriginalDeclarationAfterARetype(t *testing.T) {
	const retypeQty = `{"table":"inv","op":"ALTER_COLUMN_TYPE","txn":0,"lsn":20,"commit_lsn":20,"commit_end_lsn":21,` +
		`"before":null,"after":{"columns":[` +
		`{"name":"id","type":"INTEGER","nullable":false},` +
		`{"name":"qty","type":"DECIMAL","nullable":true}],` +
		`"alter":{"column":"qty","from":"INTEGER","to":"DECIMAL"}}}`
	dir := t.TempDir()
	db := filepath.Join(dir, "out.sqlite")
	feed := filepath.Join(dir, "feed.jsonl")
	writeLines(t, feed, []string{createInv, insertLine(1, 10, 10, ""), retypeQty})
	if err := runSink(feed, db, "id", "sqlite"); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	// A replay from the top: at-least-once, which the module documents as the contract.
	err := runSink(feed, db, "id", "sqlite")
	if err != nil {
		t.Fatalf("REGRESSION: replaying a feed that contains a retype now refuses at the "+
			"original CREATE_TABLE: %v", err)
	}
}

// G — the type-agreement check against a type `sqlType` maps to TEXT from a name the
// destination stores verbatim. `pragma_table_info` returns the DECLARED string, so a destination
// created by hand — or by an older version of this sink — spells its types differently.
func TestI20RegressionTypeCheckAgainstAHandBuiltDestination(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "out.sqlite")
	s, err := openSink(db, "id")
	if err != nil {
		t.Fatal(err)
	}
	// An older sink (or a DBA) spelled the same affinity differently. SQLite treats VARCHAR(20) and
	// TEXT as the same affinity, and every value round-trips identically.
	// The bookkeeping columns are NOT optional and never have been: `ensureWriterColumns` adds the
	// attribution columns but not these three, so a destination without them fails at INSERT with
	// `no such column: _commit_lsn` regardless of anything I20 changed. Included so this test
	// exercises the affinity question it claims to and not that pre-existing requirement.
	if _, err := s.db.Exec(`CREATE TABLE inv ("id" INTEGER PRIMARY KEY, "note" VARCHAR(20),
		"_commit_lsn" INTEGER NOT NULL, "_lsn" INTEGER NOT NULL DEFAULT 0,
		"_deleted" INTEGER NOT NULL DEFAULT 0)`); err != nil {
		t.Fatal(err)
	}
	s.Close()

	feed := filepath.Join(dir, "feed.jsonl")
	writeLines(t, feed, []string{
		`{"table":"inv","op":"CREATE_TABLE","txn":0,"lsn":1,"commit_lsn":1,"commit_end_lsn":2,` +
			`"before":null,"after":{"columns":[` +
			`{"name":"id","type":"INTEGER","nullable":false},` +
			`{"name":"note","type":"VARCHAR(20)","nullable":true}]}}`,
		`{"table":"inv","op":"INSERT","txn":1,"lsn":10,"commit_lsn":10,"commit_end_lsn":11,` +
			`"before":null,"after":{"id":1,"note":"hello"}}`,
	})
	err = runSink(feed, db, "id", "sqlite")
	if err != nil {
		t.Fatalf("REGRESSION: a destination whose affinity is identical but whose declared "+
			"spelling differs is now refused: %v", err)
	}
	if !strings.Contains(fmt.Sprint(sqliteRows(t, db, `SELECT note FROM inv`)), "hello") {
		t.Fatalf("the row did not land")
	}
}
