package main

// B11 — column-level schema evolution, from this side of the wire.
//
// The producer and this program were changed together, on purpose. E69 records what happens when
// they are not: the SQLite sink had had a `DROP_TABLE` branch since E15 and the validator had had a
// rule about it, and the two disagreed about whether the event carries an after image — which
// nobody discovered until the day a real drop reached a real feed and failed the whole run. The
// tests here pin **both halves of the contract**: that a well-formed column-level event is
// accepted, and that each way the producer could get it wrong is refused.

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeLines writes a feed file, newline-terminated as the format requires.
func writeLines(t *testing.T, path string, lines []string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatalf("write feed: %v", err)
	}
}

// The shape after a column was added, as the source would declare it.
const widerShape = `{"columns":[` +
	`{"name":"id","type":"INTEGER","nullable":false},` +
	`{"name":"qty","type":"INTEGER","nullable":true},` +
	`{"name":"note","type":"VARCHAR(20)","nullable":true}]`

func shapeLine(op, alter string, lsn int) string {
	return fmt.Sprintf(
		`{"table":"inv","op":%q,"txn":0,"lsn":%d,"commit_lsn":%d,"commit_end_lsn":%d,"before":null,`+
			`"after":%s,"alter":%s}}`,
		op, lsn, lsn, lsn+1, widerShape, alter)
}

// A well-formed line for each of the three ops. Both halves present, and agreeing.
func addColumnLine(lsn int) string {
	return shapeLine("ADD_COLUMN", `{"column":"note"}`, lsn)
}

func renameLine(lsn int) string {
	return fmt.Sprintf(
		`{"table":"inv","op":"RENAME_COLUMN","txn":0,"lsn":%d,"commit_lsn":%d,"commit_end_lsn":%d,`+
			`"before":null,"after":{"columns":[`+
			`{"name":"id","type":"INTEGER","nullable":false},`+
			`{"name":"quantity","type":"INTEGER","nullable":true}],`+
			`"alter":{"from":"qty","to":"quantity"}}}`, lsn, lsn, lsn+1)
}

func retypeLine(lsn int) string {
	return fmt.Sprintf(
		`{"table":"inv","op":"ALTER_COLUMN_TYPE","txn":0,"lsn":%d,"commit_lsn":%d,"commit_end_lsn":%d,`+
			`"before":null,"after":{"columns":[`+
			`{"name":"id","type":"INTEGER","nullable":false},`+
			`{"name":"qty","type":"BIGINT","nullable":true}],`+
			`"alter":{"column":"qty","from":"INTEGER","to":"BIGINT"}}}`, lsn, lsn, lsn+1)
}

// **The anti-vacuity half, and it comes first.** Every refusal below is worthless if the validator
// simply refuses all three ops — which is exactly what it did before this change, with
// `unknown op "ADD_COLUMN"`.
func TestWellFormedColumnLevelEventsAreAccepted(t *testing.T) {
	for _, line := range []string{addColumnLine(10), renameLine(10), retypeLine(10)} {
		if _, err := decodeLine(line, 1); err != nil {
			t.Errorf("a well-formed column-level event was refused: %v\n%s", err, line)
		}
	}
}

// The cross-checks are the point of an independent implementation: they verify the event's two
// halves — the full new shape, and the alteration that produced it — agree with each other. A
// producer validated by its own idea of the format cannot check that for itself.
func TestColumnLevelEventsAreCrossCheckedAgainstTheirOwnShape(t *testing.T) {
	bad := map[string]string{
		// The shape says nothing about `note`, so a consumer adding it would add a column the
		// source does not have.
		"ADD_COLUMN naming a column the shape lacks": strings.Replace(
			addColumnLine(10), `"column":"note"`, `"column":"nosuch"`, 1),
		// A rename whose old name is STILL in the shape is a rename that did not happen.
		"RENAME_COLUMN whose from survives in the shape": strings.Replace(
			renameLine(10), `"from":"qty"`, `"from":"id"`, 1),
		// ...and one whose new name is absent is a rename to nowhere.
		"RENAME_COLUMN whose to is absent from the shape": strings.Replace(
			renameLine(10), `"to":"quantity"`, `"to":"nosuch"`, 1),
		"RENAME_COLUMN renaming a column to itself": strings.Replace(
			renameLine(10), `"from":"qty","to":"quantity"`, `"from":"quantity","to":"quantity"`, 1),
		// The declared new type must be the type the shape gives that column, or a sink issuing the
		// conversion and a sink reconciling the shape reach different destinations.
		"ALTER_COLUMN_TYPE disagreeing with its own shape": strings.Replace(
			retypeLine(10), `"to":"BIGINT"}`, `"to":"DECIMAL"}`, 1),
		"ALTER_COLUMN_TYPE from and to the same type": strings.Replace(
			retypeLine(10), `"from":"INTEGER"`, `"from":"BIGINT"`, 1),
		// Both halves are required. The shape alone cannot say a rename happened...
		"a column-level event with no alter payload": strings.Replace(
			addColumnLine(10), `,"alter":{"column":"note"}`, "", 1),
		// ...and the alteration alone leaves a sink nothing to reconcile against.
		"a column-level event with no columns": `{"table":"inv","op":"ADD_COLUMN","txn":0,"lsn":1,` +
			`"commit_lsn":1,"commit_end_lsn":2,"before":null,"after":{"alter":{"column":"note"}}}`,
		"a column-level event carrying a before image": strings.Replace(
			addColumnLine(10), `"before":null`, `"before":{"id":1}`, 1),
		"ADD_COLUMN with an empty column name": strings.Replace(
			addColumnLine(10), `"column":"note"`, `"column":""`, 1),
	}
	for name, line := range bad {
		if _, err := decodeLine(line, 1); err == nil {
			t.Errorf("%s: accepted, should have been refused\n%s", name, line)
		}
	}
}

// **A declaration is re-emitted; news is not.** `bypassesCursor` exempts the ops whose idempotence
// cannot come from the resume cursor because the source re-sends them at every checkpoint. The
// column-level three are delivered exactly once, at the position the DDL occupied, so the cursor is
// the right mechanism for them — and exempting them would be worse than useless: replaying a feed
// would re-apply the rename.
func TestColumnLevelChangesAreNewsNotDeclarations(t *testing.T) {
	for _, op := range []string{"CREATE_TABLE", "DROP_TABLE"} {
		if !isDeclaration(op) || !bypassesCursor(op) {
			t.Errorf("%s stopped being a re-emitted declaration", op)
		}
	}
	for _, op := range []string{"ADD_COLUMN", "RENAME_COLUMN", "ALTER_COLUMN_TYPE"} {
		if isDeclaration(op) {
			t.Errorf("%s is delivered once; treating it as a declaration would re-apply it", op)
		}
		if bypassesCursor(op) {
			t.Errorf("%s bypasses the cursor, so replaying a feed would apply it twice", op)
		}
		if !isSchema(op) {
			t.Errorf("%s describes the table's shape and should read as a schema op", op)
		}
	}
}

func decoded(t *testing.T, line string) *Event {
	t.Helper()
	e, err := decodeLine(line, 1)
	if err != nil {
		t.Fatalf("fixture line is invalid: %v", err)
	}
	return e
}

// The in-memory fold is what `follow` prints and what `diff` compares against the source, so it has
// to survive a shape change independently of any destination database.
func TestTableFoldSurvivesAColumnLevelChange(t *testing.T) {
	tab := newTable("id")
	insert := func(after string) {
		e := &Event{Table: "inv", Op: "INSERT"}
		if err := json.Unmarshal([]byte(after), &e.After); err != nil {
			t.Fatal(err)
		}
		if err := tab.apply(e); err != nil {
			t.Fatal(err)
		}
	}
	insert(`{"id":1,"qty":10}`)

	// ADD_COLUMN: the shape grows, the row stays, and the new column is simply absent from a row
	// written before it existed. That is the honest reading — the source did not re-image the row,
	// because adding a nullable column changed no value.
	if err := tab.apply(decoded(t, addColumnLine(10))); err != nil {
		t.Fatalf("ADD_COLUMN: %v", err)
	}
	if len(tab.columns) != 3 || tab.columns[2] != "note" {
		t.Fatalf("the fold did not adopt the wider shape: %v", tab.columns)
	}
	if len(tab.rows) != 1 {
		t.Fatalf("ADD_COLUMN lost rows: %d", len(tab.rows))
	}
}

// **The case that makes `alter.from` load-bearing.** Renaming the shape and leaving the held rows
// alone would leave every one of them keyed by a name the table no longer has, and `diff` against
// the source — which reports the new name — would show every row as different.
func TestTheFoldMovesRenamedValuesInRowsItAlreadyHolds(t *testing.T) {
	tab := newTable("id")
	e := &Event{Table: "inv", Op: "INSERT"}
	if err := json.Unmarshal([]byte(`{"id":1,"qty":10}`), &e.After); err != nil {
		t.Fatal(err)
	}
	if err := tab.apply(e); err != nil {
		t.Fatal(err)
	}
	if err := tab.apply(decoded(t, renameLine(10))); err != nil {
		t.Fatalf("RENAME_COLUMN: %v", err)
	}
	row := tab.rows["1"]
	if _, stale := row["qty"]; stale {
		t.Errorf("the old column name survived in a held row: %v", row)
	}
	if got := fmt.Sprint(row["quantity"]); got != "10" {
		t.Errorf("the value did not move to the new name: %v", row)
	}
}

// A retype changes the column's *encoding* in the feed — the producer ships BIGINT as a JSON string
// because a double cannot hold an i64 exactly — and the source does NOT re-image existing rows,
// because their values did not change. So the fold holds two encodings for one column unless it
// re-encodes what it already has. Breaking shape: any row delivered before the retype.
func TestTheFoldReEncodesAColumnThatBecameStringTyped(t *testing.T) {
	tab := newTable("id")
	e := &Event{Table: "inv", Op: "INSERT"}
	if err := json.Unmarshal([]byte(`{"id":1,"qty":10}`), &e.After); err != nil {
		t.Fatal(err)
	}
	// The producer's own decoder keeps numbers exact; mirror it so the fixture is the real shape.
	dec := json.NewDecoder(strings.NewReader(`{"id":1,"qty":10}`))
	dec.UseNumber()
	if err := dec.Decode(&e.After); err != nil {
		t.Fatal(err)
	}
	if err := tab.apply(e); err != nil {
		t.Fatal(err)
	}
	if err := tab.apply(decoded(t, retypeLine(10))); err != nil {
		t.Fatalf("ALTER_COLUMN_TYPE: %v", err)
	}
	if got, ok := tab.rows["1"]["qty"].(string); !ok || got != "10" {
		t.Errorf("a row delivered before the retype kept the old encoding: %#v", tab.rows["1"]["qty"])
	}

	// Anti-vacuity: a retype to a type the feed does NOT string-encode must leave the value alone,
	// or every INTEGER column would be stringified by any retype at all.
	tab2 := newTable("id")
	e2 := &Event{Table: "inv", Op: "INSERT"}
	dec2 := json.NewDecoder(strings.NewReader(`{"id":1,"qty":10}`))
	dec2.UseNumber()
	if err := dec2.Decode(&e2.After); err != nil {
		t.Fatal(err)
	}
	if err := tab2.apply(e2); err != nil {
		t.Fatal(err)
	}
	widen := strings.Replace(
		strings.Replace(retypeLine(10), `"type":"BIGINT"`, `"type":"VARCHAR(9)"`, 1),
		`"to":"BIGINT"`, `"to":"VARCHAR(9)"`, 1)
	if err := tab2.apply(decoded(t, widen)); err != nil {
		t.Fatalf("widening retype: %v", err)
	}
	if _, stringified := tab2.rows["1"]["qty"].(string); stringified {
		t.Error("a retype to a non-string-encoded type stringified the value anyway")
	}
}

func sqliteRows(t *testing.T, path, query string) [][]any {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	rows, err := db.Query(query)
	if err != nil {
		t.Fatalf("%s: %v", query, err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out [][]any
	for rows.Next() {
		cells := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range cells {
			ptrs[i] = &cells[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		out = append(out, cells)
	}
	return out
}

const createInv = `{"table":"inv","op":"CREATE_TABLE","txn":0,"lsn":1,"commit_lsn":1,"commit_end_lsn":2,` +
	`"before":null,"after":{"columns":[` +
	`{"name":"id","type":"INTEGER","nullable":false},` +
	`{"name":"qty","type":"INTEGER","nullable":true}]}}`

func insertLine(id, qty, lsn int, extra string) string {
	return fmt.Sprintf(
		`{"table":"inv","op":"INSERT","txn":%d,"lsn":%d,"commit_lsn":%d,"commit_end_lsn":%d,`+
			`"before":null,"after":{"id":%d,"qty":%d%s}}`, id, lsn, lsn, lsn+1, id, qty, extra)
}

// **The exit criterion, from the consumer's side: a column added mid-stream reaches the destination
// and the sink keeps landing rows.**
//
// Breaking shape: rows on BOTH sides of the ADD_COLUMN. A feed whose rows all arrive before the
// alter would pass with a sink that landed the DDL and then stopped writing; one whose rows all
// arrive after would pass with a sink that dropped and recreated the table.
func TestTheSqliteSinkAddsAColumnMidStreamAndKeepsLandingRows(t *testing.T) {
	dir := t.TempDir()
	feed := filepath.Join(dir, "feed.jsonl")
	db := filepath.Join(dir, "out.sqlite")
	lines := []string{
		createInv,
		insertLine(1, 10, 10, ""),
		insertLine(2, 20, 12, ""),
		addColumnLine(20),
		insertLine(3, 30, 30, `,"note":"after"`),
	}
	writeLines(t, feed, lines)

	if err := runSink(feed, db, "id", "sqlite"); err != nil {
		t.Fatalf("the sink failed on a feed containing an ADD_COLUMN: %v", err)
	}

	// The column is there, and the DATA columns are in the source's order.
	//
	// Physical order is not: SQLite's `ALTER TABLE ADD COLUMN` appends at the end of the table,
	// which is after this sink's three bookkeeping columns, so `pragma_table_info` reports
	// [id qty _commit_lsn _lsn _deleted note]. That is fine and it is why both sinks filter the
	// bookkeeping names out before they compare anything — but the *data* order does matter: the
	// DuckDB sink's `checkSchemaAgrees` compares the destination's columns against the source's
	// declared shape POSITIONALLY, so a column landing in the wrong data position would abort every
	// later run rather than being cosmetic.
	// The bookkeeping set includes B5's seven attribution columns, not just the three this test was
	// written against. The attribution half is driven off `writerColumns` rather than spelled out
	// again: a hand-written copy is a place to miss the next column, and that is exactly how the
	// sink's own filter came to be wrong here.
	bookkeeping := map[string]bool{"_commit_lsn": true, "_lsn": true, "_deleted": true}
	for _, w := range writerColumns {
		bookkeeping[w.name] = true
	}
	var names []string
	for _, r := range sqliteRows(t, db, `SELECT name FROM pragma_table_info('inv') ORDER BY cid`) {
		n := fmt.Sprint(r[0])
		if !bookkeeping[n] {
			names = append(names, n)
		}
	}
	if fmt.Sprint(names) != "[id qty note]" {
		t.Fatalf("the destination's data columns are not the source's shape: %v", names)
	}
	// ...the rows from BEFORE it survived, with the new column null...
	rows := sqliteRows(t, db, `SELECT id, qty, note FROM inv ORDER BY id`)
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d: %v", len(rows), rows)
	}
	if rows[0][2] != nil {
		t.Errorf("a row written before the column got a value for it: %v", rows[0])
	}
	// ...and the row from AFTER it landed with its value, which is "the sink keeps landing rows".
	if fmt.Sprint(rows[2][2]) != "after" {
		t.Errorf("the row written after the ALTER did not land its new column: %v", rows[2])
	}
}

// Replaying the same feed must change nothing. This is the property `bypassesCursor` decides: an
// ALTER that bypassed the cursor would be re-applied on every replay.
func TestReplayingAFeedWithASchemaChangeIsANoOp(t *testing.T) {
	dir := t.TempDir()
	feed := filepath.Join(dir, "feed.jsonl")
	db := filepath.Join(dir, "out.sqlite")
	writeLines(t, feed, []string{createInv, insertLine(1, 10, 10, ""), addColumnLine(20),
		insertLine(2, 20, 30, `,"note":"x"`)})

	if err := runSink(feed, db, "id", "sqlite"); err != nil {
		t.Fatalf("first pass: %v", err)
	}
	before := sqliteRows(t, db, `SELECT id, qty, note FROM inv ORDER BY id`)
	if err := runSink(feed, db, "id", "sqlite"); err != nil {
		t.Fatalf("replay: %v", err)
	}
	after := sqliteRows(t, db, `SELECT id, qty, note FROM inv ORDER BY id`)
	if fmt.Sprint(before) != fmt.Sprint(after) {
		t.Errorf("replaying the feed changed the destination:\n before %v\n after  %v", before, after)
	}
}

// A rename must move the column, not drop it and add another. Breaking shape: a destination that
// already holds rows — the values have to arrive under the new name.
func TestTheSqliteSinkRenamesRatherThanRebuilding(t *testing.T) {
	dir := t.TempDir()
	feed := filepath.Join(dir, "feed.jsonl")
	db := filepath.Join(dir, "out.sqlite")
	writeLines(t, feed, []string{createInv, insertLine(1, 10, 10, ""), renameLine(20)})

	if err := runSink(feed, db, "id", "sqlite"); err != nil {
		t.Fatalf("the sink failed on a RENAME_COLUMN: %v", err)
	}
	rows := sqliteRows(t, db, `SELECT id, quantity FROM inv ORDER BY id`)
	if len(rows) != 1 {
		t.Fatalf("the rename lost the table's rows: %v", rows)
	}
	if fmt.Sprint(rows[0][1]) != "10" {
		t.Errorf("the renamed column lost its data: %v", rows[0])
	}
}

// SQLite cannot change a declared type in place, and the declared type is not decoration: it sets
// the column's affinity, and `sqlType` maps DECIMAL to TEXT while INTEGER and BIGINT both map to
// INTEGER. A retype to DECIMAL that left INTEGER affinity in place would coerce the source's digit
// strings back to integers on the way in — losing exactly the digits the source ships them as text
// to protect. So the sink rebuilds, and the rebuild must carry the rows.
func TestTheSqliteSinkRebuildsForARetypeAndKeepsTheRows(t *testing.T) {
	dir := t.TempDir()
	feed := filepath.Join(dir, "feed.jsonl")
	db := filepath.Join(dir, "out.sqlite")
	toDecimal := fmt.Sprintf(
		`{"table":"inv","op":"ALTER_COLUMN_TYPE","txn":0,"lsn":20,"commit_lsn":20,"commit_end_lsn":21,` +
			`"before":null,"after":{"columns":[` +
			`{"name":"id","type":"INTEGER","nullable":false},` +
			`{"name":"qty","type":"DECIMAL","nullable":true}],` +
			`"alter":{"column":"qty","from":"INTEGER","to":"DECIMAL"}}}`)
	writeLines(t, feed, []string{createInv, insertLine(1, 10, 10, ""), toDecimal,
		`{"table":"inv","op":"INSERT","txn":2,"lsn":30,"commit_lsn":30,"commit_end_lsn":31,` +
			`"before":null,"after":{"id":2,"qty":"123456789012345678901234567890"}}`})

	if err := runSink(feed, db, "id", "sqlite"); err != nil {
		t.Fatalf("the sink failed on an ALTER_COLUMN_TYPE: %v", err)
	}
	// The pre-existing row survived the rebuild...
	rows := sqliteRows(t, db, `SELECT id, qty FROM inv ORDER BY id`)
	if len(rows) != 2 {
		t.Fatalf("the rebuild lost rows: %v", rows)
	}
	// ...the affinity actually moved, so the 30-digit value is stored whole rather than coerced...
	if got := fmt.Sprint(rows[1][1]); got != "123456789012345678901234567890" {
		t.Errorf("the retyped column did not keep every digit: %q", got)
	}
	// ...and the declared type in the destination catalog says so, which is what a later
	// re-declaration is reconciled against.
	info := sqliteRows(t, db, `SELECT name, type FROM pragma_table_info('inv') WHERE name='qty'`)
	if len(info) != 1 || !strings.EqualFold(fmt.Sprint(info[0][1]), "TEXT") {
		t.Errorf("the destination column was not retyped: %v", info)
	}
	// The primary key survived the rebuild — losing it would silently turn every upsert into an
	// append, which the ordering guard depends on.
	pk := sqliteRows(t, db, `SELECT name FROM pragma_table_info('inv') WHERE pk = 1`)
	if len(pk) != 1 || fmt.Sprint(pk[0][0]) != "id" {
		t.Errorf("the rebuild dropped the primary key: %v", pk)
	}
}

// A sink whose first sight of a table is the ALTER — the resume case, where the source truncated
// its log below the CREATE — must create the table from the shape the event carries rather than
// failing or landing nothing.
func TestASinkThatNeverSawTheCreateStillGetsTheShape(t *testing.T) {
	dir := t.TempDir()
	feed := filepath.Join(dir, "feed.jsonl")
	db := filepath.Join(dir, "out.sqlite")
	writeLines(t, feed, []string{addColumnLine(20), insertLine(1, 10, 30, `,"note":"x"`)})

	if err := runSink(feed, db, "id", "sqlite"); err != nil {
		t.Fatalf("a feed starting at an ADD_COLUMN was refused: %v", err)
	}
	rows := sqliteRows(t, db, `SELECT id, qty, note FROM inv`)
	if len(rows) != 1 || fmt.Sprint(rows[0][2]) != "x" {
		t.Errorf("the row did not land under the shape the ALTER declared: %v", rows)
	}
}

// **A consumer that missed the ALTER window must be able to catch up, not be stuck forever.**
//
// This state did not exist before column-level DDL and is reachable without anybody doing anything
// wrong: a consumer applies `CREATE_TABLE(id, qty)`, dies before the `ADD_COLUMN`, and by the time
// it resumes the source has checkpointed — which truncates the log and re-declares the table at its
// **evolved** shape. The alter record it needed is gone. `CREATE TABLE IF NOT EXISTS` is a no-op on
// the table it already has, so without a catch-up the destination stays two columns wide and every
// later row either fails to insert (SQLite) or is refused by the schema check (DuckDB), permanently.
//
// Breaking shape: a destination built from the OLD declaration, then handed only the NEW one.
func TestASinkThatMissedTheAlterCatchesUpFromTheReDeclaration(t *testing.T) {
	dir := t.TempDir()
	db := filepath.Join(dir, "out.sqlite")

	// Pass one: the pre-alter world. The consumer dies here.
	first := filepath.Join(dir, "first.jsonl")
	writeLines(t, first, []string{createInv, insertLine(1, 10, 10, "")})
	if err := runSink(first, db, "id", "sqlite"); err != nil {
		t.Fatalf("first pass: %v", err)
	}

	// Pass two: the source truncated its log, so all the consumer is handed is the re-emitted
	// declaration at the evolved shape — no ADD_COLUMN anywhere in it — and then rows using it.
	wider := `{"table":"inv","op":"CREATE_TABLE","txn":0,"lsn":40,"commit_lsn":40,"commit_end_lsn":41,` +
		`"before":null,"after":` + widerShape + `}}`
	second := filepath.Join(dir, "second.jsonl")
	writeLines(t, second, []string{wider, insertLine(2, 20, 50, `,"note":"later"`)})
	if err := runSink(second, db, "id", "sqlite"); err != nil {
		t.Fatalf("a consumer resuming after the alter window was refused: %v", err)
	}

	rows := sqliteRows(t, db, `SELECT id, qty, COALESCE(note,'<null>') FROM inv ORDER BY id`)
	if len(rows) != 2 {
		t.Fatalf("expected both rows, got %v", rows)
	}
	if fmt.Sprint(rows[0][2]) != "<null>" {
		t.Errorf("the row from before the column got a value for it: %v", rows[0])
	}
	if fmt.Sprint(rows[1][2]) != "later" {
		t.Errorf("the row that needed the new column did not land it: %v", rows[1])
	}
}

// **Anti-vacuity for the catch-up: only a strict PREFIX is caught up.** A declaration that renames
// or retypes a column the destination already has must still be refused, because a shape diff
// cannot tell a rename from a drop-plus-add and guessing there loses the column's data.
//
// Measured on the DuckDB sink, which is the one with a schema check to subvert.
func TestTheCatchUpRefusesAnythingThatIsNotAPrefix(t *testing.T) {
	s := &DuckSink{key: "id"}
	// Destination has [id INTEGER, qty BIGINT]; the declaration renames the second column.
	grown, err := s.catchUpToDeclaredShape("inv",
		[]string{"id", "quantity", "note"}, []string{"BIGINT", "BIGINT", "VARCHAR"},
		[]string{"id", "qty"}, []string{"BIGINT", "BIGINT"})
	if err != nil || grown {
		t.Errorf("a rename was treated as a catch-up (grown=%v, err=%v)", grown, err)
	}
	// ...and one that retypes it.
	grown, err = s.catchUpToDeclaredShape("inv",
		[]string{"id", "qty", "note"}, []string{"BIGINT", "VARCHAR", "VARCHAR"},
		[]string{"id", "qty"}, []string{"BIGINT", "BIGINT"})
	if err != nil || grown {
		t.Errorf("a retype was treated as a catch-up (grown=%v, err=%v)", grown, err)
	}
	// ...and a destination that is not behind at all is left alone.
	grown, err = s.catchUpToDeclaredShape("inv",
		[]string{"id", "qty"}, []string{"BIGINT", "BIGINT"},
		[]string{"id", "qty"}, []string{"BIGINT", "BIGINT"})
	if err != nil || grown {
		t.Errorf("an up-to-date destination was altered (grown=%v, err=%v)", grown, err)
	}
	// A type the sink will not emit is refused rather than concatenated into DDL, even here.
	if _, err := s.catchUpToDeclaredShape("inv",
		[]string{"id", "evil"}, []string{"BIGINT", "VARCHAR; DROP TABLE inv"},
		[]string{"id"}, []string{"BIGINT"}); err == nil {
		t.Error("a type outside the allowlist reached the catch-up's DDL")
	}
}

// The DuckDB half of the catch-up, against a real DuckDB file.
//
// DuckDB is the engine with a schema check to satisfy: `checkSchemaAgrees` compares the destination
// against the declared shape positionally and refuses any disagreement, which is right and which is
// exactly what would strand a consumer that missed the ALTER window. Breaking shape: a destination
// created from the OLD declaration, then handed only the NEW one and a row that needs it.
func TestTheFeedLandsInDuckdbAcrossAMissedAlter(t *testing.T) {
	s := newSink(t)
	old := &Event{Table: "inv", Op: "CREATE_TABLE", CommitLSN: 1, CommitEndLSN: 2, After: map[string]any{
		"columns": []any{
			map[string]any{"name": "id", "type": "INTEGER", "nullable": false},
			map[string]any{"name": "qty", "type": "INTEGER", "nullable": true},
		},
	}}
	if err := s.apply(old); err != nil {
		t.Fatalf("the pre-alter declaration was refused: %v", err)
	}
	if err := s.apply(&Event{Table: "inv", Op: "INSERT", CommitLSN: 3, CommitEndLSN: 4, LSN: 3,
		After: map[string]any{"id": 1, "qty": 10}}); err != nil {
		t.Fatalf("pre-alter row: %v", err)
	}

	// The consumer never sees the ADD_COLUMN — only the evolved re-declaration.
	evolved := &Event{Table: "inv", Op: "CREATE_TABLE", CommitLSN: 5, CommitEndLSN: 6, After: map[string]any{
		"columns": []any{
			map[string]any{"name": "id", "type": "INTEGER", "nullable": false},
			map[string]any{"name": "qty", "type": "INTEGER", "nullable": true},
			map[string]any{"name": "note", "type": "VARCHAR(20)", "nullable": true},
		},
	}}
	if err := s.apply(evolved); err != nil {
		t.Fatalf("a consumer resuming after the alter window was refused: %v", err)
	}
	if err := s.apply(&Event{Table: "inv", Op: "INSERT", CommitLSN: 7, CommitEndLSN: 8, LSN: 7,
		After: map[string]any{"id": 2, "qty": 20, "note": "later"}}); err != nil {
		t.Fatalf("the row that needed the new column: %v", err)
	}

	var note string
	if err := s.db.QueryRow(`SELECT note FROM inv WHERE id = 2`).Scan(&note); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if note != "later" {
		t.Errorf("the caught-up column did not take the value: %q", note)
	}
	// The row from before the catch-up survived it — a sink that "fixed" the shape by recreating
	// the table would have a correct schema over an empty table.
	var n int
	if err := s.db.QueryRow(`SELECT count(*) FROM inv`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 2 {
		t.Errorf("the catch-up lost rows: %d", n)
	}
}
