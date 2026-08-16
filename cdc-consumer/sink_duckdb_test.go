package main

import (
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Unit tests for the DuckDB sink.
//
// The Rust integration test lands a real feed and checks the destination with the `duckdb` CLI —
// that is the end-to-end proof. These cover the parts a whole-pipeline test reaches only obliquely:
// the DDL allowlist, the type inference used when a CREATE_TABLE has been truncated away, the
// catalog lookup that replaces guessing on restart, and the ordering guard exercised directly with
// out-of-order events rather than through a feed file.

func TestDuckTypeMapping(t *testing.T) {
	cases := map[string]string{
		// INTEGER widens to BIGINT deliberately: the feed's integers are JSON numbers with no
		// declared width, and narrowing them here would reject a value the source accepted.
		"INTEGER":     "BIGINT",
		"BOOLEAN":     "BOOLEAN",
		"FLOAT":       "DOUBLE",
		"VARCHAR(32)": "VARCHAR",
		"VARCHAR(1)":  "VARCHAR",
		"TEXT":        "VARCHAR",

		// The wide types. These were reaching the `default` arm and being declared VARCHAR by
		// accident rather than by decision — the same way the SQLite sink was declaring them TEXT,
		// and for the same root cause: `ColumnSpec::sql_type`'s doc described the wire contract as
		// four types long after it was seven, and both sinks were written against that list.
		//
		// BIGINT is exact in DuckDB, which has a real 64-bit integer, and the feed's
		// string-encoded digits go into it losslessly — measured, not assumed: inserting
		// "9223372036854775807" into a BIGINT column stores 9223372036854775807.
		"BIGINT": "BIGINT",

		// TIMESTAMP is epoch MILLISECONDS as an i64, so it maps to BIGINT and deliberately NOT to
		// DuckDB's TIMESTAMP, even though TIMESTAMP is in the allowlist and is the tempting
		// choice. Measured: inserting "1700000000123" into a DuckDB TIMESTAMP column fails with
		// `Conversion Error: timestamp field value out of range`, because DuckDB reads that string
		// as a datetime literal rather than a millisecond count. Mapping it there would break
		// every timestamped feed at the first row.
		"TIMESTAMP": "BIGINT",

		// DECIMAL stays VARCHAR, and now says so on purpose. DuckDB's bare DECIMAL is
		// DECIMAL(18,3); the feed's exact-decimal type has no digit cap at all. Measured:
		// "123456789012345678901234567890.12345678901234567890" fails with
		// `Could not convert string ... to DECIMAL(18,3)`. VARCHAR is the only destination that
		// keeps the digits the type exists to preserve.
		"DECIMAL": "VARCHAR",

		// An unknown type must land somewhere storable rather than produce invalid DDL.
		"SOMETHING": "VARCHAR",
	}
	for in, want := range cases {
		if got := duckType(in); got != want {
			t.Errorf("duckType(%q) = %q, want %q", in, got, want)
		}
	}
	// Every type this function can produce must be in the allowlist, or ensureDuckTable would refuse
	// DDL that duckType itself generated. That is the kind of gap a rename opens silently.
	for _, in := range []string{
		"INTEGER", "BOOLEAN", "FLOAT", "VARCHAR(32)", "TEXT", "SOMETHING",
		"BIGINT", "DECIMAL", "TIMESTAMP",
	} {
		if !duckTypes[duckType(in)] {
			t.Errorf("duckType(%q) produced %q, which is not in duckTypes", in, duckType(in))
		}
	}
}

// **The wide types, end to end into a real DuckDB file.**
//
// `TestDuckTypeMapping` pins the string a type maps to. That is not the same claim as "a wide
// value survives", because the mapping is only right if DuckDB actually accepts what the feed
// sends into the column the mapping chose — the feed ships BIGINT, DECIMAL and TIMESTAMP as JSON
// strings, so every one of these is a string going into a non-VARCHAR column.
//
// This drives the real sink and reads the values back, so a mapping that produces valid DDL and
// then rejects every row cannot pass.
func TestWideTypesLandExactlyInDuckDB(t *testing.T) {
	s := newSink(t)
	create := &Event{Table: "w", Op: "CREATE_TABLE", CommitLSN: 1, CommitEndLSN: 2, After: map[string]any{
		"columns": []any{
			map[string]any{"name": "id", "type": "INTEGER", "nullable": false},
			map[string]any{"name": "big", "type": "BIGINT", "nullable": true},
			map[string]any{"name": "ts", "type": "TIMESTAMP", "nullable": true},
			map[string]any{"name": "dec", "type": "DECIMAL", "nullable": true},
		},
	}}
	if err := s.apply(create); err != nil {
		t.Fatalf("create: %v", err)
	}

	// The declared column types must be the ones the mapping chose, or the assertions below could
	// pass against a table that stored everything as text.
	for col, want := range map[string]string{"big": "BIGINT", "ts": "BIGINT", "dec": "VARCHAR"} {
		var got string
		err := s.db.QueryRow(
			`SELECT data_type FROM duckdb_columns() WHERE table_name='w' AND column_name=?`, col,
		).Scan(&got)
		if err != nil {
			t.Fatalf("read declared type of %s: %v", col, err)
		}
		if got != want {
			t.Errorf("column %s was declared %s, want %s", col, got, want)
		}
	}

	// Exactly as the feed sends them: JSON strings, at the extremes a double could not hold.
	const bigMax = "9223372036854775807"
	const decManyDigits = "123456789012345678901234567890.12345678901234567890"
	const tsMillis = "1700000000123"
	if err := s.apply(ins("w", 5, map[string]any{
		"id": int64(1), "big": bigMax, "ts": tsMillis, "dec": decManyDigits,
	})); err != nil {
		t.Fatalf("a wide row was refused by the sink: %v", err)
	}

	// Read back as text so the comparison is on digits rather than on Go's rendering of them.
	var big, ts, dec string
	err := s.db.QueryRow(
		`SELECT CAST(big AS VARCHAR), CAST(ts AS VARCHAR), CAST("dec" AS VARCHAR) FROM w WHERE id=1`,
	).Scan(&big, &ts, &dec)
	if err != nil {
		t.Fatalf("read back: %v", err)
	}
	if big != bigMax {
		t.Errorf("BIGINT landed as %s, want %s", big, bigMax)
	}
	if ts != tsMillis {
		t.Errorf("TIMESTAMP landed as %s, want %s", ts, tsMillis)
	}
	if dec != decManyDigits {
		t.Errorf("DECIMAL landed as %s, want %s", dec, decManyDigits)
	}

	// BIGINT must be a NUMBER in the destination, not digits in a string: the whole reason to map
	// it onto a real 64-bit column is that ordering and arithmetic work. Under the old VARCHAR
	// fallthrough this comparison sorted lexicographically and "9223372036854775807" < "99".
	var above bool
	if err := s.db.QueryRow(`SELECT big > 99 FROM w WHERE id=1`).Scan(&above); err != nil {
		t.Fatalf("compare big numerically: %v", err)
	}
	if !above {
		t.Errorf("i64::MAX did not compare greater than 99; the column is not ordering as a number")
	}
}

func TestDuckTypeOfInfersFromTheValue(t *testing.T) {
	cases := []struct {
		v    any
		want string
	}{
		{json.Number("42"), "BIGINT"},
		{json.Number("-42"), "BIGINT"},
		{json.Number("1.5"), "DOUBLE"},
		{"hello", "VARCHAR"},
		{true, "BOOLEAN"},
		// A NULL in the first row says nothing about the column's type, so the widest thing wins.
		{nil, "VARCHAR"},
	}
	for _, c := range cases {
		if got := duckTypeOf(c.v); got != c.want {
			t.Errorf("duckTypeOf(%#v) = %q, want %q", c.v, got, c.want)
		}
		if !duckTypes[duckTypeOf(c.v)] {
			t.Errorf("duckTypeOf(%#v) produced %q, which is not in duckTypes", c.v, duckTypeOf(c.v))
		}
	}
}

func newSink(t *testing.T) *DuckSink {
	t.Helper()
	s, err := openDuckSink(filepath.Join(t.TempDir(), "t.duckdb"), "id")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

// The type string is concatenated into DDL, where it cannot be parameterised. The allowlist is the
// only thing between the feed and that concatenation, so it has to be shown firing.
func TestEnsureDuckTableRefusesATypeOutsideTheAllowlist(t *testing.T) {
	s := newSink(t)
	err := s.ensureDuckTable("t", []map[string]any{
		{"name": "id", "type": "BIGINT"},
		{"name": "evil", "type": "VARCHAR); DROP TABLE t; --"},
	})
	if err == nil {
		t.Fatal("a type outside the allowlist was concatenated into DDL")
	}
	if !strings.Contains(err.Error(), "not one this sink will emit") {
		t.Errorf("refused for the wrong reason: %v", err)
	}
	// And the well-formed neighbour must still be accepted, or the allowlist is just broken.
	if err := s.ensureDuckTable("t", []map[string]any{
		{"name": "id", "type": "BIGINT"},
		{"name": "ok", "type": "VARCHAR"},
	}); err != nil {
		t.Fatalf("a legitimate table was refused: %v", err)
	}
}

// No key column means no conflict target, which means no ordering guard — absent, not degraded.
// Refusing is the only honest answer; creating the table would produce a destination that corrupts
// itself on the first replay and says nothing.
func TestEnsureDuckTableRefusesATableWithoutTheKey(t *testing.T) {
	s := newSink(t)
	err := s.ensureDuckTable("t", []map[string]any{{"name": "other", "type": "BIGINT"}})
	if err == nil {
		t.Fatal("a table with no key column was created; ON CONFLICT would have nothing to attach to")
	}
	if !strings.Contains(err.Error(), "ordering guard") {
		t.Errorf("refused for the wrong reason: %v", err)
	}
}

func ins(table string, lsn uint64, row map[string]any) *Event {
	return &Event{Table: table, Op: "INSERT", CommitLSN: lsn, CommitEndLSN: lsn + 1, After: row}
}

// Rows are built with json.Number, not int64, because that is what `decodeLine` produces — a test
// that fed the sink plain Go integers would not exercise `normalise` at all.
func row(id int64, item string, qty int64) map[string]any {
	return map[string]any{
		"id":   json.Number(strconv.FormatInt(id, 10)),
		"item": item,
		"qty":  json.Number(strconv.FormatInt(qty, 10)),
	}
}

func (s *DuckSink) scanRow(t *testing.T, id int64) (item string, qty int64, lsn int64, deleted bool) {
	t.Helper()
	err := s.db.QueryRow(`SELECT item, qty, "_commit_lsn", "_deleted" FROM inv WHERE id = ?`, id).
		Scan(&item, &qty, &lsn, &deleted)
	if err != nil {
		t.Fatalf("read id %d: %v", id, err)
	}
	return
}

func schemaEvent(table string) *Event {
	return &Event{Table: table, Op: "CREATE_TABLE", CommitLSN: 1, CommitEndLSN: 2, After: map[string]any{
		"columns": []any{
			map[string]any{"name": "id", "type": "INTEGER", "nullable": false},
			map[string]any{"name": "item", "type": "VARCHAR(32)", "nullable": true},
			map[string]any{"name": "qty", "type": "INTEGER", "nullable": true},
		},
	}}
}

// The guard, exercised directly rather than through a feed file: a stale event handed to apply()
// AFTER a newer one must not land. Nothing in Go control flow is between these calls and the SQL,
// which is the whole claim.
func TestApplyRejectsAStaleEventAndAcceptsANewerOne(t *testing.T) {
	s := newSink(t)
	if err := s.apply(schemaEvent("inv")); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.apply(ins("inv", 100, row(1, "widget", 10))); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := s.apply(&Event{Table: "inv", Op: "UPDATE", CommitLSN: 200, CommitEndLSN: 201,
		Before: row(1, "widget", 10), After: row(1, "widget", 999)}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, qty, lsn, _ := s.scanRow(t, 1); qty != 999 || lsn != 200 {
		t.Fatalf("the update did not land: qty=%d lsn=%d", qty, lsn)
	}

	// Stale: the original insert, re-delivered after the update. Applying it would revert live data.
	if err := s.apply(ins("inv", 100, row(1, "widget", 10))); err != nil {
		t.Fatalf("stale apply errored (it should be a silent no-op): %v", err)
	}
	if _, qty, lsn, _ := s.scanRow(t, 1); qty != 999 || lsn != 200 {
		t.Errorf("a stale event overwrote newer data: qty=%d lsn=%d, want 999/200", qty, lsn)
	}

	// Equal LSN is also stale: strictly greater, not greater-or-equal. An at-least-once feed
	// re-delivers the SAME event, so this is the common case, not a boundary curiosity.
	if err := s.apply(ins("inv", 200, row(1, "sabotage", 1))); err != nil {
		t.Fatalf("equal-LSN apply errored: %v", err)
	}
	if item, qty, _, _ := s.scanRow(t, 1); item != "widget" || qty != 999 {
		t.Errorf("an event at the SAME commit_lsn overwrote the row: item=%q qty=%d", item, qty)
	}

	// Newer must still get through, or the guard is just a wall.
	if err := s.apply(ins("inv", 300, row(1, "widget", 5))); err != nil {
		t.Fatalf("newer apply: %v", err)
	}
	if _, qty, lsn, _ := s.scanRow(t, 1); qty != 5 || lsn != 300 {
		t.Errorf("a newer event was rejected: qty=%d lsn=%d, want 5/300", qty, lsn)
	}
}

// Deletes are soft because a hard delete throws away the LSN that rejects a stale re-insert. This
// checks both halves: the tombstone exists, and it does its job.
func TestDeleteIsSoftAndRejectsAStaleResurrection(t *testing.T) {
	s := newSink(t)
	if err := s.apply(schemaEvent("inv")); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := s.apply(ins("inv", 100, row(2, "gadget", 20))); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := s.apply(&Event{Table: "inv", Op: "DELETE", CommitLSN: 200, CommitEndLSN: 201,
		Before: row(2, "gadget", 20)}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	_, _, lsn, deleted := s.scanRow(t, 2)
	if !deleted {
		t.Error("the delete did not leave a tombstone")
	}
	if lsn != 200 {
		t.Errorf("the tombstone lost the deleting commit's lsn: %d", lsn)
	}

	// The re-insert a hard delete would have accepted.
	if err := s.apply(ins("inv", 100, row(2, "gadget", 20))); err != nil {
		t.Fatalf("resurrection apply errored: %v", err)
	}
	if _, _, _, deleted := s.scanRow(t, 2); !deleted {
		t.Error("a stale INSERT resurrected a deleted row")
	}
}

// On restart with no CREATE_TABLE in the feed segment, the destination's own catalog is better
// evidence than the first row that happens along. Guessing produces an INSERT naming a column that
// does not exist — this asserts the guess is not made when there is something to ask.
func TestColumnsComeFromTheCatalogNotFromGuessing(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "restart.duckdb")

	first, err := openDuckSink(path, "id")
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if err := first.apply(schemaEvent("inv")); err != nil {
		t.Fatalf("create: %v", err)
	}
	if err := first.apply(ins("inv", 100, row(1, "widget", 10))); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	// A new process. It has never seen the CREATE_TABLE, and the first row it gets carries only two
	// of the three columns — inference would produce {id, qty} and lose `item` from its idea of the
	// table, which is exactly the disagreement the catalog lookup exists to prevent.
	second, err := openDuckSink(path, "id")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer second.Close()
	partial := map[string]any{"id": json.Number("1"), "qty": json.Number("777")}
	if err := second.apply(&Event{Table: "inv", Op: "UPDATE", CommitLSN: 300, CommitEndLSN: 301,
		Before: partial, After: partial}); err != nil {
		t.Fatalf("apply after restart: %v", err)
	}
	if got := second.columns["inv"]; len(got) != 3 || got[0] != "id" || got[1] != "item" || got[2] != "qty" {
		t.Errorf("columns were guessed, not read from the catalog: %v", got)
	}

	// What the partial image did to `item` is worth pinning rather than leaving to be discovered.
	// This sink writes the WHOLE row every time, like the SQLite one, so a column the event omitted
	// lands as NULL — it does not merge. ferrodb's feed always emits full before/after images, so
	// this is a documented assumption about the source rather than a bug, and a feed that ever
	// emitted partial images would need a different statement, not a different caller.
	var item sql.NullString
	var qty, lsn int64
	if err := second.db.QueryRow(
		`SELECT item, qty, "_commit_lsn" FROM inv WHERE id = 1`).Scan(&item, &qty, &lsn); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if qty != 777 || lsn != 300 {
		t.Errorf("the post-restart update did not land: qty=%d lsn=%d", qty, lsn)
	}
	if item.Valid {
		t.Errorf("a column absent from the event's row image survived as %q; this sink replaces "+
			"whole rows and that is what the destination must show", item.String)
	}
	// Bookkeeping columns must not be mistaken for source columns, or the sink would try to read
	// `_commit_lsn` out of every event's row image and write NULL into a NOT NULL column.
	for _, c := range second.columns["inv"] {
		if bookkeeping[c] {
			t.Errorf("bookkeeping column %q leaked into the source column list", c)
		}
	}
}

// A row with no key column has no conflict target, so it would land as a fresh insert every time it
// was re-delivered. Say so rather than duplicating it.
func TestApplyRefusesARowWithNoKeyColumn(t *testing.T) {
	s := newSink(t)
	if err := s.apply(schemaEvent("inv")); err != nil {
		t.Fatalf("create: %v", err)
	}
	err := s.apply(&Event{Table: "inv", Op: "INSERT", CommitLSN: 100, CommitEndLSN: 101,
		After: map[string]any{"item": "orphan", "qty": json.Number("1")}})
	if err == nil {
		t.Fatal("a row with no key column was accepted")
	}
}

// A typo in -engine that quietly landed the feed somewhere other than where the operator asked is
// worse than an error: the destination they were watching stays empty and the one they were not
// fills up.
func TestOpenDestinationRefusesAnUnknownEngine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "x.db")
	if _, err := openDestination("duckdbb", path, "id"); err == nil {
		t.Fatal("an unknown engine was accepted")
	}
	for _, e := range []string{"sqlite", "duckdb"} {
		s, err := openDestination(e, filepath.Join(t.TempDir(), e), "id")
		if err != nil {
			t.Fatalf("engine %q was refused: %v", e, err)
		}
		s.Close()
	}
}

// A table first created by INFERENCE and then described by an authoritative CREATE_TABLE must not
// keep the guess silently.
//
// `CREATE TABLE IF NOT EXISTS` makes that disagreement a no-op, which is the one door that bypasses
// the typing DuckDB is being used for: `qty` inferred VARCHAR from a null first row would go on
// storing 42 as the string "42" for the life of the table, reading back looking fine.
func TestALaterCreateTableThatDisagreesIsRefused(t *testing.T) {
	s := newSink(t)

	// A first row whose qty is null: nothing to infer from, so it becomes VARCHAR.
	if err := s.apply(ins("inv", 100, map[string]any{
		"id": json.Number("1"), "item": "widget", "qty": nil,
	})); err != nil {
		t.Fatalf("first insert: %v", err)
	}
	_, types, err := s.catalogSchema("inv")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if types[2] != "VARCHAR" {
		t.Fatalf("precondition failed: qty inferred as %q, so this test is not exercising the gap", types[2])
	}

	// The authoritative schema now arrives and says BIGINT. Silently ignoring it is the bug.
	err = s.apply(schemaEvent("inv"))
	if err == nil {
		t.Fatal("a CREATE_TABLE disagreeing with the destination was silently ignored; " +
			"qty would keep storing integers as strings")
	}
	if !strings.Contains(err.Error(), "BIGINT") || !strings.Contains(err.Error(), "VARCHAR") {
		t.Errorf("the error does not name both types, so it cannot be acted on: %v", err)
	}
}

// Re-emission is the COMMON case: a CREATE_TABLE is re-sent at every checkpoint of the source. The
// refusal above must not fire on it, or every table breaks on its second checkpoint.
func TestARepeatedIdenticalCreateTableIsStillANoOp(t *testing.T) {
	s := newSink(t)
	for i := 0; i < 3; i++ {
		if err := s.apply(schemaEvent("inv")); err != nil {
			t.Fatalf("CREATE_TABLE #%d was refused, but it is identical: %v", i+1, err)
		}
	}
	if err := s.apply(ins("inv", 100, row(1, "widget", 10))); err != nil {
		t.Fatalf("insert after re-emitted schema: %v", err)
	}
}

// The allowlist proven on the path the FEED actually takes, not by calling ensureDuckTable directly.
//
// `duckType` collapses anything unrecognised to VARCHAR, so a hostile type string in a CREATE_TABLE
// event should never reach the DDL intact. That is the claim; this drives it through `apply` — the
// only entry point the feed has — rather than reaching past it.
func TestAHostileTypeInACreateTableEventCannotReachTheDDL(t *testing.T) {
	s := newSink(t)
	evil := "VARCHAR); DROP TABLE inv; CREATE TABLE pwned(x INTEGER"
	err := s.apply(&Event{Table: "inv", Op: "CREATE_TABLE", CommitLSN: 1, CommitEndLSN: 2,
		After: map[string]any{"columns": []any{
			map[string]any{"name": "id", "type": "INTEGER"},
			map[string]any{"name": "qty", "type": evil},
		}}})
	if err != nil {
		t.Fatalf("the event was refused outright: %v", err)
	}
	// The hostile type must have been collapsed to VARCHAR, not concatenated.
	names, types, err := s.catalogSchema("inv")
	if err != nil {
		t.Fatalf("catalog: %v", err)
	}
	if len(names) != 2 || types[1] != "VARCHAR" {
		t.Errorf("the DDL did not come out as expected: %v / %v", names, types)
	}
	// And the injected statement must not have run.
	if cols, err := s.catalogColumns("pwned"); err != nil || len(cols) != 0 {
		t.Errorf("the injected CREATE TABLE ran: cols=%v err=%v", cols, err)
	}
}

// The fallback reader has to render EXACTLY like the `duckdb` CLI, or an assertion written against
// the CLI fails on a machine that fell back — and reports as a data disagreement rather than a
// rendering one, sending the reader after a corruption that is not there.
//
// Expectations measured against `duckdb -noheader -list` v1.5.5, not assumed.
func TestRenderCellMatchesTheDuckdbCLI(t *testing.T) {
	ts := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	cases := []struct {
		in   any
		want string
	}{
		// The CLI prints the four characters NULL, not an empty cell.
		{nil, "NULL"},
		{true, "true"},
		{false, "false"},
		{[]byte("ab"), "ab"},
		{int64(7), "7"},
		{1.5, "1.5"},
		// A DOUBLE holding an integral value keeps its point; fmt.Sprint would give "2".
		{float64(2), "2.0"},
		{ts, "2024-01-02 03:04:05"},
		{ts.Add(123 * time.Millisecond), "2024-01-02 03:04:05.123"},
	}
	for _, c := range cases {
		if got := renderCell(c.in); got != c.want {
			t.Errorf("renderCell(%#v) = %q, want %q", c.in, got, c.want)
		}
	}
}

// duckdbCLI finds the `duckdb` CLI, and refuses to report it missing when the environment has said
// it must be there.
//
// The same contract as the Rust suite's guard, deliberately: the CLI is optional on a developer
// machine and mandatory under FERRODB_REQUIRE_DUCKDB_CLI=1, which CI sets. Returning "" is how a
// developer machine legitimately opts out; it is never how a misconfigured CI run opts out, because
// that path fails the test instead.
func duckdbCLI(t *testing.T) string {
	t.Helper()
	// Parsed FIRST, and on every call, rather than only when no CLI turned up. A typo like
	// `FERRODB_REQUIRE_DUCKDB_CLI=true` would otherwise sit unnoticed on every machine that happens
	// to have a CLI, and stop being a requirement on the exact day one went missing.
	//
	// An unrecognised value is refused rather than read as "not required": a guard that falls
	// through to allow when it cannot parse its own input waves through exactly the
	// misconfiguration it exists to catch.
	required := false
	switch v := strings.TrimSpace(os.Getenv("FERRODB_REQUIRE_DUCKDB_CLI")); v {
	case "", "0":
	case "1":
		required = true
	default:
		t.Fatalf("FERRODB_REQUIRE_DUCKDB_CLI is %q; it takes 1 or 0", v)
	}
	for _, c := range []string{"duckdb", "/opt/homebrew/bin/duckdb", "/usr/local/bin/duckdb"} {
		if err := exec.Command(c, "--version").Run(); err == nil {
			return c
		}
	}
	if required {
		t.Fatal("FERRODB_REQUIRE_DUCKDB_CLI=1, but no `duckdb` CLI is on PATH; the rendering " +
			"expectations below would then be checked against nothing but themselves")
	}
	return ""
}

// The hardcoded table above is a claim about what a program on ANOTHER machine prints, and nothing
// in this package could contradict it — the expectations were measured once, by hand, and would go
// on passing forever after the CLI changed or after someone mistyped one of them.
//
// This runs the real CLI and the fallback reader over the same values and compares the two. It is
// the unit-level twin of the Rust suite's `both_readers_agree`, and it covers the cases that can
// actually differ: NULL, a DOUBLE holding an integral value, and timestamps with and without a
// fractional part. Anything returning plain non-null scalars agrees by accident and proves nothing.
func TestRenderCellAgreesWithTheRealDuckdbCLI(t *testing.T) {
	cli := duckdbCLI(t)
	if cli == "" {
		// Reachable only on a developer machine with no CLI. CI cannot land here: the guard above
		// fails the test instead, so this is never how a CI run goes green.
		t.Skip("no duckdb CLI on this machine; set FERRODB_REQUIRE_DUCKDB_CLI=1 to make this fatal")
	}
	const probe = `SELECT NULL, CAST(2 AS DOUBLE), CAST(1.5 AS DOUBLE), CAST(7 AS BIGINT), ` +
		`true, false, CAST('2024-01-02 03:04:05' AS TIMESTAMP), ` +
		`CAST('2024-01-02 03:04:05.123' AS TIMESTAMP);`

	// The CLI with no database argument runs in memory, which is all this needs.
	out, err := exec.Command(cli, "-noheader", "-list", "-c", probe).Output()
	if err != nil {
		t.Fatalf("run the duckdb CLI: %v", err)
	}
	// The CLI terminates rows with CRLF on Windows; duckSQL joins with "\n" everywhere. The probe
	// below is a single row, so TrimSpace would cover it today - normalised anyway, because the
	// day someone adds a second row is not the day to rediscover this on the Windows runner only.
	// Only the row separator, so a bare \r inside a value still registers as a difference.
	want := strings.TrimSpace(strings.ReplaceAll(string(out), "\r\n", "\n"))

	got, err := duckSQL(filepath.Join(t.TempDir(), "probe.duckdb"), probe)
	if err != nil {
		t.Fatalf("run the fallback reader: %v", err)
	}
	got = strings.TrimSpace(got)

	if want == "" {
		t.Fatal("the CLI printed nothing, so the comparison would be vacuous")
	}
	if got != want {
		t.Errorf("the fallback reader and the duckdb CLI disagree:\n  CLI:      %q\n  fallback: %q", want, got)
	}
}
