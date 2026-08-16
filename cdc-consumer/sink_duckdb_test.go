package main

import (
	"database/sql"
	"encoding/json"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
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
	for _, in := range []string{"INTEGER", "BOOLEAN", "FLOAT", "VARCHAR(32)", "TEXT", "SOMETHING"} {
		if !duckTypes[duckType(in)] {
			t.Errorf("duckType(%q) produced %q, which is not in duckTypes", in, duckType(in))
		}
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
