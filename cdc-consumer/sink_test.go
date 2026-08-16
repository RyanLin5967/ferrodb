package main

import (
	"encoding/json"
	"strings"
	"testing"
)

// The Go side had no tests of its own. It is an independent implementation used to check the Rust
// producer, which makes it load-bearing — an independent checker that is itself wrong agrees with
// nothing and reports confidently.

func TestQuoteIdentDoublesEmbeddedQuotes(t *testing.T) {
	// A column named `we"ird` is a column name, not an attack, and it must round-trip. Doubling the
	// embedded quote is the whole of SQLite's escape; getting it wrong turns a name into syntax.
	cases := map[string]string{
		`id`:        `"id"`,
		`we"ird`:    `"we""ird"`,
		`"; DROP`:   `"""; DROP"`,
		``:          `""`,
		`a""b`:      `"a""""b"`,
	}
	for in, want := range cases {
		if got := quoteIdent(in); got != want {
			t.Errorf("quoteIdent(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestSQLTypeMapping(t *testing.T) {
	cases := map[string]string{
		"INTEGER":     "INTEGER",
		"BOOLEAN":     "INTEGER",
		"FLOAT":       "REAL",
		"VARCHAR(32)": "TEXT",
		"VARCHAR(1)":  "TEXT",

		// The wide types. These were reaching the `default` arm and being stored as TEXT by
		// accident rather than by decision — the feed's schema event has named them for a while,
		// but `ColumnSpec`'s doc still described the contract as the four types above, and this
		// consumer was written against that list.
		//
		// TEXT is not a harmless place to land a BIGINT. SQLite compares TEXT lexicographically,
		// so `WHERE big > 5` would put "10" below "5", and the doc's promise that "a consumer
		// recreating the column gets the same one" would be false in the way that costs a query
		// its answer. SQLite's INTEGER is 8 bytes, so it holds every i64 exactly, and INTEGER
		// affinity converts the feed's digit-string losslessly on the way in.
		"BIGINT": "INTEGER",
		// Epoch milliseconds, an i64 like any other.
		"TIMESTAMP": "INTEGER",
		// DECIMAL must stay TEXT, and for the opposite reason: it is the one type with no bound on
		// its digits, so REAL would round it and INTEGER would refuse its fraction. Text is what
		// preserves `123456789012345678901234567890.12345678901234567890` intact.
		"DECIMAL": "TEXT",

		// An unknown type must land somewhere storable rather than produce invalid DDL.
		"SOMETHING": "TEXT",
	}
	for in, want := range cases {
		if got := sqlType(in); got != want {
			t.Errorf("sqlType(%q) = %q, want %q", in, got, want)
		}
	}
}

// decodeLine is the boundary the whole independent-checker argument rests on. If it accepts
// malformed input, it stops being a check.
func TestDecodeLineRejectsMalformedEnvelopes(t *testing.T) {
	valid := `{"table":"t","op":"INSERT","txn":1,"lsn":1,"commit_lsn":2,"commit_end_lsn":3,"before":null,"after":{"id":1}}`
	if _, err := decodeLine(valid, 1); err != nil {
		t.Fatalf("a valid line was rejected: %v", err)
	}

	bad := map[string]string{
		"bare NaN is not JSON":        `{"table":"t","op":"INSERT","txn":1,"lsn":1,"commit_lsn":2,"commit_end_lsn":3,"before":null,"after":{"x":NaN}}`,
		"bare Infinity is not JSON":   `{"table":"t","op":"INSERT","txn":1,"lsn":1,"commit_lsn":2,"commit_end_lsn":3,"before":null,"after":{"x":Infinity}}`,
		"unknown op":                  `{"table":"t","op":"FROBNICATE","txn":1,"lsn":1,"commit_lsn":2,"commit_end_lsn":3,"before":null,"after":{"id":1}}`,
		"empty table name":            `{"table":"","op":"INSERT","txn":1,"lsn":1,"commit_lsn":2,"commit_end_lsn":3,"before":null,"after":{"id":1}}`,
		"INSERT carrying a before":    `{"table":"t","op":"INSERT","txn":1,"lsn":1,"commit_lsn":2,"commit_end_lsn":3,"before":{"id":1},"after":{"id":1}}`,
		"DELETE carrying an after":    `{"table":"t","op":"DELETE","txn":1,"lsn":1,"commit_lsn":2,"commit_end_lsn":3,"before":{"id":1},"after":{"id":1}}`,
		"UPDATE missing an image":     `{"table":"t","op":"UPDATE","txn":1,"lsn":1,"commit_lsn":2,"commit_end_lsn":3,"before":null,"after":{"id":1}}`,
		"resume point not past commit": `{"table":"t","op":"INSERT","txn":1,"lsn":1,"commit_lsn":5,"commit_end_lsn":5,"before":null,"after":{"id":1}}`,
		"trailing content":            valid + `{"extra":1}`,
		"not an object":               `["nope"]`,
		"CREATE_TABLE with no columns": `{"table":"t","op":"CREATE_TABLE","txn":0,"lsn":1,"commit_lsn":1,"commit_end_lsn":2,"before":null,"after":{"columns":[]}}`,
	}
	for name, line := range bad {
		if _, err := decodeLine(line, 1); err == nil {
			t.Errorf("%s: accepted, should have been refused", name)
		}
	}
}

// A snapshot READ legitimately stamps all three LSNs the same — it is not a log record and has no
// commit of its own. That exemption must not leak to streamed changes.
func TestReadIsExemptFromTheResumePointRule(t *testing.T) {
	read := `{"table":"t","op":"READ","txn":0,"lsn":9,"commit_lsn":9,"commit_end_lsn":9,"before":null,"after":{"id":1}}`
	if _, err := decodeLine(read, 1); err != nil {
		t.Errorf("a snapshot READ was rejected: %v", err)
	}
	insert := strings.Replace(read, `"op":"READ"`, `"op":"INSERT"`, 1)
	if _, err := decodeLine(insert, 1); err == nil {
		t.Error("an INSERT with commit_end_lsn == commit_lsn was accepted; the exemption leaked")
	}
}

// The materialised table is what the end-to-end tests compare against the source, so its fold has
// to be right independently of the pipeline that feeds it.
func TestTableApplyFoldsOpsCorrectly(t *testing.T) {
	tab := newTable("id")
	ev := func(op string, before, after string) *Event {
		e := &Event{Table: "t", Op: op}
		if before != "" {
			_ = json.Unmarshal([]byte(before), &e.Before)
		}
		if after != "" {
			_ = json.Unmarshal([]byte(after), &e.After)
		}
		return e
	}

	must := func(e *Event) {
		if err := tab.apply(e); err != nil {
			t.Fatalf("apply %s: %v", e.Op, err)
		}
	}
	must(ev("INSERT", "", `{"id":1,"v":10}`))
	must(ev("INSERT", "", `{"id":2,"v":20}`))
	if len(tab.rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(tab.rows))
	}

	// An UPDATE must replace, not accumulate.
	must(ev("UPDATE", `{"id":1,"v":10}`, `{"id":1,"v":99}`))
	if got := tab.rows["1"]["v"]; got != float64(99) && got != json.Number("99") {
		t.Errorf("update did not replace the value: %v", got)
	}
	if len(tab.rows) != 2 {
		t.Errorf("update changed the row count: %d", len(tab.rows))
	}

	// A DELETE must key off the BEFORE image — there is no after image to read.
	must(ev("DELETE", `{"id":2,"v":20}`, ""))
	if _, still := tab.rows["2"]; still {
		t.Error("the deleted row is still present")
	}

	// DROP_TABLE clears everything.
	must(ev("DROP_TABLE", "", ""))
	if len(tab.rows) != 0 {
		t.Errorf("DROP_TABLE left %d rows", len(tab.rows))
	}
}

// A row with no key column cannot be folded honestly, and must say so rather than silently landing
// under a zero value where it would collide with every other keyless row.
func TestApplyRefusesARowWithNoKey(t *testing.T) {
	tab := newTable("id")
	e := &Event{Table: "t", Op: "INSERT", After: map[string]any{"other": 1}}
	if err := tab.apply(e); err == nil {
		t.Error("a row with no key column was accepted")
	}
}
