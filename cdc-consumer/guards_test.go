package main

// E56 — the guards that stop a silent no-op being reported as success.
//
// Every subcommand here is exercised end to end by the Rust suite, which spawns this binary and
// checks what it produced. That covers the happy path thoroughly and the error paths not at all:
// a feed that arrives empty, or truncated, or that lands nothing, never happens in a test whose
// whole purpose is to hand over a good feed.
//
// Those are the guards worth having most. A CDC consumer that exits zero on an empty feed has told
// its operator the pipeline is healthy while delivering nothing, and that is indistinguishable from
// success until someone queries the destination. Each is forced here, and each has an anti-vacuity
// half so the refusal is shown to be about the condition rather than about the function refusing
// everything.

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// one well-formed line, so "this feed is fine" is a thing the tests can actually produce
const goodLine = `{"op":"INSERT","txn":1,"lsn":10,"commit_lsn":20,"commit_end_lsn":21,` +
	`"table":"t","before":null,"after":{"id":1}}` + "\n"

func writeFeed(t *testing.T, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "feed.jsonl")
	if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
		t.Fatalf("write feed: %v", err)
	}
	return p
}

// An empty feed is the case where "no violations found" and "nothing was looked at" produce the
// same exit code unless something refuses.
func TestValidateRefusesAnEmptyFeed(t *testing.T) {
	err := validate(writeFeed(t, ""))
	if err == nil {
		t.Fatal("an empty feed validated; a run that collected nothing is not a pass")
	}
	if !strings.Contains(err.Error(), "collected nothing") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}

	// Anti-vacuity: a one-line feed passes, so the refusal is about emptiness.
	if err := validate(writeFeed(t, goodLine)); err != nil {
		t.Fatalf("a well-formed feed was refused: %v", err)
	}
}

// A feed whose last byte is not a newline may have been cut mid-record. Accepting it silently
// drops or corrupts whatever the writer had not finished writing.
func TestValidateRefusesAFeedThatIsMissingItsFinalNewline(t *testing.T) {
	truncated := strings.TrimSuffix(goodLine, "\n")
	err := validate(writeFeed(t, truncated))
	if err == nil {
		t.Fatal("a feed with no trailing newline validated; its last record may be truncated")
	}
	if !strings.Contains(err.Error(), "newline") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}

	// The same bytes plus the newline validate, so this is about the terminator and not the content.
	if err := validate(writeFeed(t, truncated+"\n")); err != nil {
		t.Fatalf("the same record with its newline was refused: %v", err)
	}
}

// The sink's version of the same rule: landing nothing is not landing successfully.
func TestSinkRefusesAnEmptyFeed(t *testing.T) {
	db := filepath.Join(t.TempDir(), "out.sqlite")
	err := runSink(writeFeed(t, ""), db, "id", "sqlite")
	if err == nil {
		t.Fatal("an empty feed was sunk; a sink that landed nothing has not succeeded")
	}
	if !strings.Contains(err.Error(), "landed nothing") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}

	// Anti-vacuity: a real feed lands. Without this, a sink broken for every input would pass above.
	if err := runSink(writeFeed(t, goodLine), db, "id", "sqlite"); err != nil {
		t.Fatalf("a well-formed feed was refused by the sink: %v", err)
	}
}

// A DuckDB sink with no key cannot express "insert or update this row", so it would append
// duplicates on every re-delivery of an at-least-once feed rather than converging.
func TestDuckSinkRefusesWithoutAKeyColumn(t *testing.T) {
	db := filepath.Join(t.TempDir(), "out.duckdb")
	_, err := openDuckSink(db, "")
	if err == nil {
		t.Fatal("a duckdb sink opened with no key column; it cannot upsert")
	}
	if !strings.Contains(err.Error(), "cannot upsert") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}

	// Anti-vacuity: with a key it opens. Otherwise this passes against a sink that never opens.
	s, err := openDuckSink(db, "id")
	if err != nil {
		t.Fatalf("a duckdb sink with a key column was refused: %v", err)
	}
	_ = s.Close()
}
