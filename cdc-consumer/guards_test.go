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
	"fmt"
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

// The two malformed-source-dump guards, which the pipeline cannot produce and so cannot force.
//
// `table_dump` always emits one object per live row with the primary key present, so a Rust
// integration test driving the real pipeline can never reach either of these. They matter because the
// source dump is the diff's expected side: if it silently accepted a dump with two rows for one key,
// whichever row happened to land last in the map would become "the source" and half the comparison
// would be against a value nobody wrote.
func TestDiffRefusesAMalformedSourceDump(t *testing.T) {
	dir := t.TempDir()
	feed := filepath.Join(dir, "feed.jsonl")
	if err := os.WriteFile(feed, []byte(goodLine), 0o644); err != nil {
		t.Fatalf("write feed: %v", err)
	}

	write := func(name, body string) string {
		p := filepath.Join(dir, name)
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
		return p
	}

	// Two rows claiming the same key: not a table state.
	dup := write("dup.json", `[{"id":1,"v":10},{"id":1,"v":11}]`)
	err := diffAgainstSource(feed, dup, "id")
	if err == nil {
		t.Fatal("a source dump with two rows for one key was accepted")
	}
	if !strings.Contains(err.Error(), "two rows with key") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}

	// A row with no key column at all: there is nothing to compare it by.
	nokey := write("nokey.json", `[{"v":10}]`)
	err = diffAgainstSource(feed, nokey, "id")
	if err == nil {
		t.Fatal("a source row with no key column was accepted")
	}
	if !strings.Contains(err.Error(), "no key column") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}

	// Not an array at all.
	notarray := write("notarray.json", `{"id":1}`)
	if err := diffAgainstSource(feed, notarray, "id"); err == nil {
		t.Fatal("a source dump that is not an array of rows was accepted")
	}

	// Anti-vacuity: the well-formed dump matching that feed passes, so the three refusals above are
	// about the dumps and not about `diff` rejecting everything.
	good := write("good.json", `[{"id":1}]`)
	if err := diffAgainstSource(feed, good, "id"); err != nil {
		t.Fatalf("a well-formed source dump matching the feed was refused: %v", err)
	}
}

// **A commit carries many rows, and every one of them has to land.**
//
// The idempotence key was `commit_lsn` alone. A commit_lsn identifies a TRANSACTION, so every row of
// a multi-row statement — and every row of a backfill snapshot, which emits its whole scan at one
// LSN — shares it. The first row applied and advanced the cursor to that commit; every sibling then
// compared `commit_lsn <= cursor` and was discarded as a re-delivery.
//
// Measured on the shipped binary before the fix: a 3-row commit landed ONE row, printed
// `APPLIED 2 SKIPPED 2`, and exited 0. Silent, self-consistent data loss in the direction a CDC
// pipeline must never fail — and it is the backfill path, so it scaled with the size of the table.
//
// The key is now the composite (commit_lsn, record lsn), compared lexicographically.
func TestEveryRowOfOneCommitLands(t *testing.T) {
	dir := t.TempDir()
	// Three rows, one transaction: same txn and commit_lsn, distinct record lsn.
	var b strings.Builder
	b.WriteString(`{"op":"CREATE_TABLE","txn":1,"lsn":1,"commit_lsn":1,"commit_end_lsn":2,"table":"t","before":null,` +
		`"after":{"columns":[{"name":"id","type":"INTEGER","nullable":false},{"name":"v","type":"INTEGER","nullable":true}]}}` + "\n")
	for i, lsn := range []int{100, 101, 102} {
		fmt.Fprintf(&b, `{"op":"INSERT","txn":9,"lsn":%d,"commit_lsn":100,"commit_end_lsn":103,"table":"t",`+
			`"before":null,"after":{"id":%d,"v":%d}}`+"\n", lsn, i+1, (i+1)*10)
	}
	feed := filepath.Join(dir, "feed.jsonl")
	if err := os.WriteFile(feed, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("write feed: %v", err)
	}

	db := filepath.Join(dir, "out.sqlite")
	if err := runSink(feed, db, "id", "sqlite"); err != nil {
		t.Fatalf("sink: %v", err)
	}

	s, err := openSink(db, "id")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()
	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 3 {
		t.Fatalf("a 3-row commit landed %d row(s); rows sharing one commit_lsn are being discarded as "+
			"re-deliveries", n)
	}

	// Replaying the same feed must still change nothing — the fix must not have bought row coverage by
	// giving up idempotence, which is the whole reason the cursor exists.
	if err := runSink(feed, db, "id", "sqlite"); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
		t.Fatalf("recount: %v", err)
	}
	if n != 3 {
		t.Fatalf("replaying the feed changed the row count to %d", n)
	}

	// And the cursor advanced to the LAST record of the commit, not the first.
	cc, cl := s.cursor("t")
	if cc != 100 || cl != 102 {
		t.Fatalf("cursor is (%d, %d); expected (100, 102) — the resume point must be the last record "+
			"absorbed, or a restart re-reads rows it already applied", cc, cl)
	}
}

// **Two writes to ONE key inside one commit: the later record must win.**
//
// The cursor filter cannot decide this — both events pass it — so the answer rests entirely on the
// `ON CONFLICT ... WHERE` clause, and a guard of `excluded._commit_lsn > _commit_lsn` is FALSE when
// the two share a commit. The row then keeps its first value and the second write is silently
// dropped.
//
// Found by mutation: deleting the lexicographic half of that clause left the whole Go suite green,
// because every other test writes each key in its own commit. This is the case that fires.
func TestTheLaterRecordInOneCommitWinsForTheSameKey(t *testing.T) {
	dir := t.TempDir()
	feed := filepath.Join(dir, "feed.jsonl")
	body := `{"op":"CREATE_TABLE","txn":1,"lsn":1,"commit_lsn":1,"commit_end_lsn":2,"table":"t","before":null,` +
		`"after":{"columns":[{"name":"id","type":"INTEGER","nullable":false},{"name":"v","type":"INTEGER","nullable":true}]}}` + "\n" +
		`{"op":"INSERT","txn":9,"lsn":100,"commit_lsn":100,"commit_end_lsn":103,"table":"t","before":null,` +
		`"after":{"id":1,"v":10}}` + "\n" +
		`{"op":"UPDATE","txn":9,"lsn":101,"commit_lsn":100,"commit_end_lsn":103,"table":"t",` +
		`"before":{"id":1,"v":10},"after":{"id":1,"v":99}}` + "\n"
	if err := os.WriteFile(feed, []byte(body), 0o644); err != nil {
		t.Fatalf("write feed: %v", err)
	}

	db := filepath.Join(dir, "out.sqlite")
	if err := runSink(feed, db, "id", "sqlite"); err != nil {
		t.Fatalf("sink: %v", err)
	}
	s, err := openSink(db, "id")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()

	var v int
	if err := s.db.QueryRow(`SELECT v FROM t WHERE id = 1`).Scan(&v); err != nil {
		t.Fatalf("read back: %v", err)
	}
	if v != 99 {
		t.Fatalf("the row holds v=%d; the second write of the same key in one commit was rejected "+
			"because the ordering guard compares commit_lsn only", v)
	}

	// Anti-vacuity: a genuinely STALE write - lower commit_lsn entirely - is still rejected, so the
	// widened guard did not simply become last-write-wins.
	stale := filepath.Join(dir, "stale.jsonl")
	staleBody := `{"op":"UPDATE","txn":8,"lsn":50,"commit_lsn":50,"commit_end_lsn":51,"table":"t",` +
		`"before":{"id":1,"v":99},"after":{"id":1,"v":7}}` + "\n"
	if err := os.WriteFile(stale, []byte(staleBody), 0o644); err != nil {
		t.Fatalf("write stale: %v", err)
	}
	if _, err := s.db.Exec(`DELETE FROM _cdc_checkpoint`); err != nil {
		t.Fatalf("clear cursor: %v", err)
	}
	if _, _, _, _, err := applyFeed(s, staleBody); err != nil {
		t.Fatalf("apply stale: %v", err)
	}
	if err := s.db.QueryRow(`SELECT v FROM t WHERE id = 1`).Scan(&v); err != nil {
		t.Fatalf("re-read: %v", err)
	}
	if v != 99 {
		t.Fatalf("a stale write from an older commit landed: v=%d", v)
	}
}

// **A backfill snapshot: every row shares one LSN, and every row must still land.**
//
// `replication::snapshot` stamps every row of a snapshot with the same lsn, commit_lsn AND
// commit_end_lsn — it is one logical batch taken at one position. So the composite (commit_lsn, lsn)
// key that fixed multi-row COMMITS cannot separate snapshot rows: they are identical on both halves.
// The first row still advanced the cursor past all its siblings.
//
// Measured before the fix: a 3-row snapshot landed ONE row, printed `APPLIED 2 SKIPPED 2`, exit 0. A
// full-table backfill of any size landed one row, silently. This is the worse half of the bug the
// composite key was meant to fix, and it survived that fix.
func TestEveryRowOfASnapshotLands(t *testing.T) {
	dir := t.TempDir()
	schema := `{"op":"CREATE_TABLE","txn":0,"lsn":1,"commit_lsn":1,"commit_end_lsn":2,"table":"t",` +
		`"before":null,"after":{"columns":[{"name":"id","type":"INTEGER","nullable":false},` +
		`{"name":"v","type":"INTEGER","nullable":true}]}}` + "\n"
	var b strings.Builder
	b.WriteString(schema)
	for i := 1; i <= 3; i++ {
		// Same lsn, same commit_lsn, same commit_end_lsn on every row — exactly what snapshot.rs emits.
		fmt.Fprintf(&b, `{"op":"READ","txn":0,"lsn":9,"commit_lsn":9,"commit_end_lsn":9,"table":"t",`+
			`"before":null,"after":{"id":%d,"v":%d}}`+"\n", i, i*10)
	}
	feed := filepath.Join(dir, "snap.jsonl")
	if err := os.WriteFile(feed, []byte(b.String()), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}

	db := filepath.Join(dir, "out.sqlite")
	if err := runSink(feed, db, "id", "sqlite"); err != nil {
		t.Fatalf("sink: %v", err)
	}
	s, err := openSink(db, "id")
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s.Close()

	var n int
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	if n != 3 {
		t.Fatalf("a 3-row snapshot landed %d row(s): rows sharing one LSN are being discarded as "+
			"re-deliveries, so a backfill of any size lands one row", n)
	}

	// Replaying the snapshot must not duplicate or disturb anything. Its idempotence now comes from the
	// upsert's strictly-greater guard rather than from the cursor, so this is the half that proves
	// exempting READ from the cursor did not cost idempotence.
	if err := runSink(feed, db, "id", "sqlite"); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if err := s.db.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&n); err != nil {
		t.Fatalf("recount: %v", err)
	}
	if n != 3 {
		t.Fatalf("replaying the snapshot changed the row count to %d", n)
	}

	// **The cutover hazard, which is why exempting READ is safe rather than reckless.** The snapshot
	// boundary is taken BEFORE the scan, so a snapshot row can arrive after a newer stream event for the
	// same key. It must lose. Nothing but the SQL guard can decide this, because the cursor no longer
	// filters READs at all.
	stream := `{"op":"UPDATE","txn":7,"lsn":100,"commit_lsn":100,"commit_end_lsn":101,"table":"t",` +
		`"before":{"id":1,"v":10},"after":{"id":1,"v":999}}` + "\n"
	if _, _, _, _, err := applyFeed(s, stream); err != nil {
		t.Fatalf("apply stream: %v", err)
	}
	var v int
	if err := s.db.QueryRow(`SELECT v FROM t WHERE id = 1`).Scan(&v); err != nil {
		t.Fatalf("read: %v", err)
	}
	if v != 999 {
		t.Fatalf("the newer stream event did not land: v=%d", v)
	}
	// Now the stale snapshot row for the same key, arriving late.
	staleSnap := `{"op":"READ","txn":0,"lsn":9,"commit_lsn":9,"commit_end_lsn":9,"table":"t",` +
		`"before":null,"after":{"id":1,"v":10}}` + "\n"
	if _, _, _, _, err := applyFeed(s, staleSnap); err != nil {
		t.Fatalf("apply stale snapshot row: %v", err)
	}
	if err := s.db.QueryRow(`SELECT v FROM t WHERE id = 1`).Scan(&v); err != nil {
		t.Fatalf("re-read: %v", err)
	}
	if v != 999 {
		t.Fatalf("a stale snapshot row overwrote a newer stream event: v=%d. Exempting READ from the "+
			"cursor is only safe because the upsert guard rejects it", v)
	}
}
