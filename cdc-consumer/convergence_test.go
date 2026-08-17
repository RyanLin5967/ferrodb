package main

// E57 — at-least-once convergence, as a property rather than a handful of cases.
//
// The feed is at-least-once by construction: the snapshot handoff LSN is taken *before* the scan
// precisely so a racing change is re-delivered rather than skipped, because duplication is
// recoverable and loss is not. That choice is only safe if the sink actually converges, and the
// four claims the README makes about it were each pinned by one hand-picked case.
//
// One case proves one delivery. The guarantee is over all of them, so this generates deliveries.
//
// # What the contract actually is, learned by getting it wrong
//
// The first version of this shuffled the feed arbitrarily and failed immediately — and the sink was
// right, not the test. `applyFeed` skips any event at or below the table's saved cursor, because
// that is what a re-delivery IS for a cursor-based consumer. Under an arbitrary permutation an
// event with a lower commit_lsn arriving late is indistinguishable from a replay, so it is skipped,
// and rows legitimately vanish.
//
// That is correct, because ferrodb's feed is an ORDERED stream: the decoder emits in commit order
// and the wire preserves it. Arbitrary reordering is not something this transport does, and a test
// asserting convergence under it would have reported a design decision as a defect.
//
// So the generated deliveries are the ones at-least-once really produces: duplicates anywhere, and
// replays of an arbitrary prefix — a consumer that crashed and resumed from an older cursor —
// always order-preserving.
//
// # Seeded, not random
//
// A shuffle from an unseeded source that fails once and never reproduces is worse than no test: it
// reports a real defect as flakiness and trains the reader to re-run until green. Every case here
// is derived from a fixed seed, so a failure names the exact delivery that produced it and running
// it again produces the same one.

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// The state a destination is in, as a comparable string: every row, every column, sorted.
func destinationState(t *testing.T, db *sql.DB, table string) string {
	t.Helper()
	rows, err := db.Query(fmt.Sprintf(`SELECT id, item, qty, "_commit_lsn", "_deleted" FROM %s`, quoteIdent(table)))
	if err != nil {
		t.Fatalf("read back %s: %v", table, err)
	}
	defer rows.Close()

	var out []string
	for rows.Next() {
		var id, qty, lsn, deleted sql.NullInt64
		var item sql.NullString
		if err := rows.Scan(&id, &item, &qty, &lsn, &deleted); err != nil {
			t.Fatalf("scan: %v", err)
		}
		out = append(out, fmt.Sprintf("id=%v item=%v qty=%v lsn=%v del=%v",
			id.Int64, item.String, qty.Int64, lsn.Int64, deleted.Int64))
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	sort.Strings(out)
	return strings.Join(out, "\n")
}

// A schema event carries a column list, not a row — the shape the producer actually emits.
func schemaLine() string {
	e := map[string]any{
		"op": "CREATE_TABLE", "txn": 1, "lsn": 1, "commit_lsn": 1, "commit_end_lsn": 2,
		"table": "inv", "before": nil,
		"after": map[string]any{"columns": []map[string]any{
			{"name": "id", "type": "INTEGER", "nullable": false},
			{"name": "item", "type": "VARCHAR(32)", "nullable": true},
			{"name": "qty", "type": "INTEGER", "nullable": true},
		}},
	}
	b, _ := json.Marshal(e)
	return string(b)
}

func event(op string, id int, item string, qty int, commitLSN uint64) string {
	after := map[string]any{"id": id, "item": item, "qty": qty}
	e := map[string]any{
		"op": op, "txn": commitLSN, "lsn": commitLSN, "commit_lsn": commitLSN,
		"commit_end_lsn": commitLSN + 1, "table": "inv", "before": nil, "after": after,
	}
	switch op {
	case "DELETE":
		e["before"] = after
		e["after"] = nil
	case "UPDATE":
		// An UPDATE carries both images. The before-image's contents do not matter to the ordering
		// guard - that is decided by commit_lsn - but a feed missing one is malformed and the
		// consumer is right to refuse it.
		before := map[string]any{"id": id, "item": item, "qty": qty - 1}
		e["before"] = before
	}
	b, _ := json.Marshal(e)
	return string(b)
}

// A workload with every shape that makes ordering matter: repeated updates to one row, a delete,
// and a re-insert after that delete.
func workload() []string {
	return []string{
		schemaLine(),
		event("INSERT", 1, "widget", 10, 10),
		event("INSERT", 2, "gadget", 20, 11),
		event("UPDATE", 1, "widget", 15, 20),
		event("UPDATE", 1, "widget", 99, 30), // the newest write to row 1
		event("DELETE", 2, "gadget", 20, 40),
		event("INSERT", 3, "cog", 5, 50),
		event("UPDATE", 3, "cog", 7, 60),
	}
}

func applyTo(t *testing.T, dir string, name string, lines []string) string {
	t.Helper()
	s, err := openSink(filepath.Join(dir, name+".sqlite"), "id")
	if err != nil {
		t.Fatalf("open sink: %v", err)
	}
	defer s.Close()
	if _, _, _, _, err := applyFeed(s, strings.Join(lines, "\n")+"\n"); err != nil {
		t.Fatalf("apply feed: %v", err)
	}
	return destinationState(t, s.db, "inv")
}

// **The property.** Any duplication, and any replay of an earlier prefix, must leave the
// destination in the same state as a single clean delivery.
func TestAnyDuplicationOrReplayConvergesToTheSameState(t *testing.T) {
	dir := t.TempDir()
	ordered := workload()
	want := applyTo(t, dir, "ordered", ordered)

	// Anti-vacuity: the ordered feed must actually land something, and land the NEWEST value for a
	// row that was updated twice. Comparing two empty destinations would satisfy every assertion
	// below while proving nothing.
	if want == "" {
		t.Fatal("the ordered feed landed nothing; every comparison below would be vacuous")
	}
	if !strings.Contains(want, "qty=99") {
		t.Fatalf("the ordered feed did not keep the newest update to row 1:\n%s", want)
	}

	// A fixed seed: a failure below reproduces exactly.
	rng := rand.New(rand.NewSource(0x5eed))
	for trial := 0; trial < 60; trial++ {
		// An order-preserving delivery with duplicates and a replayed prefix - what an
		// at-least-once transport with a cursor actually produces.
		var delivery []string
		for _, line := range ordered {
			delivery = append(delivery, line)
			// An immediate re-delivery of the same event.
			if rng.Intn(3) == 0 {
				delivery = append(delivery, line)
			}
		}
		// A consumer that crashed and resumed from an older position: re-send a prefix, in order.
		if n := rng.Intn(len(ordered)); n > 0 {
			delivery = append(delivery, ordered[:n]...)
		}

		got := applyTo(t, dir, fmt.Sprintf("trial%d", trial), delivery)
		if got != want {
			t.Fatalf("trial %d did not converge.\nseed 0x5eed, delivery:\n%s\n\nwant:\n%s\n\ngot:\n%s",
				trial, strings.Join(delivery, "\n"), want, got)
		}
	}
}

// A stale event arriving later in the SAME feed is dropped before it reaches SQL.
//
// This exercises `applyFeed`'s cursor filter, which is a different guard from the one in the
// `ON CONFLICT` clause — and the distinction is not academic. Replacing the SQL guard with an
// always-true clause leaves BOTH this test and the property above passing, because the cursor
// filter removes stale events before any statement runs. The SQL guard needs its own test, below.
func TestAStaleEventLaterInTheFeedIsFilteredByTheCursor(t *testing.T) {
	dir := t.TempDir()
	ordered := workload()
	want := applyTo(t, dir, "ref", ordered)

	// Deliver the newest update to row 1 BEFORE the older one. A sink that let the older write land
	// on top - last delivery wins rather than highest commit_lsn wins - ends with qty=15.
	stale := []string{
		ordered[0], // CREATE_TABLE
		ordered[1], ordered[2],
		ordered[4], // qty 99, commit_lsn 30
		ordered[3], // qty 15, commit_lsn 20 — arrives later, must NOT apply
		ordered[5], ordered[6], ordered[7],
	}
	got := applyTo(t, dir, "stale", stale)
	if got != want {
		t.Fatalf("a stale update was allowed to land:\nwant:\n%s\ngot:\n%s", want, got)
	}
	if !strings.Contains(got, "qty=99") {
		t.Fatalf("the newest value did not survive a late stale update:\n%s", got)
	}
}

// **The ordering guard in the SQL, forced directly.**
//
// The README says the test lives in the `ON CONFLICT … DO UPDATE … WHERE` clause rather than in the
// program's control flow, "so every write path inherits it — including one added later by someone
// who did not read the comment above it". That is a claim about `apply`, and `applyFeed` cannot
// check it: its cursor filter drops a stale event before any statement runs, so a sink whose SQL
// guard had been replaced with an always-true clause passed every other test in this file.
//
// So this calls `apply` directly, which is exactly the position of "a write path added later".
func TestTheSQLOrderingGuardRejectsAStaleWriteOnItsOwn(t *testing.T) {
	s, err := openSink(filepath.Join(t.TempDir(), "direct.sqlite"), "id")
	if err != nil {
		t.Fatalf("open sink: %v", err)
	}
	defer s.Close()

	decode := func(line string) *Event {
		e, derr := decodeLine(line, 1)
		if derr != nil {
			t.Fatalf("decode: %v", derr)
		}
		return e
	}

	if err := s.apply(decode(schemaLine())); err != nil {
		t.Fatalf("schema: %v", err)
	}
	if err := s.apply(decode(event("INSERT", 1, "widget", 10, 10))); err != nil {
		t.Fatalf("insert: %v", err)
	}
	// The newest write.
	if err := s.apply(decode(event("UPDATE", 1, "widget", 99, 30))); err != nil {
		t.Fatalf("newer update: %v", err)
	}
	// An older one, applied afterwards with no cursor in the way. The SQL is the only thing that
	// can reject it.
	if err := s.apply(decode(event("UPDATE", 1, "widget", 15, 20))); err != nil {
		t.Fatalf("stale update returned an error rather than being ignored: %v", err)
	}

	got := destinationState(t, s.db, "inv")
	if !strings.Contains(got, "qty=99") {
		t.Fatalf("a stale write landed on top of a newer one; the ON CONFLICT guard is not "+
			"rejecting it:\n%s", got)
	}
	if strings.Contains(got, "qty=15") {
		t.Fatalf("the stale value is present:\n%s", got)
	}
}
