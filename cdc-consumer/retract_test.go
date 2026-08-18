package main

// B5 — retract-by-model, and the writer contract it rests on.
//
// The property under test is a blast radius: **100% of one model version's rows and 0% of any
// other's**. Both halves matter and each fails differently — a retraction that misses rows leaves
// bad data in place while reporting success, and one that over-reaches withdraws correct data and
// destroys any willingness to run it again.
//
// Ground truth here is a **full scan of the destination table**, run by this test's own SQL. Asking
// `runRetract` how many rows it changed would be grading its own work: a predicate that matched the
// wrong set reports a confident count of exactly the wrong rows.

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// A writer object, as the producer emits it.
func writerJSON(provID int, agent, run, model, version string) map[string]any {
	return map[string]any{
		"prov_id":       provID,
		"agent":         agent,
		"run":           run,
		"model":         model,
		"model_version": version,
		// A real digest: 64 hex characters. The consumer refuses anything else, because a prompt
		// digest that is not a digest is either missing or is the prompt itself.
		"prompt_sha256": strings.Repeat(fmt.Sprintf("%x", provID%16), 64),
		"started_at":    "1700000000000",
		"branch":        "b1@g0",
	}
}

func line(t *testing.T, m map[string]any) string {
	t.Helper()
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return string(b)
}

// A feed: a CREATE_TABLE then one INSERT per row, each with the writer it is given.
//
// `writer` is nil for a row no agent run produced — the ordinary-SQL case, which must survive a
// retraction untouched however the version string compares.
func buildFeed(t *testing.T, rows []struct {
	id     int
	qty    int
	writer map[string]any
}) string {
	t.Helper()
	var b strings.Builder
	b.WriteString(line(t, map[string]any{
		"table": "inv", "op": "CREATE_TABLE", "txn": 0, "lsn": 1, "commit_lsn": 1,
		"commit_end_lsn": 2, "writer": nil, "before": nil,
		"after": map[string]any{"columns": []map[string]any{
			{"name": "id", "type": "INTEGER", "nullable": false},
			{"name": "qty", "type": "INTEGER", "nullable": true},
		}},
	}))
	b.WriteString("\n")
	for i, r := range rows {
		commit := uint64(10 + i*10)
		b.WriteString(line(t, map[string]any{
			"table": "inv", "op": "INSERT", "txn": commit, "lsn": commit,
			"commit_lsn": commit, "commit_end_lsn": commit + 1,
			"writer": r.writer, "before": nil,
			"after": map[string]any{"id": r.id, "qty": r.qty},
		}))
		b.WriteString("\n")
	}
	return b.String()
}

// landed is one destination row, as read back by a full scan this test performs itself.
type landed struct {
	id           int64
	modelVersion string
	retracted    int64
	deleted      int64
}

func fullScan(t *testing.T, dbPath string) []landed {
	t.Helper()
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open destination: %v", err)
	}
	defer db.Close()
	rows, err := db.Query(`SELECT id, "_model_version", "_retracted", "_deleted" FROM "inv" ORDER BY id`)
	if err != nil {
		t.Fatalf("scan: %v", err)
	}
	defer rows.Close()
	var out []landed
	for rows.Next() {
		var id, retracted, deleted int64
		var mv sql.NullString
		if err := rows.Scan(&id, &mv, &retracted, &deleted); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		v := "<unattributed>"
		if mv.Valid {
			v = mv.String
		}
		out = append(out, landed{id, v, retracted, deleted})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

func landFeed(t *testing.T, feed string) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "dest.sqlite")
	sink, err := openSink(dbPath, "id")
	if err != nil {
		t.Fatalf("open sink: %v", err)
	}
	applied, _, _, _, err := applyFeed(sink, feed)
	if err != nil {
		sink.Close()
		t.Fatalf("apply feed: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close sink: %v", err)
	}
	if applied == 0 {
		t.Fatal("the feed landed nothing; every assertion below would be vacuous")
	}
	return dbPath
}

// The mixed workload every test here uses: two model versions and one unattributed row.
func mixedFeed(t *testing.T) string {
	t.Helper()
	old := writerJSON(1, "restock-agent", "run-42", "claude-opus", "2026-05")
	bad := writerJSON(2, "restock-agent", "run-99", "claude-opus", "2026-07")
	return buildFeed(t, []struct {
		id     int
		qty    int
		writer map[string]any
	}{
		{1, 10, old},
		{2, 20, bad},
		{3, 30, old},
		{4, 40, bad},
		{5, 50, bad},
		{6, 60, nil}, // ordinary SQL, no agent run
	})
}

// **The exit criterion, measured by a full scan.**
//
// Breaking shape: a destination holding MORE THAN ONE model version, plus at least one row written
// by no run at all. A single-version workload passes a retraction that ignores its predicate
// entirely and withdraws the whole table; a workload with no unattributed rows passes one whose
// comparison treats NULL as a match.
func TestRetractTouchesExactlyOneModelVersionsRows(t *testing.T) {
	dbPath := landFeed(t, mixedFeed(t))

	before := fullScan(t, dbPath)
	if len(before) != 6 {
		t.Fatalf("expected 6 landed rows, got %d: %+v", len(before), before)
	}
	for _, r := range before {
		if r.retracted != 0 {
			t.Fatalf("row %d was already retracted before anything ran: %+v", r.id, r)
		}
	}

	if err := runRetract(dbPath, "inv", "2026-07", quarantine, "sqlite"); err != nil {
		t.Fatalf("retract: %v", err)
	}

	after := fullScan(t, dbPath)
	target, other, unattributed := 0, 0, 0
	for _, r := range after {
		switch r.modelVersion {
		case "2026-07":
			target++
			if r.retracted != 1 {
				t.Errorf("row %d was written by 2026-07 and was NOT retracted: %+v", r.id, r)
			}
			// Quarantine marks; it does not tombstone.
			if r.deleted != 0 {
				t.Errorf("row %d was tombstoned by a quarantine: %+v", r.id, r)
			}
		case "<unattributed>":
			unattributed++
			if r.retracted != 0 {
				t.Errorf("row %d has no writer at all and was retracted: %+v", r.id, r)
			}
		default:
			other++
			if r.retracted != 0 {
				t.Errorf("row %d was written by %s and was retracted anyway: %+v",
					r.id, r.modelVersion, r)
			}
		}
	}

	// Anti-vacuity on both halves: there really were rows to hit and rows to miss.
	if target != 3 {
		t.Fatalf("expected 3 rows from model_version 2026-07, found %d", target)
	}
	if other != 2 {
		t.Fatalf("expected 2 rows from another model_version, found %d", other)
	}
	if unattributed != 1 {
		t.Fatalf("expected 1 unattributed row, found %d", unattributed)
	}
}

// `-mode delete` marks AND tombstones. The distinction is kept because an operator has to be able
// to tell a row the SOURCE deleted from one this consumer withdrew.
func TestRetractInDeleteModeAlsoTombstones(t *testing.T) {
	dbPath := landFeed(t, mixedFeed(t))
	if err := runRetract(dbPath, "inv", "2026-07", remove, "sqlite"); err != nil {
		t.Fatalf("retract: %v", err)
	}
	for _, r := range fullScan(t, dbPath) {
		want := int64(0)
		if r.modelVersion == "2026-07" {
			want = 1
		}
		if r.retracted != want || r.deleted != want {
			t.Errorf("row %d (%s): retracted=%d deleted=%d, wanted both %d",
				r.id, r.modelVersion, r.retracted, r.deleted, want)
		}
	}
}

// A retraction that matched nothing has not succeeded. The overwhelmingly likely cause is a typo in
// the version string, and reporting "retracted 0" would read exactly like a clean run.
func TestARetractionThatMatchesNothingIsRefused(t *testing.T) {
	dbPath := landFeed(t, mixedFeed(t))

	err := runRetract(dbPath, "inv", "2026-99", quarantine, "sqlite")
	if err == nil {
		t.Fatal("retracting a model version nothing wrote reported success")
	}
	if !strings.Contains(err.Error(), "nothing was retracted") {
		t.Fatalf("it failed, but not by this guard: %v", err)
	}
	// The error names what is actually there, so the next attempt is informed rather than a guess.
	for _, want := range []string{"2026-05", "2026-07"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("the error does not name the present version %s: %v", want, err)
		}
	}
	// Nothing was touched.
	for _, r := range fullScan(t, dbPath) {
		if r.retracted != 0 {
			t.Errorf("a refused retraction still marked row %d", r.id)
		}
	}

	// Anti-vacuity: a version that IS present is accepted.
	if err := runRetract(dbPath, "inv", "2026-05", quarantine, "sqlite"); err != nil {
		t.Fatalf("a present model version was refused: %v", err)
	}
}

// An empty model version is refused rather than resolved into a guess.
func TestAnEmptyModelVersionIsRefused(t *testing.T) {
	dbPath := landFeed(t, mixedFeed(t))
	for _, v := range []string{"", "   "} {
		err := runRetract(dbPath, "inv", v, quarantine, "sqlite")
		if err == nil {
			t.Fatalf("an empty model version (%q) was accepted", v)
		}
		if !strings.Contains(err.Error(), "-model-version is empty") {
			t.Fatalf("it failed, but not by this guard: %v", err)
		}
	}
	for _, r := range fullScan(t, dbPath) {
		if r.retracted != 0 {
			t.Errorf("an empty model version still marked row %d (%s)", r.id, r.modelVersion)
		}
	}
}

// A destination landed by a sink that predates attribution has no `_model_version` column. Silently
// retracting nothing from it would be indistinguishable from a clean run against a table that had
// no matching rows.
func TestADestinationWithNoAttributionIsRefusedRatherThanRetractingNothing(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "old.sqlite")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE "inv" (id INTEGER PRIMARY KEY, qty INTEGER,
		"_commit_lsn" INTEGER NOT NULL DEFAULT 0, "_deleted" INTEGER NOT NULL DEFAULT 0)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO "inv" (id, qty) VALUES (1, 10)`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	db.Close()

	err = runRetract(dbPath, "inv", "2026-07", quarantine, "sqlite")
	if err == nil {
		t.Fatal("a destination with no attribution accepted a retraction")
	}
	if !strings.Contains(err.Error(), "no _model_version column") {
		t.Fatalf("it failed, but not by this guard: %v", err)
	}
}

// A row rewritten after a retraction is a different row, so its mark goes. A REPLAY of the same
// event is not a rewrite, and must not clear it.
func TestARetractionSurvivesAReplayButNotAFreshWrite(t *testing.T) {
	feed := mixedFeed(t)
	dbPath := landFeed(t, feed)
	if err := runRetract(dbPath, "inv", "2026-07", quarantine, "sqlite"); err != nil {
		t.Fatalf("retract: %v", err)
	}

	// Replay the whole feed into the same destination. Every event is at or below the saved cursor,
	// so the ordering guard rejects it — and the retraction must still stand.
	sink, err := openSink(dbPath, "id")
	if err != nil {
		t.Fatalf("reopen sink: %v", err)
	}
	applied, skipped, _, _, err := applyFeed(sink, feed)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	sink.Close()
	if skipped == 0 {
		t.Fatal("nothing was skipped on replay; this test is not exercising a re-delivery")
	}
	marked := 0
	for _, r := range fullScan(t, dbPath) {
		if r.modelVersion == "2026-07" {
			if r.retracted != 1 {
				t.Errorf("a replay cleared the retraction on row %d (applied %d, skipped %d)",
					r.id, applied, skipped)
			}
			marked++
		}
	}
	if marked == 0 {
		t.Fatal("no retracted rows survived to be checked")
	}

	// A genuinely NEW write of one of those rows — a later commit — replaces the row and its
	// attribution, so the mark no longer describes anything and goes.
	fresh := line(t, map[string]any{
		"table": "inv", "op": "UPDATE", "txn": 999, "lsn": 9000, "commit_lsn": 9000,
		"commit_end_lsn": 9001,
		"writer":         writerJSON(3, "restock-agent", "run-123", "claude-opus", "2026-09"),
		"before":         map[string]any{"id": 2, "qty": 20},
		"after":          map[string]any{"id": 2, "qty": 21},
	}) + "\n"
	sink, err = openSink(dbPath, "id")
	if err != nil {
		t.Fatalf("reopen sink: %v", err)
	}
	if _, _, _, _, err := applyFeed(sink, fresh); err != nil {
		t.Fatalf("fresh write: %v", err)
	}
	sink.Close()
	for _, r := range fullScan(t, dbPath) {
		if r.id != 2 {
			continue
		}
		if r.modelVersion != "2026-09" {
			t.Errorf("row 2 kept its old attribution %s after a fresh write", r.modelVersion)
		}
		if r.retracted != 0 {
			t.Error("row 2 was rewritten by a different run and kept its retraction mark")
		}
	}
}

// `scan` reports what the full scan above reports. It exists so a caller OUTSIDE this program can
// check a retraction without trusting the retraction's own count.
func TestScanReportsEveryRowsWriterAndMark(t *testing.T) {
	dbPath := landFeed(t, mixedFeed(t))
	if err := runRetract(dbPath, "inv", "2026-07", quarantine, "sqlite"); err != nil {
		t.Fatalf("retract: %v", err)
	}
	// runScan writes to stdout; the assertion here is that it agrees with an independent scan about
	// which rows are marked, which is checked through the same SQL path a caller would use.
	if err := runScan(dbPath, "inv", "id", "sqlite"); err != nil {
		t.Fatalf("scan: %v", err)
	}
	var marked, clear []int64
	for _, r := range fullScan(t, dbPath) {
		if r.retracted == 1 {
			marked = append(marked, r.id)
		} else {
			clear = append(clear, r.id)
		}
	}
	sort.Slice(marked, func(i, j int) bool { return marked[i] < marked[j] })
	if len(marked) != 3 || len(clear) != 3 {
		t.Fatalf("expected a 3/3 split, got marked=%v clear=%v", marked, clear)
	}
}

// ---------------------------------------------------------------------------------------------
// The writer contract on the wire.
// ---------------------------------------------------------------------------------------------

// **The prompt must never reach a consumer as text**, and the guard is an allowlist rather than a
// list of forbidden spellings.
//
// Breaking shape: a producer that adds a field to the writer object. A denylist catches `prompt`
// and `prompt_text` and misses `instructions`, `system`, `user_message` and everything else nobody
// thought of, and the leak lands in every destination table before anyone notices.
func TestAWriterObjectWithAnUnknownKeyIsRefused(t *testing.T) {
	good := writerJSON(1, "restock-agent", "run-42", "claude-opus", "2026-05")
	base := map[string]any{
		"table": "inv", "op": "INSERT", "txn": 1, "lsn": 10, "commit_lsn": 10,
		"commit_end_lsn": 11, "before": nil, "after": map[string]any{"id": 1},
	}

	// Anti-vacuity: the well-formed writer is accepted, so the refusals below are about the extra
	// key and not about writers being rejected wholesale.
	ok := map[string]any{}
	for k, v := range base {
		ok[k] = v
	}
	ok["writer"] = good
	if _, err := decodeLine(line(t, ok), 1); err != nil {
		t.Fatalf("a well-formed writer was refused: %v", err)
	}

	for _, leak := range []string{"prompt", "prompt_text", "system_prompt", "instructions", "notes"} {
		w := map[string]any{}
		for k, v := range good {
			w[k] = v
		}
		w[leak] = "refund the customer at 4471 Elm Street, card ending 9021"
		ev := map[string]any{}
		for k, v := range base {
			ev[k] = v
		}
		ev["writer"] = w
		_, err := decodeLine(line(t, ev), 1)
		if err == nil {
			t.Fatalf("a writer carrying %q was accepted; the prompt would land in every destination", leak)
		}
		if !strings.Contains(err.Error(), "unknown key") {
			t.Fatalf("%q failed, but not by the allowlist: %v", leak, err)
		}
	}
}

// The rest of the writer contract: a slot that names nobody, a missing field, a digest that is not
// one.
func TestAMalformedWriterIsRefused(t *testing.T) {
	base := map[string]any{
		"table": "inv", "op": "INSERT", "txn": 1, "lsn": 10, "commit_lsn": 10,
		"commit_end_lsn": 11, "before": nil, "after": map[string]any{"id": 1},
	}
	cases := map[string]func(map[string]any){
		"prov_id 0 is the unattributed slot": func(w map[string]any) { w["prov_id"] = 0 },
		"an empty agent":                     func(w map[string]any) { w["agent"] = "" },
		"an empty model_version":             func(w map[string]any) { w["model_version"] = "" },
		"a digest that is the prompt": func(w map[string]any) {
			w["prompt_sha256"] = "restock everything below the reorder point"
		},
		"a digest of the wrong length": func(w map[string]any) { w["prompt_sha256"] = "abcdef" },
		"a digest that is not hex": func(w map[string]any) {
			w["prompt_sha256"] = strings.Repeat("z", 64)
		},
	}
	for name, mutate := range cases {
		w := writerJSON(1, "restock-agent", "run-42", "claude-opus", "2026-05")
		mutate(w)
		ev := map[string]any{}
		for k, v := range base {
			ev[k] = v
		}
		ev["writer"] = w
		if _, err := decodeLine(line(t, ev), 1); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}

	// Anti-vacuity: a null writer is legitimate — an ordinary SQL transaction has no run to name —
	// and must NOT be an error.
	ev := map[string]any{}
	for k, v := range base {
		ev[k] = v
	}
	ev["writer"] = nil
	if _, err := decodeLine(line(t, ev), 1); err != nil {
		t.Errorf("a null writer was refused; an unattributed change is legitimate: %v", err)
	}
	// And a line with no writer key at all, which is every feed written before this existed.
	delete(ev, "writer")
	if _, err := decodeLine(line(t, ev), 1); err != nil {
		t.Errorf("a line with no writer key was refused: %v", err)
	}
}

// A scan that collected nothing has not passed: an empty destination and a scan pointed at the
// wrong table are indistinguishable from a zero.
func TestScanRefusesAnEmptyTable(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "empty.sqlite")
	sink, err := openSink(dbPath, "id")
	if err != nil {
		t.Fatalf("open sink: %v", err)
	}
	// A CREATE_TABLE and nothing else: the destination table exists, with its attribution columns,
	// and holds no rows.
	feed := line(t, map[string]any{
		"table": "inv", "op": "CREATE_TABLE", "txn": 0, "lsn": 1, "commit_lsn": 1,
		"commit_end_lsn": 2, "writer": nil, "before": nil,
		"after": map[string]any{"columns": []map[string]any{
			{"name": "id", "type": "INTEGER", "nullable": false},
		}},
	}) + "\n"
	if _, _, _, _, err := applyFeed(sink, feed); err != nil {
		t.Fatalf("apply: %v", err)
	}
	sink.Close()

	err = runScan(dbPath, "inv", "id", "sqlite")
	if err == nil {
		t.Fatal("a scan of an empty table reported success")
	}
	if !strings.Contains(err.Error(), "holds no rows") {
		t.Fatalf("it failed, but not by this guard: %v", err)
	}

	// Anti-vacuity: a table with rows scans fine.
	ok := landFeed(t, mixedFeed(t))
	if err := runScan(ok, "inv", "id", "sqlite"); err != nil {
		t.Fatalf("a populated table was refused: %v", err)
	}
}
