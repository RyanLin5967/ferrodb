package main

// B5 — retract-by-model against the DuckDB destination.
//
// The SQLite sink landed the writer with every row first, and the DuckDB one did not — which meant
// half the destinations this consumer supports carried no attribution at all while the README
// presented retract-by-model as *the* answer for the pipeline. A feature that works on one of two
// documented engines and says nothing about the other is worse than one that works on neither,
// because the operator finds out only when they need it.
//
// The two sinks necessarily differ in the column TYPES — DuckDB has a real BOOLEAN and SQLite does
// not — but `retract`/`scan` read those columns by NAME through engine-agnostic SQL, so the names are
// what must agree. `TestBothSinksLandTheSameWriterColumnNames` pins that; without it a column added
// to one list and not the other surfaces as a retraction that silently matches nothing.

import (
	"database/sql"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

// The names are the contract between the sinks and `retract`/`scan`. The types are not.
func TestBothSinksLandTheSameWriterColumnNames(t *testing.T) {
	sqliteNames := make([]string, 0, len(writerColumns))
	for _, w := range writerColumns {
		sqliteNames = append(sqliteNames, w.name)
	}
	duckNames := make([]string, 0, len(duckWriterColumns))
	for _, w := range duckWriterColumns {
		duckNames = append(duckNames, w.name)
	}
	if strings.Join(sqliteNames, ",") != strings.Join(duckNames, ",") {
		t.Fatalf("the two sinks land different attribution columns:\n sqlite: %v\n duckdb: %v\n"+
			"`retract` and `scan` address them by name, so a column in one list and not the other is "+
			"a retraction that matches nothing against that engine", sqliteNames, duckNames)
	}
	// And every one of them is in `bookkeeping`, or `catalogSchema` reports it as a column of the
	// source table and `checkSchemaAgrees` then refuses every CREATE_TABLE after the first.
	for _, n := range duckNames {
		if !bookkeeping[n] {
			t.Errorf("%s is not in `bookkeeping`; the DuckDB sink will read it back as a source column", n)
		}
	}
}

// landFeedDuck lands a feed in a DuckDB destination and returns its path.
func landFeedDuck(t *testing.T, feed string) string {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "dest.duckdb")
	sink, err := openDuckSink(dbPath, "id")
	if err != nil {
		t.Fatalf("open duckdb sink: %v", err)
	}
	applied, _, _, _, err := applyFeed(sink, feed)
	if err != nil {
		sink.Close()
		t.Fatalf("apply feed: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close duckdb sink: %v", err)
	}
	if applied == 0 {
		t.Fatal("the feed landed nothing; every assertion below would be vacuous")
	}
	return dbPath
}

// fullScanDuck is this test's own ground truth: a full scan through DuckDB, not through `runRetract`.
func fullScanDuck(t *testing.T, dbPath string) []landed {
	t.Helper()
	db, err := sql.Open("duckdb", dbPath)
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
		var id int64
		var mv sql.NullString
		// Real booleans here, unlike SQLite's integers — the difference `truthy` exists for.
		var retracted, deleted bool
		if err := rows.Scan(&id, &mv, &retracted, &deleted); err != nil {
			t.Fatalf("scan row: %v", err)
		}
		v := "<unattributed>"
		if mv.Valid {
			v = mv.String
		}
		out = append(out, landed{id, v, b2i64(retracted), b2i64(deleted)})
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	return out
}

func b2i64(b bool) int64 {
	if b {
		return 1
	}
	return 0
}

// **The same 100%/0% property, against the other engine.**
//
// Breaking shape: identical to the SQLite case — more than one model version in the destination plus
// at least one row written by no run — with the engine difference on top. A retraction that emitted
// `_retracted = 1` rather than `TRUE` does not even execute here, and one that scanned the flag into
// an int64 fails at the driver; neither shows up against SQLite.
func TestRetractWorksAgainstADuckDBDestination(t *testing.T) {
	dbPath := landFeedDuck(t, mixedFeed(t))

	before := fullScanDuck(t, dbPath)
	if len(before) != 6 {
		t.Fatalf("expected 6 landed rows, got %d: %+v", len(before), before)
	}
	for _, r := range before {
		if r.retracted != 0 {
			t.Fatalf("row %d was already retracted before anything ran", r.id)
		}
	}

	if err := runRetract(dbPath, "inv", "2026-07", quarantine, "duckdb"); err != nil {
		t.Fatalf("retract against duckdb: %v", err)
	}

	after := fullScanDuck(t, dbPath)
	target, other, unattributed := 0, 0, 0
	for _, r := range after {
		switch r.modelVersion {
		case "2026-07":
			target++
			if r.retracted != 1 {
				t.Errorf("row %d was written by 2026-07 and was NOT retracted", r.id)
			}
			if r.deleted != 0 {
				t.Errorf("row %d was tombstoned by a quarantine", r.id)
			}
		case "<unattributed>":
			unattributed++
			if r.retracted != 0 {
				t.Errorf("row %d has no writer at all and was retracted", r.id)
			}
		default:
			other++
			if r.retracted != 0 {
				t.Errorf("row %d was written by %s and was retracted anyway", r.id, r.modelVersion)
			}
		}
	}
	if target != 3 || other != 2 || unattributed != 1 {
		t.Fatalf("expected 3 target / 2 other / 1 unattributed, got %d / %d / %d",
			target, other, unattributed)
	}

	// `scan` reads the same destination through the engine-agnostic path, and must not fail on
	// DuckDB's boolean flags.
	if err := runScan(dbPath, "inv", "id", "duckdb"); err != nil {
		t.Fatalf("scan against duckdb: %v", err)
	}
}

// `-mode delete` tombstones as well, with DuckDB's boolean literal rather than SQLite's 1.
func TestRetractInDeleteModeAgainstDuckDB(t *testing.T) {
	dbPath := landFeedDuck(t, mixedFeed(t))
	if err := runRetract(dbPath, "inv", "2026-07", remove, "duckdb"); err != nil {
		t.Fatalf("retract: %v", err)
	}
	for _, r := range fullScanDuck(t, dbPath) {
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

// An engine this program does not know is refused, the same way `openDestination` refuses one. A
// typo in `-engine` that opened the wrong file would leave the destination the operator is watching
// untouched while reporting success.
func TestRetractAndScanRefuseAnUnknownEngine(t *testing.T) {
	dbPath := landFeed(t, mixedFeed(t))
	for _, bad := range []string{"sqlite3", "postgres", "", "SQLITE"} {
		err := runRetract(dbPath, "inv", "2026-07", quarantine, bad)
		if err == nil {
			t.Errorf("-engine %q was accepted by retract", bad)
		} else if !strings.Contains(err.Error(), "unknown -engine") {
			t.Errorf("-engine %q failed, but not by this guard: %v", bad, err)
		}
		if err := runScan(dbPath, "inv", "id", bad); err == nil {
			t.Errorf("-engine %q was accepted by scan", bad)
		}
	}
	// Anti-vacuity: the engine that IS known works, and nothing above touched the rows.
	for _, r := range fullScan(t, dbPath) {
		if r.retracted != 0 {
			t.Fatalf("a refused engine still marked row %d", r.id)
		}
	}
	if err := runRetract(dbPath, "inv", "2026-07", quarantine, "sqlite"); err != nil {
		t.Fatalf("the known engine was refused: %v", err)
	}
}

// A DuckDB destination landed before attribution existed gains the columns rather than being
// refused, and then retracts. The upgrade decision comes from the catalog, not from a driver's
// error wording.
func TestAnOlderDuckDBDestinationIsUpgradedInPlace(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "old.duckdb")
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	// The shape an earlier build of this sink produced: bookkeeping, no attribution.
	if _, err := db.Exec(`CREATE TABLE "inv" (id BIGINT PRIMARY KEY, qty BIGINT,
		"_commit_lsn" BIGINT NOT NULL DEFAULT 0, "_lsn" BIGINT NOT NULL DEFAULT 0,
		"_deleted" BOOLEAN NOT NULL DEFAULT false)`); err != nil {
		t.Fatalf("create: %v", err)
	}
	if _, err := db.Exec(`INSERT INTO "inv" VALUES (99, 1, 1, 1, false)`); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.Exec("CHECKPOINT"); err != nil {
		t.Fatalf("checkpoint: %v", err)
	}
	db.Close()

	// Before the upgrade, a retraction is refused rather than silently marking nothing.
	err = runRetract(dbPath, "inv", "2026-07", quarantine, "duckdb")
	if err == nil {
		t.Fatal("a destination with no attribution accepted a retraction")
	}
	if !strings.Contains(err.Error(), "no _model_version column") {
		t.Fatalf("it failed, but not by this guard: %v", err)
	}

	// Landing the feed upgrades the table in place; the pre-existing row survives.
	sink, err := openDuckSink(dbPath, "id")
	if err != nil {
		t.Fatalf("reopen sink: %v", err)
	}
	if _, _, _, _, err := applyFeed(sink, mixedFeed(t)); err != nil {
		sink.Close()
		t.Fatalf("apply: %v", err)
	}
	if err := sink.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	rows := fullScanDuck(t, dbPath)
	if len(rows) != 7 {
		t.Fatalf("expected the 6 feed rows plus the pre-existing one, got %d: %+v", len(rows), rows)
	}
	if err := runRetract(dbPath, "inv", "2026-07", quarantine, "duckdb"); err != nil {
		t.Fatalf("retract after upgrade: %v", err)
	}
	marked := 0
	for _, r := range fullScanDuck(t, dbPath) {
		if r.modelVersion == "2026-07" {
			if r.retracted != 1 {
				t.Errorf("row %d was not retracted after the upgrade", r.id)
			}
			marked++
		} else if r.retracted != 0 {
			t.Errorf("row %d (%s) was retracted and should not have been", r.id, r.modelVersion)
		}
	}
	if marked != 3 {
		t.Fatalf("expected 3 retracted rows, got %d", marked)
	}
	// The row that predates attribution has none, and a retraction must never claim it.
	found := false
	for _, r := range fullScanDuck(t, dbPath) {
		if r.id == 99 {
			found = true
			if r.modelVersion != "<unattributed>" || r.retracted != 0 {
				t.Errorf("the pre-existing row was attributed or retracted: %+v", r)
			}
		}
	}
	if !found {
		t.Error(fmt.Sprintf("the pre-existing row 99 is gone after the upgrade"))
	}
}
