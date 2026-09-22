//! D154 — the fire-check for `wal::log::write_str`'s length guard.
//!
//! The guard itself lives in ONE place, which is the whole point of the row. But every call site
//! needs its own `?`, and a missing `?` on a `Result` is a WARNING, not an error — CI denies only
//! `dead_code` and `duplicate_macro_attributes`. So a site could silently drop the refusal and the
//! build would stay green. **This file exists to make each site prove it propagates.**
//!
//! Every production call site of `write_str` inside `RecKind::serialize` is driven over the limit
//! by exactly one field, and asserted to refuse. The mirror half — the largest value the prefix CAN
//! express, still encoding — is at the bottom, and it is what stops a guard that refused
//! everything from passing this file.
//!
//! The refusals are deliberately NOT asserted through `consensus::transport::encode`. That path
//! used to carry a send-side re-encode check which would have masked a missing guard here; D154
//! deleted it as unfirable, and this file drives the encoder directly so the instrument does not
//! depend on that deletion staying done.

use ferrodb::branch::types::BranchId;
use ferrodb::catalog::column::DataType;
use ferrodb::error::FerroError;
use ferrodb::provenance::{ProvId, RunEntity};
use ferrodb::wal::log::{ColumnAlteration, DdlOp, RecKind};

/// One byte past what a `u16` length prefix can express.
const TOO_LONG: usize = u16::MAX as usize + 1;
/// Exactly what it can express.
const MAXIMAL: usize = u16::MAX as usize;

fn ddl(table: &str, columns: Vec<(String, DataType, bool)>) -> RecKind {
    RecKind::Ddl {
        op: DdlOp::CreateTable,
        table: table.to_string(),
        dir_root: 0,
        time_travel_root: 0,
        columns,
    }
}

fn alter(alt: ColumnAlteration) -> RecKind {
    RecKind::Ddl {
        op: DdlOp::AlterColumn(alt),
        table: "t".to_string(),
        dir_root: 0,
        time_travel_root: 0,
        columns: Vec::new(),
    }
}

fn run_with(agent: &str, run_id: &str, model: &str, version: &str) -> RecKind {
    RecKind::RunIdentity {
        // A real id, not `ProvId::NONE`: `deserialize` refuses a run-identity record that names
        // the "unattributed" value, and this fixture has to survive a round trip below.
        run: RunEntity::new(
            ProvId(1),
            agent,
            run_id,
            model,
            version,
            [0u8; 32],
            1_700_000_000_000,
            BranchId::new(4, 0),
        ),
    }
}

#[track_caller]
fn assert_refused(rec: &RecKind, site: &str, expect_len: usize) {
    let mut buf = Vec::new();
    match rec.serialize(&mut buf) {
        Err(FerroError::Unrepresentable { what: _, len, limit }) => {
            assert_eq!(len, expect_len, "{site}: the refusal reported the wrong length");
            assert_eq!(limit, MAXIMAL, "{site}: the refusal reported the wrong limit");
        }
        Err(other) => panic!("{site}: expected Unrepresentable, got {other:?}"),
        Ok(_) => panic!(
            "{site}: DID NOT FIRE — a {expect_len}-byte value was encoded behind a u16 prefix, \
             so the record is durable and permanently undecodable"
        ),
    }
    assert!(
        buf.len() < TOO_LONG,
        "{site}: refused, but the full bytes were already pushed into the buffer"
    );
}

#[test]
fn every_write_str_site_in_a_ddl_record_refuses_what_its_prefix_cannot_express() {
    let long = "x".repeat(TOO_LONG);

    assert_refused(&ddl(&long, Vec::new()), "Ddl / table name", TOO_LONG);
    assert_refused(
        &ddl("t", vec![(long.clone(), DataType::Integer, false)]),
        "Ddl / column name",
        TOO_LONG,
    );
    assert_refused(
        &alter(ColumnAlteration::Add { column: long.clone() }),
        "AlterColumn::Add / column name",
        TOO_LONG,
    );
    assert_refused(
        &alter(ColumnAlteration::Rename { from: long.clone(), to: "b".into() }),
        "AlterColumn::Rename / old name",
        TOO_LONG,
    );
    assert_refused(
        &alter(ColumnAlteration::Rename { from: "a".into(), to: long.clone() }),
        "AlterColumn::Rename / new name",
        TOO_LONG,
    );
    assert_refused(
        &alter(ColumnAlteration::Retype { column: long, from: DataType::Integer }),
        "AlterColumn::Retype / column name",
        TOO_LONG,
    );
}

/// The count field, which is the same hazard one field over from the names.
#[test]
fn a_ddl_record_refuses_a_column_count_its_prefix_cannot_express() {
    let columns: Vec<(String, DataType, bool)> =
        (0..TOO_LONG).map(|_| ("c".to_string(), DataType::Integer, false)).collect();
    assert_eq!(columns.len(), 65536);
    assert_refused(&ddl("t", columns), "Ddl / column count", TOO_LONG);
}

#[test]
fn every_write_str_site_in_a_run_identity_record_refuses() {
    let long = "x".repeat(TOO_LONG);
    assert_refused(&run_with(&long, "r", "m", "v"), "RunIdentity / agent_id", TOO_LONG);
    assert_refused(&run_with("a", &long, "m", "v"), "RunIdentity / run_id", TOO_LONG);
    assert_refused(&run_with("a", "r", &long, "v"), "RunIdentity / model", TOO_LONG);
    assert_refused(&run_with("a", "r", "m", &long), "RunIdentity / model_version", TOO_LONG);
}

/// `Clr` serializes its `redo` by recursion, so the `?` on that recursive call is its own site:
/// without it a compensation record would carry a truncated inner record.
#[test]
fn a_compensation_record_refuses_through_the_record_it_wraps() {
    let long = "x".repeat(TOO_LONG);
    let clr = RecKind::Clr {
        undone_lsn: 1,
        undo_next: 2,
        redo: Box::new(ddl(&long, Vec::new())),
    };
    assert_refused(&clr, "Clr / recursive redo", TOO_LONG);
}

/// **The anti-vacuity half.** A guard that refused everything would satisfy every test above and
/// break the write path, so each site is exercised at exactly the largest value its prefix CAN
/// express and the record has to encode.
///
/// The column COUNT is not checked at its boundary here and cannot usefully be: 65535 columns each
/// costing at least four bytes is a ~260 KB record, which is a test about `Vec` allocation rather
/// than about this guard. Its non-fire evidence is that ordinary counts encode, which every other
/// test in the suite exercises.
#[test]
fn the_largest_value_each_prefix_can_express_still_encodes() {
    let max = "y".repeat(MAXIMAL);
    let mut encoded_at_least_one = false;

    for (site, rec) in [
        ("Ddl / table name", ddl(&max, Vec::new())),
        ("Ddl / column name", ddl("t", vec![(max.clone(), DataType::Integer, false)])),
        ("AlterColumn::Add", alter(ColumnAlteration::Add { column: max.clone() })),
        (
            "AlterColumn::Rename",
            alter(ColumnAlteration::Rename { from: max.clone(), to: max.clone() }),
        ),
        (
            "AlterColumn::Retype",
            alter(ColumnAlteration::Retype { column: max.clone(), from: DataType::Integer }),
        ),
        ("RunIdentity / agent_id", run_with(&max, "r", "m", "v")),
        ("RunIdentity / run_id", run_with("a", &max, "m", "v")),
        ("RunIdentity / model", run_with("a", "r", &max, "v")),
        ("RunIdentity / model_version", run_with("a", "r", "m", &max)),
        ("Clr / recursive redo", RecKind::Clr {
            undone_lsn: 1,
            undo_next: 2,
            redo: Box::new(ddl(&max, Vec::new())),
        }),
    ] {
        let mut buf = Vec::new();
        rec.serialize(&mut buf)
            .unwrap_or_else(|e| panic!("{site}: a value AT the limit was refused: {e}"));
        assert!(buf.len() >= MAXIMAL, "{site}: encoded, but the bytes are not in the buffer");

        // And it has to read back as what went in — a guard that let the value through while
        // corrupting the prefix would pass an `is_ok()` check.
        let back = RecKind::deserialize(&buf)
            .unwrap_or_else(|e| panic!("{site}: encoded a record its own reader cannot parse: {e}"));
        assert_eq!(back, rec, "{site}: a maximal value did not survive its own encoding");
        encoded_at_least_one = true;
    }

    assert!(encoded_at_least_one, "the table was empty, so this test asserted nothing");
}

// ---- the four sites the sweep found UNCOVERED ------------------------------------------------
//
// bench/d154_firecheck.txt's first run: 13 killed, 4 SURVIVED, and the survivors were exactly
// `provenance::durable`'s four `write_str` calls. The guard was fine; the INSTRUMENT could not
// reach them, because everything above drives `RecKind::serialize` and these live behind
// `DurableProvenanceStore::intern`. A sweep scoped to what it already covers returns a clean sheet
// that means nothing, so the sweep kept them as survivors and this closes them.

use ferrodb::provenance::{DurableProvenanceStore, ProvenanceStore};

fn store_at(dir: &std::path::Path) -> DurableProvenanceStore {
    DurableProvenanceStore::open(dir.join("p.prov")).expect("a fresh store must open")
}

fn entity(agent: &str, run_id: &str, model: &str, version: &str) -> RunEntity {
    RunEntity::new(
        ProvId(1),
        agent,
        run_id,
        model,
        version,
        [0u8; 32],
        1_700_000_000_000,
        BranchId::new(4, 0),
    )
}

/// Each of the durable provenance store's four `write_str` sites refuses what it cannot encode —
/// **and a refused run does not linger in the store's own view of itself.**
///
/// The second half is the law, not decoration. `intern` interns into memory and *then* encodes, so
/// a refusal returns early with the run already in `self.mem` but never in the file. It would be
/// reported by `run_count`/`runs` and would vanish on the next `open`: the store answering for a
/// run that is not durable.
#[test]
fn the_durable_provenance_store_refuses_a_run_it_cannot_encode() {
    let long = "x".repeat(TOO_LONG);
    let cases = [
        ("agent_id", entity(&long, "r", "m", "v")),
        ("run_id", entity("a", &long, "m", "v")),
        ("model", entity("a", "r", &long, "v")),
        ("model_version", entity("a", "r", "m", &long)),
    ];

    for (field, run) in cases {
        let dir = tempfile::tempdir().unwrap();
        let store = store_at(dir.path());
        let before = store.run_count();

        match store.intern(&run) {
            Err(FerroError::Unrepresentable { len, limit, .. }) => {
                assert_eq!(len, TOO_LONG, "{field}: wrong length in the refusal");
                assert_eq!(limit, MAXIMAL, "{field}: wrong limit in the refusal");
            }
            Err(other) => panic!("{field}: expected Unrepresentable, got {other:?}"),
            Ok(_) => panic!(
                "{field}: DID NOT FIRE — a {TOO_LONG}-byte value was written behind a u16 prefix \
                 into a durable provenance record"
            ),
        }

        assert_eq!(
            store.run_count(),
            before,
            "{field}: the run was REFUSED but is still in the store's in-memory view. It is not in \
             the file, so `runs()` reports a run that disappears on the next open"
        );
    }

    // Anti-vacuity: an ordinary run still interns, and at the exact limit too.
    let dir = tempfile::tempdir().unwrap();
    let store = store_at(dir.path());
    store.intern(&entity("agent", "run", "model", "v1")).expect("an ordinary run must intern");
    store
        .intern(&entity(&"y".repeat(MAXIMAL), "run2", "model", "v1"))
        .expect("an agent id AT the limit must intern, or the guard refuses what it can express");
    assert_eq!(store.run_count(), 2, "both runs must be in the store");
}
