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
