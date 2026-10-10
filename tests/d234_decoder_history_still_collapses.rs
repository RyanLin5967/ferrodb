//! D234 — `LogicalDecoder`'s `collapse_repeats` keeps working after D234 took away its trigger.
//!
//! Until D234, a checkpoint that a pin kept from truncating still re-appended every table's
//! `CreateTable`, so under a live subscription the decoder met the same declaration once per
//! checkpoint. `collapse_repeats` exists to keep that from growing the decoder's history for the life
//! of the process. The two tests that pinned it do it through checkpoints under a pin:
//! `integration_alter_column::the_decoders_history_does_not_grow_with_every_checkpoint` and
//! `adversarial_i20::a5_history_growth_under_a_live_subscription_pin`. Since D234 a checkpoint under a
//! pin re-declares only once a pin has passed the last declaration, and both tests hold their pin at
//! the base, so those checkpoints append nothing and both tests pass whether or not
//! `collapse_repeats` exists (the D234 adversary's F7). Identical declarations still reach a
//! decoder: every real truncation re-declares, a following pin's advance re-declares, and an
//! archived log holds whatever its writer wrote. So this test feeds the same run by hand.
//!
//! Passes at `00f4c39` and must keep passing. It fails when `collapse_repeats` drops nothing
//! (bench/d216/mutants.py M19).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use ferrodb::catalog::column::DataType;
use ferrodb::replication::logical::LogicalDecoder;
use ferrodb::wal::log::{DdlOp, RecKind, WalManager};

#[test]
fn identical_declarations_under_a_held_pin_do_not_grow_the_decoders_history() {
    const ROUNDS: usize = 60;
    let dir = tempfile::tempdir().unwrap();
    let wal = Arc::new(WalManager::new(dir.path().join("history.wal")).unwrap());
    let declaration = RecKind::Ddl {
        op: DdlOp::CreateTable,
        table: "a".into(),
        dir_root: 7,
        time_travel_root: 8,
        columns: vec![("id".into(), DataType::Integer, false)],
    };
    wal.append(0, 0, &declaration).unwrap();
    wal.flush().unwrap();
    let base = wal.base_lsn.load(Ordering::SeqCst);
    // A live subscription's claim: nothing below it may be pruned from the history.
    let _pin = wal.pin(base).expect("pin");

    let decoder = LogicalDecoder::blank();
    let mut cursor = base;
    for _ in 0..ROUNDS {
        wal.append(0, 0, &declaration).unwrap();
        wal.flush().unwrap();
        let to = wal.next_lsn.load(Ordering::SeqCst);
        decoder.decode(&wal, cursor, to).expect("decode");
        cursor = to;
    }
    assert_eq!(wal.base_lsn.load(Ordering::SeqCst), base, "premise failed: the base moved, so pruning could hide the growth");
    let len = decoder.history_len();
    assert!(
        len <= 2,
        "the decoder remembers {len} entries for {} identical declarations of one table; the history grows \
         with every re-declaration",
        ROUNDS + 1
    );
}
