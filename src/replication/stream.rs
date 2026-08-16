//! E11 — streaming the change feed, so it is a source rather than a decode.
//!
//! [`super::logical`] decodes a fixed LSN range and [`super::jsonl`] writes it out. A CDC consumer
//! does not want a range: it wants to follow a database as it changes, and to resume where it left
//! off after a restart. That is a loop around those two pieces plus **one cursor rule**, and the
//! cursor rule is the whole of the difficulty.
//!
//! # The cursor may only advance past a commit
//!
//! After emitting a batch, the obvious move is to set the cursor to the durable frontier — the
//! decode covered everything up to there, so everything up to there is done. **That is wrong, and
//! it loses data silently.**
//!
//! A transaction in flight when the pump runs has already written records below the frontier and
//! has not written its commit. The decoder correctly withholds them (reporting the transaction as
//! open). If the cursor then moves to the frontier, the next pump starts *after* those records —
//! and when the commit finally lands, the decoder sees a commit for a transaction whose changes it
//! never read. The rows are gone from the feed permanently, and nothing downstream can tell,
//! because a feed that is missing records looks exactly like a feed that had none.
//!
//! So the cursor advances **only to the highest `commit_end_lsn` actually emitted**. If a pump
//! emits nothing, the cursor does not move at all, however much log it just read. Re-reading the
//! records of an in-flight transaction on the next pump is pure waste and is the correct waste:
//! the alternative is losing them.
//!
//! # The other rule, inherited
//!
//! **Never emit a change derived from a record the primary has not durably written.** It is the
//! same rule physical replication has and it matters more here, because a CDC consumer acts on
//! events — it writes to a warehouse, sends a webhook, bills someone. A crash that erases work a
//! consumer has already acted on cannot be walked back. So a pump reads only up to `flushed_lsn`.
//!
//! # What a consumer gets
//!
//! At-least-once, in commit order. The feed itself will not deliver a committed change twice — a
//! consumer resuming from a `commit_end_lsn` it recorded starts strictly after that commit — but a
//! consumer that acts on an event and dies before recording its position will see it again. That
//! is the usual CDC contract and it is why events carry `commit_lsn`: it is a natural idempotence
//! key.

use std::io::Write;

use crate::error::FerroError;
use crate::wal::log::WalManager;

use super::jsonl::write_feed;
use super::logical::{Decoded, LogicalDecoder};

/// What one pump did.
#[derive(Debug, Clone, PartialEq)]
pub struct Pumped {
    /// Change events written this time.
    pub emitted: usize,
    /// Where the consumer should resume **reading**.
    ///
    /// This is NOT "everything below here has been delivered" - it is clamped back to the earliest
    /// record of any still-open transaction, so the reader can pick that transaction up when it
    /// commits. Delivery progress is [`Pumped::emitted_through`], and the two are different numbers
    /// whenever a transaction is in flight.
    pub cursor: u64,
    /// The highest `commit_lsn` already delivered.
    ///
    /// Pass it back to the next `pump` alongside `cursor`. Without it, clamping `cursor` for an
    /// open transaction re-delivers every committed transaction after it on EVERY pump, for as long
    /// as that transaction stays open - measured at 15 events for 3 transactions over 5 rounds
    /// before this existed, and unbounded in principle.
    pub emitted_through: u64,
    /// How far the pump was allowed to read — the durable frontier at the time.
    pub frontier: u64,
    /// Transactions seen but withheld because they had not committed yet. They are **not** lost;
    /// the cursor deliberately did not advance past them.
    pub withheld: usize,
    /// Records the decoder could not attribute to a table. Non-zero means the feed is incomplete
    /// and a caller should say so rather than present it as a clean run.
    pub unresolved: usize,
}

impl Pumped {
    /// Whether everything readable became an event or was legitimately withheld.
    pub fn is_clean(&self) -> bool {
        self.unresolved == 0
    }

    /// How far behind the log this consumer is, in bytes.
    ///
    /// **An upper bound, not an exact distance, and the difference is not pedantry.** The cursor
    /// tracks *commits* while the frontier is a byte position that includes records producing no
    /// events at all — a `TxnEnd` sits above the final commit permanently. So a fully caught-up
    /// consumer reports a small non-zero lag rather than zero, and code that waits for `lag == 0`
    /// waits for ever. That mistake hung the CDC server until a Go consumer reading to EOF found
    /// it; "nothing left to emit" is the caught-up test, not "lag is zero".
    pub fn lag_bytes(&self) -> u64 {
        self.frontier.saturating_sub(self.cursor)
    }
}

/// Follows a WAL, emitting committed changes as JSON Lines.
pub struct FeedStreamer {
    decoder: LogicalDecoder,
    /// Largest batch of log to decode in one pump, in bytes.
    max_bytes: u64,
}

impl FeedStreamer {
    pub fn new(decoder: LogicalDecoder) -> Self {
        FeedStreamer { decoder, max_bytes: 1 << 20 }
    }

    /// Bound how much log one pump will decode. A consumer that has been away for a long time
    /// should not cause one unbounded allocation.
    pub fn with_max_bytes(mut self, max_bytes: u64) -> Self {
        self.max_bytes = max_bytes.max(1);
        self
    }

    /// Where a brand-new consumer should start: the beginning of the retained log.
    ///
    /// Not zero — `truncate` moves the base forward, so asking from 0 is asking for records that no
    /// longer exist.
    pub fn start_cursor(wal: &WalManager) -> u64 {
        wal.base_lsn.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Decode everything committed between `cursor` and the durable frontier, write it, and return
    /// the new cursor.
    ///
    /// See the module docs: the returned cursor is the highest `commit_end_lsn` emitted, **not**
    /// the frontier, and it is unchanged when nothing was emitted.
    pub fn pump<W: Write>(
        &self,
        wal: &WalManager,
        cursor: u64,
        emitted_through: u64,
        w: &mut W,
    ) -> Result<Pumped, FerroError> {
        use std::sync::atomic::Ordering;

        // Durable only. Reading to `next_lsn` would let a consumer act on work a crash erases.
        let frontier = wal.flushed_lsn.load(Ordering::SeqCst);
        let base = wal.base_lsn.load(Ordering::SeqCst);
        if cursor < base {
            return Err(FerroError::Wal(format!(
                "cursor {cursor} is below the log's base {base}: the records it points at have been \
                 truncated away, so resuming there would silently skip everything between. Take a \
                 new snapshot of the tables and restart the feed from {base}."
            )));
        }
        if cursor >= frontier {
            return Ok(Pumped {
                emitted: 0,
                cursor,
                emitted_through,
                frontier,
                withheld: 0,
                unresolved: 0,
            });
        }

        let to = frontier.min(cursor.saturating_add(self.max_bytes));
        let decoded: Decoded = self.decoder.decode(wal, cursor, to)?;

        // Suppress what has already been delivered. The cursor is a READ position and is clamped
        // back for open transactions, so a plain re-read would hand the consumer every committed
        // transaction after that one again, on every pump, forever.
        let fresh: Vec<_> = decoded
            .events
            .iter()
            .filter(|e| e.commit_lsn > emitted_through)
            .cloned()
            .collect();
        let emitted = write_feed(&fresh, w)?;
        let delivered_through = fresh
            .iter()
            .map(|e| e.commit_lsn)
            .max()
            .unwrap_or(emitted_through)
            .max(emitted_through);

        // **The cursor rule**, and it has two halves. The second was missing and cost real data.
        //
        // First: only past a commit that was actually emitted. An empty pump does not move.
        //
        // Second: NEVER past the earliest record of a transaction still open. Without this, a
        // transaction that is in flight while a LATER-started one commits in the same batch gets
        // stepped over - the cursor jumps to the later commit, and when the open one finally
        // commits the decoder sees a `Commit` with nothing staged and emits nothing. The rows are
        // gone, silently, and the feed reports success. Reproduced before fixing:
        //
        //   T1 commit, T2 open, T3 commit   ->  pump1 emitted=2 cursor=415 withheld=1
        //   T2 commits                      ->  pump2 emitted=0
        //   feed holds T1 and T3; T2's row never arrives.
        //
        // Clamping rewinds over T3's already-emitted events, so they are delivered twice. That is
        // the right trade and it is the contract: this feed is AT-LEAST-ONCE, consumers dedupe on
        // `commit_lsn`, and duplication is recoverable where loss is not.
        let emitted_max = decoded
            .events
            .iter()
            .map(|e| e.commit_end_lsn)
            .max()
            .unwrap_or(cursor)
            .max(cursor);
        let next = match decoded.open_from {
            Some(open_from) => emitted_max.min(open_from),
            None => emitted_max,
        };

        Ok(Pumped {
            emitted,
            cursor: next,
            emitted_through: delivered_through,
            frontier,
            withheld: decoded.open.len(),
            unresolved: decoded.unresolved.values().sum::<usize>()
                + decoded.undecodable.values().sum::<usize>(),
        })
    }
}

/// A live consumer's place in the log, with a **claim on the records it still needs**.
///
/// Without this, a streaming consumer is broken by every checkpoint. Measured rather than
/// theorised: a latency run of 1000 commits died at commit ~256 with *"cursor 46446 is below the
/// log's base 46661: the records it points at have been truncated away"*. The 200-commit run before
/// it had passed — for the same reason E4's 40-row replication test passed, which is to say for a
/// reason that does not generalise past the checkpoint interval.
///
/// So a subscription pins the log at its cursor, exactly as a base backup does, and moves the pin
/// forward as it advances. The cost is the same one and it is not free: **the WAL cannot be
/// reclaimed below the slowest live consumer**, so a consumer that stops reading and never drops
/// its subscription is a log that never shrinks. That is the trade every replication slot makes,
/// and it is the right one here — the alternative was measured too, and it is a feed that breaks
/// every 256 commits.
pub struct Subscription {
    wal: std::sync::Arc<WalManager>,
    cursor: u64,
    /// Highest `commit_lsn` delivered so far. Held here so a caller using a `Subscription` cannot
    /// forget it - the bare `pump` makes it the caller's problem, and forgetting it is unbounded
    /// re-delivery rather than a visible error.
    emitted_through: u64,
    pin: crate::wal::log::WalPin,
}

impl std::fmt::Debug for Subscription {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Deliberately not printing the WalManager: it has no Debug, and a subscription is
        // identified by where it sits, not by the log it sits in.
        write!(f, "Subscription(cursor {}, {:?})", self.cursor, self.pin)
    }
}

impl Subscription {
    /// Subscribe from `from`, claiming the log there.
    ///
    /// Refuses if that position has already been truncated away — the same refusal `pump` gives,
    /// made at subscription time so a consumer learns immediately rather than on its first read.
    pub fn new(wal: &std::sync::Arc<WalManager>, from: u64) -> Result<Self, FerroError> {
        let pin = wal.pin(from)?;
        Ok(Subscription { wal: std::sync::Arc::clone(wal), cursor: from, emitted_through: 0, pin })
    }

    /// Subscribe from the start of the retained log.
    pub fn from_start(wal: &std::sync::Arc<WalManager>) -> Result<Self, FerroError> {
        let from = FeedStreamer::start_cursor(wal);
        Self::new(wal, from)
    }

    /// Resume a subscription a previous process was running, restoring BOTH positions.
    ///
    /// A consumer must persist both, and persisting only a commit position loses data. Measured
    /// rather than reasoned: with a transaction in flight, a consumer that stored only its highest
    /// `commit_lsn` and resumed reading there missed that transaction's rows entirely when it
    /// committed, because its records sit BELOW the commit that was recorded. Storing
    /// [`Pumped::cursor`] as well kept them.
    ///
    /// `cursor` is where to resume reading; `emitted_through` is what has already been delivered
    /// and is what stops the replay between them being handed to the consumer twice.
    pub fn resume(
        wal: &std::sync::Arc<WalManager>,
        cursor: u64,
        emitted_through: u64,
    ) -> Result<Self, FerroError> {
        let pin = wal.pin(cursor)?;
        Ok(Subscription { wal: std::sync::Arc::clone(wal), cursor, emitted_through, pin })
    }

    /// What this subscription has delivered. Persist alongside [`Subscription::cursor`].
    pub fn emitted_through(&self) -> u64 {
        self.emitted_through
    }

    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Pump, then move the claim forward to the new cursor.
    ///
    /// The new pin is taken **before** the old one is released. Releasing first would open a window
    /// in which this consumer holds no claim at all, and a checkpoint landing in that window
    /// discards precisely the records it is about to ask for — the same check-then-act shape that
    /// has produced most of the defects in this codebase.
    ///
    /// **Moving the pin forward is not load-bearing today, and that is worth stating rather than
    /// implying otherwise.** `truncate` discards the whole log rather than a prefix, so a pin held
    /// at the subscription's *start* blocks reclamation exactly as effectively as one held at its
    /// cursor — measured, by removing the forward move and watching every test still pass. What it
    /// does change is `min_pinned_lsn`, which is the signal a prefix-truncating checkpoint would
    /// consult, and which is asserted below. So this is the same kind of thing as the base
    /// comparison in `read_from`: correct, cheap, and the piece that starts mattering the day
    /// truncation learns to discard a prefix.
    pub fn pump<W: Write>(
        &mut self,
        streamer: &FeedStreamer,
        w: &mut W,
    ) -> Result<Pumped, FerroError> {
        let pumped = streamer.pump(&self.wal, self.cursor, self.emitted_through, w)?;
        self.emitted_through = pumped.emitted_through;
        if pumped.cursor != self.cursor {
            let next = self.wal.pin(pumped.cursor)?;
            let old = std::mem::replace(&mut self.pin, next);
            drop(old);
            self.cursor = pumped.cursor;
        }
        Ok(pumped)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::column::{Column, DataType, Value};
    use crate::catalog::schema::Schema;
    use crate::storage::tuple::Tuple;
    use crate::wal::log::RecKind;

    fn schema() -> Schema {
        Schema::new(vec![
            Column { name: "id".into(), data_type: DataType::Integer, nullable: false },
            Column { name: "qty".into(), data_type: DataType::Integer, nullable: true },
        ])
    }

    fn tuple_bytes(id: i32, qty: i32) -> Vec<u8> {
        Tuple::serialize(&[Value::Integer(id), Value::Integer(qty)], &schema(), 0)
            .unwrap()
            .data
    }

    /// A streamer over one table at `dir_root` 7.
    fn streamer() -> FeedStreamer {
        FeedStreamer::new(LogicalDecoder::for_table(7, "inventory", schema(), 8))
    }

    fn wal(tag: &str) -> (tempfile::TempDir, WalManager) {
        let d = tempfile::tempdir().unwrap();
        let w = WalManager::new(d.path().join(format!("{tag}.wal"))).unwrap();
        (d, w)
    }

    fn insert(w: &WalManager, txn: u64, id: i32, qty: i32) {
        w.append(
            txn,
            0,
            &RecKind::HeapInsert { dir_root: 7, page_id: 1, slot: 0, tuple: tuple_bytes(id, qty) },
        )
        .unwrap();
    }

    /// **A transaction still open while a LATER one commits must not be stepped over.**
    ///
    /// This is silent data loss, and it was shipped: the cursor advanced to the later commit, and
    /// when the open transaction finally committed the decoder saw a `Commit` with nothing staged
    /// and emitted nothing. The rows never reached the feed and the feed reported success.
    ///
    /// The arrangement matters - T2 must open BEFORE T3 and commit AFTER it, all inside one batch.
    /// An earlier version of the cursor test only covered "nothing emitted", which this passes.
    #[test]
    fn an_open_transaction_is_not_stepped_over_when_a_later_one_commits() {
        let (_d, w) = wal("interleave");
        let s = streamer();
        let mut cursor = FeedStreamer::start_cursor(&w);

        w.append(1, 0, &RecKind::Begin).unwrap();
        insert(&w, 1, 1, 10);
        w.append(1, 0, &RecKind::Commit).unwrap();

        // Opens second, commits last.
        w.append(2, 0, &RecKind::Begin).unwrap();
        insert(&w, 2, 2, 20);

        // Opens third, commits before T2.
        w.append(3, 0, &RecKind::Begin).unwrap();
        insert(&w, 3, 3, 30);
        w.append(3, 0, &RecKind::Commit).unwrap();
        w.flush().unwrap();

        let mut feed = Vec::new();
        let p1 = s.pump(&w, cursor, 0, &mut feed).unwrap();
        assert!(p1.emitted >= 2, "T1 and T3 should both have been emitted: {p1:?}");
        assert_eq!(p1.withheld, 1, "T2 should be reported as withheld: {p1:?}");
        assert!(
            p1.cursor < p1.frontier,
            "the cursor reached the frontier while a transaction was still open at {:?}; it has \
             stepped over T2's records and they can never be emitted",
            decoded_open_hint()
        );
        cursor = p1.cursor;

        w.append(2, 0, &RecKind::Commit).unwrap();
        w.flush().unwrap();
        let p2 = s.pump(&w, cursor, 0, &mut feed).unwrap();
        assert!(p2.emitted >= 1, "T2's rows never arrived after it committed: {p2:?}");

        let text = String::from_utf8(feed).unwrap();
        assert!(
            text.contains("\"qty\":20"),
            "T2's row was lost. Feed:\n{text}"
        );
        // Every row present at least once. Duplicates are allowed and expected - clamping the
        // cursor rewinds over T3, and this feed is at-least-once by contract.
        for q in ["\"qty\":10", "\"qty\":20", "\"qty\":30"] {
            assert!(text.contains(q), "{q} missing from the feed:\n{text}");
        }
    }

    fn decoded_open_hint() -> &'static str {
        "the decoder reported an open transaction"
    }

    /// **A restart must restore BOTH positions, or an in-flight transaction is lost.**
    ///
    /// This is the failure the resumability contract used to describe: `commit_end_lsn` was
    /// documented as "where a consumer resumes", and a consumer that did exactly that lost a
    /// transaction which opened before the recorded commit and committed after the restart. Its
    /// records sit BELOW the commit that was persisted.
    #[test]
    fn resuming_from_a_commit_position_alone_loses_an_in_flight_transaction() {
        let (_d, w) = wal("restart");
        let w = std::sync::Arc::new(w);
        let s = streamer();

        // T9 opens and stays open across the restart. T1 commits after it.
        w.append(9, 0, &RecKind::Begin).unwrap();
        insert(&w, 9, 99, 990);
        w.append(1, 0, &RecKind::Begin).unwrap();
        insert(&w, 1, 1, 10);
        w.append(1, 0, &RecKind::Commit).unwrap();
        w.flush().unwrap();

        let mut sub = Subscription::from_start(&w).unwrap();
        let mut buf = Vec::new();
        let p = sub.pump(&s, &mut buf).unwrap();
        assert_eq!(p.emitted, 1, "T1 should have been delivered: {p:?}");
        let (read_at, delivered) = (sub.cursor(), sub.emitted_through());
        assert!(
            read_at < delivered,
            "the read cursor {read_at} is not behind the delivered position {delivered}, so this \
             test is not exercising a restart with a transaction in flight"
        );
        drop(sub);

        w.append(9, 0, &RecKind::Commit).unwrap();
        w.flush().unwrap();

        // The WRONG restart: resume reading at the commit position, as the old doc said to.
        let mut wrong = Subscription::resume(&w, delivered, delivered).unwrap();
        let mut wb = Vec::new();
        wrong.pump(&s, &mut wb).unwrap();
        assert!(
            !String::from_utf8(wb).unwrap().contains("\"qty\":990"),
            "resuming at the commit position happened to keep the row - if that is now true the \
             hazard is gone and this test should be rewritten rather than relaxed"
        );

        // The RIGHT restart: both positions.
        let mut right = Subscription::resume(&w, read_at, delivered).unwrap();
        let mut rb = Vec::new();
        right.pump(&s, &mut rb).unwrap();
        assert!(
            String::from_utf8(rb).unwrap().contains("\"qty\":990"),
            "restoring both positions still lost the in-flight transaction"
        );
    }

    /// **Clamping the cursor must not turn into unbounded re-delivery.**
    ///
    /// The clamp above is required so an in-flight transaction is not stepped over. On its own it
    /// pins the read position beneath every transaction that commits afterwards, so each pump
    /// re-reads and re-emits all of them - measured at 15 events for 3 transactions over 5 rounds,
    /// and unbounded for as long as the open transaction lives. A consumer using `emitted == 0` as
    /// its caught-up signal never terminates.
    ///
    /// `emitted_through` is the second position that makes both properties hold at once: read from
    /// the low-water mark, deliver only past the high-water mark.
    #[test]
    fn a_long_open_transaction_does_not_cause_endless_redelivery() {
        let (_d, w) = wal("redeliver");
        let s = streamer();

        // Opens and stays open for the whole test.
        w.append(9, 0, &RecKind::Begin).unwrap();
        insert(&w, 9, 99, 990);

        for i in 1..=3u64 {
            w.append(i, 0, &RecKind::Begin).unwrap();
            insert(&w, i, i as i32, i as i32 * 10);
            w.append(i, 0, &RecKind::Commit).unwrap();
        }
        w.flush().unwrap();

        let (mut cursor, mut through) = (FeedStreamer::start_cursor(&w), 0u64);
        let mut total = 0usize;
        for round in 1..=5 {
            let mut buf = Vec::new();
            let p = s.pump(&w, cursor, through, &mut buf).unwrap();
            total += p.emitted;
            cursor = p.cursor;
            through = p.emitted_through;
            assert_eq!(p.withheld, 1, "round {round}: the open transaction stopped being reported");
        }
        assert_eq!(
            total, 3,
            "three committed transactions produced {total} events across five pumps - the clamped \
             cursor is re-delivering them every round"
        );

        // And the open one is still picked up when it finally commits, which is the property the
        // clamp exists for. Suppressing re-delivery must not suppress this.
        w.append(9, 0, &RecKind::Commit).unwrap();
        w.flush().unwrap();
        let mut buf = Vec::new();
        let p = s.pump(&w, cursor, through, &mut buf).unwrap();
        assert_eq!(p.emitted, 1, "the long-open transaction was lost: {p:?}");
        assert!(
            String::from_utf8(buf).unwrap().contains("\"qty\":990"),
            "the wrong row arrived for the long-open transaction"
        );
    }

    /// **A live subscription survives a checkpoint that would otherwise truncate under it.**
    ///
    /// Without the pin this is the failure a 1000-commit latency run hit at commit ~256.
    #[test]
    fn a_subscription_holds_the_log_across_a_checkpoint() {
        let (_d, w) = wal("subscribe");
        let w = std::sync::Arc::new(w);
        let s = streamer();

        w.append(1, 0, &RecKind::Begin).unwrap();
        insert(&w, 1, 1, 10);
        w.append(1, 0, &RecKind::Commit).unwrap();
        w.flush().unwrap();

        let mut sub = Subscription::from_start(&w).unwrap();
        let base_before = w.base_lsn.load(std::sync::atomic::Ordering::SeqCst);

        // More work, then a checkpoint that would discard what the subscriber has not read.
        for i in 2..=4u64 {
            w.append(i, 0, &RecKind::Begin).unwrap();
            insert(&w, i, i as i32, i as i32 * 10);
            w.append(i, 0, &RecKind::Commit).unwrap();
        }
        w.flush().unwrap();
        w.truncate(99).unwrap();

        assert_eq!(
            w.base_lsn.load(std::sync::atomic::Ordering::SeqCst),
            base_before,
            "the checkpoint discarded log a live subscriber still needed"
        );

        // And the subscriber can still read everything it was promised.
        let mut buf = Vec::new();
        let p = sub.pump(&s, &mut buf).unwrap();
        assert_eq!(p.emitted, 4, "the subscriber lost events across the checkpoint: {p:?}");

        // The claim moved forward with the consumer. Not observable through `truncate`, which is
        // all-or-nothing, but this is the value a prefix-truncating checkpoint would consult — and
        // asserting it is what makes the forward move testable at all.
        assert_eq!(
            w.min_pinned_lsn(),
            Some(sub.cursor()),
            "the subscription's claim did not move with its cursor"
        );
        assert!(
            sub.cursor() > base_before,
            "the cursor never advanced, so the claim had nowhere to move"
        );

        // Dropping it releases the claim, so the log can be reclaimed again.
        drop(sub);
        w.truncate(100).unwrap();
        assert!(
            w.base_lsn.load(std::sync::atomic::Ordering::SeqCst) > base_before,
            "the log was never reclaimed even after the subscription was dropped"
        );
    }

    /// Subscribing to a position already gone is refused at subscribe time, not at first read.
    #[test]
    fn subscribing_below_the_base_is_refused_immediately() {
        let (_d, w) = wal("subgone");
        let w = std::sync::Arc::new(w);
        w.append(1, 0, &RecKind::Begin).unwrap();
        insert(&w, 1, 1, 1);
        w.append(1, 0, &RecKind::Commit).unwrap();
        w.flush().unwrap();
        w.truncate(7).unwrap();
        let base = w.base_lsn.load(std::sync::atomic::Ordering::SeqCst);
        assert!(base > 1);

        let err = Subscription::new(&w, base - 1).expect_err("a stale subscription was accepted");
        assert!(format!("{err}").contains("truncated"), "wrong reason: {err}");
        assert!(Subscription::new(&w, base).is_ok(), "a current subscription was refused");
    }

    /// Lag shrinks as a consumer catches up, and is an upper bound rather than an exact zero.
    #[test]
    fn lag_shrinks_as_a_consumer_catches_up() {
        let (_d, w) = wal("lag");
        let s = streamer();
        let start = FeedStreamer::start_cursor(&w);

        for i in 1..=5u64 {
            w.append(i, 0, &RecKind::Begin).unwrap();
            insert(&w, i, i as i32, i as i32);
            w.append(i, 0, &RecKind::Commit).unwrap();
        }
        w.flush().unwrap();

        let behind = FeedStreamer::start_cursor(&w);
        let mut buf = Vec::new();
        let p = s.pump(&w, behind, 0, &mut buf).unwrap();
        assert!(p.emitted > 0, "nothing was emitted, so lag cannot be judged");

        let mut buf2 = Vec::new();
        let caught_up = s.pump(&w, p.cursor, 0, &mut buf2).unwrap();
        assert!(
            caught_up.lag_bytes() < p.frontier - start,
            "lag did not shrink after catching up: {} vs {}",
            caught_up.lag_bytes(),
            p.frontier - start
        );
        // Deliberately NOT asserting zero: the tail holds records that produce no events, so a
        // caught-up consumer legitimately reports a small positive lag.
    }

    #[test]
    fn a_pump_emits_committed_changes_and_advances_past_them() {
        let (_d, w) = wal("basic");
        let s = streamer();
        let start = FeedStreamer::start_cursor(&w);

        w.append(1, 0, &RecKind::Begin).unwrap();
        insert(&w, 1, 1, 10);
        w.append(1, 0, &RecKind::Commit).unwrap();
        w.flush().unwrap();

        let mut buf = Vec::new();
        let p = s.pump(&w, start, 0, &mut buf).unwrap();
        assert_eq!(p.emitted, 1, "nothing was emitted for a committed insert");
        assert!(p.cursor > start, "the cursor did not advance past a commit");
        assert!(p.is_clean(), "{p:?}");
        assert!(String::from_utf8(buf).unwrap().contains("\"qty\":10"));
    }

    /// **The rule this module exists for.**
    ///
    /// A transaction in flight has records below the frontier and no commit. The pump must emit
    /// nothing AND leave the cursor alone — if it advanced to the frontier, the later commit would
    /// arrive with its changes already stepped over, and those rows would never reach the feed.
    #[test]
    fn an_in_flight_transaction_does_not_move_the_cursor_and_is_emitted_after_it_commits() {
        let (_d, w) = wal("inflight");
        let s = streamer();
        let start = FeedStreamer::start_cursor(&w);

        w.append(1, 0, &RecKind::Begin).unwrap();
        insert(&w, 1, 7, 70);
        insert(&w, 1, 8, 80);
        w.flush().unwrap(); // durable, but NOT committed

        let mut buf = Vec::new();
        let first = s.pump(&w, start, 0, &mut buf).unwrap();
        assert_eq!(first.emitted, 0, "an uncommitted change was emitted");
        assert_eq!(
            first.cursor, start,
            "the cursor advanced over an in-flight transaction; its rows would be lost forever"
        );
        assert_eq!(first.withheld, 1, "the open transaction was not reported as withheld");
        assert!(
            first.frontier > start,
            "the frontier did not move, so this test never had the chance to skip anything"
        );

        // Now it commits. The changes written BEFORE the first pump must still arrive.
        w.append(1, 0, &RecKind::Commit).unwrap();
        w.flush().unwrap();

        let mut buf2 = Vec::new();
        let second = s.pump(&w, first.cursor, 0, &mut buf2).unwrap();
        assert_eq!(second.emitted, 2, "the committed rows did not arrive: {second:?}");
        let text = String::from_utf8(buf2).unwrap();
        assert!(text.contains("\"qty\":70") && text.contains("\"qty\":80"), "{text}");
        assert!(second.cursor > first.cursor, "the cursor did not advance after the commit");
    }

    /// Pumping again with the returned cursor must not re-deliver what was already emitted.
    #[test]
    fn resuming_from_the_returned_cursor_does_not_repeat_events() {
        let (_d, w) = wal("resume");
        let s = streamer();
        let mut cursor = FeedStreamer::start_cursor(&w);

        for i in 1..=3 {
            w.append(i, 0, &RecKind::Begin).unwrap();
            insert(&w, i, i as i32, i as i32 * 10);
            w.append(i, 0, &RecKind::Commit).unwrap();
        }
        w.flush().unwrap();

        let mut all = Vec::new();
        let first = s.pump(&w, cursor, 0, &mut all).unwrap();
        assert_eq!(first.emitted, 3);
        cursor = first.cursor;

        let mut again = Vec::new();
        let second = s.pump(&w, cursor, 0, &mut again).unwrap();
        assert_eq!(second.emitted, 0, "resuming re-delivered events: {}", String::from_utf8_lossy(&again));
        assert_eq!(second.cursor, cursor, "an empty pump moved the cursor");
    }

    /// A pump must never read past the durable frontier, or a consumer acts on work a crash erases.
    #[test]
    fn changes_that_are_not_durable_yet_are_not_emitted() {
        let (_d, w) = wal("durable");
        let s = streamer();
        let start = FeedStreamer::start_cursor(&w);

        w.append(1, 0, &RecKind::Begin).unwrap();
        insert(&w, 1, 5, 50);
        w.append(1, 0, &RecKind::Commit).unwrap();
        // Deliberately NOT flushed.

        let mut buf = Vec::new();
        let p = s.pump(&w, start, 0, &mut buf).unwrap();
        assert_eq!(
            p.emitted, 0,
            "a change was emitted from a record the primary has not durably written"
        );
        assert_eq!(p.cursor, start);

        // And once it is durable, it arrives — so the assertion above is about durability and not
        // about the pump being broken.
        w.flush().unwrap();
        let mut buf2 = Vec::new();
        assert_eq!(s.pump(&w, start, 0, &mut buf2).unwrap().emitted, 1);
    }

    /// A cursor pointing into truncated log must be refused with an explanation, not silently
    /// clamped forward — clamping would skip every change between and report success.
    #[test]
    fn a_cursor_below_the_logs_base_is_refused_by_name() {
        let (_d, w) = wal("truncated");
        let s = streamer();

        w.append(1, 0, &RecKind::Begin).unwrap();
        insert(&w, 1, 1, 1);
        w.append(1, 0, &RecKind::Commit).unwrap();
        w.flush().unwrap();
        w.truncate(9).unwrap();
        let base = w.base_lsn.load(std::sync::atomic::Ordering::SeqCst);
        assert!(base > 1, "the log did not truncate, so there is nothing to have lost");

        let mut buf = Vec::new();
        let err = s.pump(&w, base - 1, 0, &mut buf).expect_err("a stale cursor was accepted");
        let msg = format!("{err}");
        assert!(msg.contains("truncated away"), "wrong reason: {msg}");
        assert!(msg.contains("skip"), "the message does not say what would go wrong: {msg}");
    }

    /// A long-absent consumer must not cause one unbounded decode. The batch stops early and the
    /// cursor still lands on a commit, so the next pump continues cleanly.
    #[test]
    fn a_large_backlog_is_delivered_in_bounded_batches() {
        let (_d, w) = wal("backlog");
        let s = streamer().with_max_bytes(300);
        let mut cursor = FeedStreamer::start_cursor(&w);

        for i in 1..=20u64 {
            w.append(i, 0, &RecKind::Begin).unwrap();
            insert(&w, i, i as i32, i as i32);
            w.append(i, 0, &RecKind::Commit).unwrap();
        }
        w.flush().unwrap();

        let mut total = 0;
        let mut rounds = 0;
        loop {
            let mut buf = Vec::new();
            let p = s.pump(&w, cursor, 0, &mut buf).unwrap();
            if p.emitted == 0 {
                break;
            }
            total += p.emitted;
            cursor = p.cursor;
            rounds += 1;
            assert!(rounds < 100, "streaming did not terminate");
        }
        assert_eq!(total, 20, "not every change was delivered across batches");
        assert!(rounds > 1, "the backlog came out in one batch, so bounding was never exercised");
    }
}
