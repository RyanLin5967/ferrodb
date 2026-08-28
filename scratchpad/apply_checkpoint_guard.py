#!/usr/bin/env python3
"""The fourth node-local decision, found by sweeping `fetch_add` across src/ rather than by the brief.

`consensus::Command::Checkpoint` exists and its own doc names this exact hole:

    **Checkpointing has to be a replicated decision**, which is not obvious and is the reason
    this variant exists. `TxnManager` checkpoints on a node-local counter and calls
    `wal.truncate`; two nodes doing that at different moments have different WAL byte streams
    from then on. Since an LSN is an offset into that stream, a follower promoted to leader
    would then append into an offset space its own followers do not share.

That counter is `commits_since_checkpoint` in src/wal/txn.rs:582 — a file this row owns. Applied
here rather than reported, because the guard is the same shape as the other three and the
alternative is a named hole in my own file.
"""
import pathlib, sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
P = ROOT / "src/wal/txn.rs"

OLD_TRIGGER = """        if self.commits_since_checkpoint.fetch_add(1, Ordering::SeqCst) + 1 >= checkpoint_interval() && self.att.lock().unwrap().is_empty() {
            self.checkpoint()?;
        }"""

NEW_TRIGGER = """        // **F4: the automatic checkpoint is a node-local decision, and on a cluster it is wrong.**
        //
        // `consensus::Command::Checkpoint` exists for this and says why in its own words:
        // "`TxnManager` checkpoints on a node-local counter and calls `wal.truncate`; two nodes
        // doing that at different moments have different WAL byte streams from then on. Since an
        // LSN is an offset into that stream, a follower promoted to leader would then append into
        // an offset space its own followers do not share."
        //
        // So a member counts and waits. The counter is deliberately **not** reset when the
        // checkpoint is withheld, so it stays over the threshold and the round that finally applies
        // `Command::Checkpoint` does the work — deferred, never dropped, and readable through
        // [`TxnManager::checkpoint_due`] so a leader loop knows to propose one.
        //
        // Withholding is the safe direction, and the asymmetry is the reason to choose it: a WAL
        // that was not truncated is replayed at recovery and costs disk, while a WAL truncated at a
        // different offset on each node is not repairable at all.
        //
        // Only the *automatic* trigger is guarded. Explicit `checkpoint()` calls — DDL in
        // `executor.rs`, clean exit in `cli.rs` — sit on paths that are themselves replicated
        // decisions (`Command::Catalog`), so they are already ordered by the log; guarding them
        // here would refuse DDL on a cluster member for a reason that does not apply.
        let due = self.commits_since_checkpoint.fetch_add(1, Ordering::SeqCst) + 1
            >= checkpoint_interval()
            && self.att.lock().unwrap().is_empty();
        if due && !crate::cluster::is_clustered() {
            self.checkpoint()?;
        }"""

OLD_ANCHOR = """    /// The id watermark: everything below it has been issued. Diagnostic and recovery-facing.
    pub fn next_txn_id(&self) -> u64 {
        self.txn_ids.issued_through()
    }
"""

NEW_ANCHOR = OLD_ANCHOR + """
    /// Apply a committed [`crate::consensus::Command::Checkpoint`].
    ///
    /// The replicated entry point for the work the commit counter drives on a single node. Every
    /// node applies this at the same round, so every node truncates its WAL at the same *logical*
    /// point even though the byte offset differs — which is the whole reason the decision has to
    /// travel in the log rather than be taken locally.
    pub fn apply_checkpoint(&self) -> Result<(), FerroError> {
        self.checkpoint()
    }

    /// Whether this node has accumulated enough commits that it wants a checkpoint.
    ///
    /// How a leader loop knows to propose [`crate::consensus::Command::Checkpoint`]. On a standalone
    /// node this is transiently true at most until the next commit, because there the automatic
    /// trigger fires and resets it.
    pub fn checkpoint_due(&self) -> bool {
        self.commits_since_checkpoint.load(Ordering::SeqCst) >= checkpoint_interval()
    }
"""


def main():
    s = P.read_text()
    for old, new in ((OLD_TRIGGER, NEW_TRIGGER), (OLD_ANCHOR, NEW_ANCHOR)):
        if old not in s:
            print(f"REFUSING: anchor not found:\n{old[:120]}")
            return 1
        s = s.replace(old, new, 1)
    P.write_text(s)
    print("applied")
    return 0


if __name__ == "__main__":
    sys.exit(main())
