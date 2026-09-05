# ferrodb

A distributed relational database written from scratch in Rust — storage engine, B+trees, query planner, WAL,
MVCC, Postgres wire protocol. No runtime dependencies; `[dependencies]` in `Cargo.toml` is empty.

The part worth your time is what sits on top: **the unit of isolation is an agent task, not a
transaction.** An agent opens a session, gets a private branch of the whole database, writes
whatever it wants, and either merges or gets thrown away. Nobody sees any of it until it merges.

```sql
BEGIN AGENT SESSION AS 'restock' RUN 'r_7' MODEL 'claude-opus-5' PROMPT 'top up anything low';
UPDATE inventory SET qty = qty - 12 WHERE id = 7;
SELECT * FROM inventory AS OF BRANCH b_1;   -- only this agent sees it
MERGE;                                       -- Clean | Commuting | Conflict | ResolvedWithLoss
```

## Running it

Needs Rust 1.85+ (edition 2024).

```
git clone https://github.com/RyanLin5967/ferrodb.git
cd ferrodb && cargo run           # or: cargo run -- mydb.db
```

Prebuilt binaries for Linux, macOS and Windows are on the releases page.

`cargo test` also wants Go 1.25+ with cgo and the `sqlite3` CLI, because the change-feed tests drive
a consumer written in Go and check its output with sqlite3. That independence is the point — an
encoder checked by its own decoder agrees with itself about any shared misreading. Those tests fail
loudly instead of skipping when the toolchain is missing, since a test that silently skips is a test
that always passes. `cargo build` and `cargo run` need none of it.

## Why transactions are the wrong unit

A transaction isolates operations. An agent's task is long, speculative, and usually abandoned
halfway through — and the load-bearing assumption is that **agents abandon work and never tell
you.** Any design that needs the client to call `close` leaks forever.

So a branch is just a root pointer. Forking sets `child.root_page_id = parent.root_page_id` and
appends a fork epoch to the parent's live-children list — one metadata record, and that's it.

Measured, not claimed: forking copies zero data pages, 44 allocated whether you have 10 branches or 1000 (`bench/branch_scaling.txt`).

Because the child's root *is* the parent's root, ordinary B+tree descent already reaches parent
data — there's no "not found here, ask my parent" step, which is the thing that makes overlay
designs fall over as branches pile up.

Every branch carries a lease. A background scan hard-reaps anything past its deadline with no client
cooperation at all, and the page count goes back to baseline. That's the whole thesis, and the demo
watches it happen: 32 branches take a lease, write pages, and are never closed — no `close`, no
`commit`, no `ABANDON`. The control is temporal, not structural: run the identical scan *before* the
leases expire and it reaps nothing.

Merges are three-way against the fork point and report four outcomes, not three. `ResolvedWithLoss`
matters — a policy that succeeded while discarding somebody's write is not `Clean`, and telling an
agent otherwise is the most dangerous thing this system could do.

```
cargo run --release --example agent_isolation_demo
```

It computes its own verdicts. Delete the code behind a criterion and the verdict changes.

The server does this too, not just the demo: both binaries run a lease thread that finishes any reap
a crash interrupted, then scans every 30s and reaps anything past its deadline. It takes the same
per-statement lock a `MERGE` holds, so it never runs mid-statement, and there's no value that turns
it off — reclaiming abandoned branches is the product, not an option.
`tests/integration_server_reaps.rs` proves it against the binaries without sending them any SQL.

## It's distributed

Three nodes, Raft-style: leader election with pre-vote and a leader lease, log replication with
quorum commit, membership changes, and a deterministic simulator that replays a whole campaign —
partition, split vote, stale-log candidate — from a seed.

The consensus state machine is *stepped*, never self-driving. It never reads a clock, never dials
anything, never spawns a thread; it returns actions and the caller performs them. That's not style,
it's the reason any of it can be tested — a protocol tested by sleeping is tested on the happy path.

Nodes agree on a **round**, not an LSN, because ferrodb's LSN is a byte offset and offsets are
node-local: two nodes holding the same records hold them at different offsets, a follower given
offsets can't tell a hole from a gap, and a checkpoint truncates the log so neither an offset nor a
record count survives it.

`tests/integration_consensus_failover.rs` starts three real processes, kills the leader with
`kill -9` mid-write, and asserts a new one takes over having lost nothing acknowledged.

One trade worth stating plainly: an agent's speculative writes never reach a quorum, only the merge
does — so you can fan out a hundred agents and pay consensus once per accepted result. The cost is
that a leader dying takes its in-flight branches with it. A branch is a transaction, and no database
survives losing a node mid-transaction with uncommitted work intact. It just stings more here,
because an agent branch holds a 15-minute lease rather than living for milliseconds.

## Change data capture

Logical decoding off the WAL, with publications deciding what may leave the database — a column left
out of a publication is absent from the feed, not nulled. Row attribution rides along, so a consumer
can see which agent, run and model wrote a row.

Two sinks land the feed (SQLite and DuckDB), both in a separate Go module under `cdc-consumer/`,
and both checked with a different reader than the one that wrote — the DuckDB sink is verified with
the `duckdb` CLI rather than the driver that produced the rows.

## What it doesn't do

- **Table space is fixed at creation.** The copy-on-write arena owns everything above a floor chosen
  when the database is made, and raising the knob later won't move it. The error says so.
- **The Postgres wire protocol is a subset.** Enough for real drivers — asyncpg and pg8000 both run
  parameterised queries against it in CI — not enough to be a drop-in.
- **Node signing proves possession of the key, not freshness.** There's no replay protection, so a
  captured frame can be sent again.

These are listed because an admitted gap is worth more than a fabricated pass.

## Where it stands

1873 Rust tests and 97 Go, green on Linux, macOS and Windows, in about 60k lines.

The one gap worth knowing about: ordinary SQL still writes to the heap, not to the copy-on-write
tree. The branch engine is real — zero-copy fork, lease reaping, interval reclamation, all measured
— but agent rows are staged in memory until a merge publishes them, so the two stores are separate.
Closing that means migrating `CREATE TABLE`, `INSERT` and `SELECT` onto the tree, and every partial
version leaves both live and able to disagree quietly, which is why it hasn't been done piecemeal.

## Why I built it

I wanted to know whether the databases underneath agent workloads are actually shaped for them, and
the honest answer seemed to be no — everything assumes a short transaction and a client that cleans
up after itself. Neither holds. Building the whole stack was the only way to find out where that
assumption is load-bearing, and it turns out to be load-bearing almost everywhere: in how you fork,
in what you can reclaim, in whether a merge can tell you it lost your write.
