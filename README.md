# ferrodb

A relational database built from scratch in Rust.

It has a second identity: **ferrobranch**, an *agent-isolation* database in which the unit of
isolation is an agent task rather than a transaction. All ten of its exit criteria are demonstrated
by a runnable demo that computes its own verdicts — `cargo run --release --example
agent_isolation_demo`. See [ferrobranch — agent isolation](#ferrobranch--agent-isolation) below,
including an explicit account of what is **not** covered.


## How to run

### From release

Prebuilt binaries (in zip files) for Linux, macOS, and Windows are in the releases page. Download the one for your OS, unzip, then run the executable. 

### From source

Requires Rust 1.85 or newer (this project uses edition 2024).

`cargo test` additionally needs **Go 1.25+ with cgo enabled** and the **`sqlite3` CLI** on PATH. The
change-feed tests drive a consumer written in Go and check its output with the sqlite3 command — the
independence is the point, since an encoder validated by its own decoder agrees with itself about
any shared misreading. Those tests **fail loudly** rather than skipping when the toolchain is
missing: a test that silently skips is a test that always passes. `cargo build` and `cargo run`
need none of it.

The **`duckdb` CLI** is optional on a developer machine and is the one exception to that rule. It is
not shipped with any OS here, so its absence is a fact about the machine rather than a broken
checkout; the DuckDB sink tests fall back to a second process through the Go driver and print which
reader ran, because a green run on the weaker reader must not be mistaken for one that exercised the
CLI. Set **`FERRODB_REQUIRE_DUCKDB_CLI=1`** to turn that notice into a failure — CI does, so the
fallback is never what CI silently measures. cgo is *not* optional: the DuckDB driver links DuckDB
statically, so `CGO_ENABLED=0` fails to build the whole consumer module, SQLite sink included.

```
git clone https://github.com/RyanLin5967/ferrodb.git
cd ferrodb
cargo run
```
You can add an argument if you want a custom name. For example, `cargo run -- customname.db` will persist tables in `customname.db`.
### Agent isolation, in the binary you just built

The branch engine is not a library sitting beside the CLI — `cargo run` puts you on it. This is a
real transcript, not an illustration:

```
$ cargo run -- shop.db
ferrodb=> CREATE TABLE inv (id INTEGER NOT NULL, qty INTEGER);
ok
ferrodb=> INSERT INTO inv VALUES (1, 10);
(1 row affected)

ferrodb=> BEGIN AGENT SESSION AS 'pricing' RUN 'r_1';
agent session b_1 on b1@g0 (agent=pricing run=r_1)
ferrodb=> INSERT INTO inv VALUES (2, 20);
(1 row affected)
ferrodb=> SELECT * FROM inv;            -- the agent sees its own write
1 | 10
2 | 20
(2 rows)
ferrodb=> DIFF;
diff b0@g0 -> b1@g0: 1 row(s)
  INSERT inv.row2 [pending]
    op RowCreate on <row>
```

Now leave that session — **without** merging — and open the database again. ferrodb is
single-writer, so this is a second session rather than a second terminal: opening the same database
twice at once is refused, because two processes writing one database would hand the same
copy-on-write pages to different branches. (Concurrent clients connect to the pgwire server instead,
which shares one runtime across connections; see the branch-isolation test in
`tests/pg/pg_agent_client.py`.)

The agent's row is not there — its branch was abandoned along with the session that opened it:

```
ferrodb=> SELECT * FROM inv;
1 | 10
(1 row)
```

`MERGE;` publishes it, reports the outcome per row, and the result survives a restart:

```
ferrodb=> MERGE;
m_1 b1@g0 -> b0@g0: Clean
  inv.row2: Clean
```

The isolation is enforced by shadow paging, not by staging rows somewhere: the agent's write copies
pages into a reserved arena above the ordinary table region, which is what
`tests/integration_cli_agent_isolation.rs` checks by watching the database file grow past the arena
floor. Those tests drive the built binary over a pipe, so they cannot pass by calling a constructor
the binary does not call — reverting the wiring fails three of the five.

`FERRODB_ARENA_HEADROOM` sets how many pages of ordinary table growth are reserved below the arena
floor (default 32736, about 128 MB). It is read once, when a database's arena is first created, and
then persisted: changing it later cannot move an existing database's floor, because moving the floor
would put pages the arena already owns back into the ordinary allocator's circulation.

### Supported SQL

Here is the SQL syntax that has been implemented so far:

```
CREATE TABLE name (col TYPE [NOT NULL], ...)
CREATE INDEX name ON table(col);

INSERT INTO table VALUES (...);
UPDATE table SET col = expr [, ...] [WHERE expr];
DELETE FROM table [where expr];

SELECT cols 
FROM table [AS] [alias] 
[ [INNER | LEFT [OUTER]] JOIN table2 [AS] [alias] ON expr] 
[WHERE expr];
```

- **Types:** INTEGER (i32), BIGINT (i64), DECIMAL / NUMERIC (exact, unbounded digits),
  TIMESTAMP (epoch milliseconds, i64), FLOAT (f64), BOOLEAN, VARCHAR(n)
- **Literals:** integers, floats, single quoted strings, TRUE, FALSE, NULL. A bare numeric literal
  is read as the declared type of the column it is written to or compared against, so
  `123456789012345678901234567890.5` reaches a DECIMAL column with every digit intact instead of
  being rounded to an f64 on the way.
- **DECIMAL** has no declared precision or scale. It stores the digits you wrote — `1.50` stays
  `1.50` — and there is no decimal arithmetic: this engine stores and ships decimals, it does not
  add them. Comparison is numeric, so `1.50` and `1.5` are equal.
- **Operators:** = != <= > >= + - * / AND OR NOT
- **Columns:** *, qualified references, table aliases, qualified star

### Try it yourself
Start the REPL (either `cargo run`/`cargo run -- mydb.db` or by unziping then executing the binary).
Statements end with a `;` and may span multiple lines. Everything is saved to the .db file, so data persists
between runs. You can delete the file (e.g. `ferro.db`) to start from scratch. Type `.exit` or press Ctrl + D to exit your current session. 

The following session creates two tables, inserts rows, filters, and runs a join. 
You can paste it line by line and you should see exactly this output:

``` 
ferrodb: type .exit to quit
ferrodb=> CREATE TABLE users (id INTEGER NOT NULL, name VARCHAR(32), age INTEGER);
ok
ferrodb=> INSERT INTO users VALUES (1, 'alice', 30);
(1 row affected)
ferrodb=> INSERT INTO users VALUES (2, 'bob', 25);
(1 row affected)
ferrodb=> SELECT * FROM users;
1 | alice | 30
2 | bob | 25
(2 rows)
ferrodb=> SELECT name, age FROM users WHERE age > 26;
alice | 30
(1 row)
ferrodb=> CREATE TABLE posts (id INTEGER NOT NULL, user_id INTEGER, title VARCHAR(32));
ok
ferrodb=> INSERT INTO posts VALUES (1, 1, 'hello');
(1 row affected)
ferrodb=> INSERT INTO posts VALUES (2, 1, 'world');
(1 row affected)
ferrodb=> SELECT u.name, p.title FROM users u INNER JOIN posts p ON u.id = p.user_id;
alice | hello
alice | world
(2 rows)
ferrodb=> .exit
bye bye
```
You can also create indexes (`CREATE INDEX idx ON users (age);`), updates (`UPDATE users SET age = 31 WHERE id = 1;`), and deletes (`DELETE FROM posts WHERE id = 2;`).

Here is a resource for the SQL language (refer back to `Supported SQL` to see what syntax is supported): https://www.w3schools.com/sql/default.asp 
## How it works

Queries go through layers one at a time:

```
SQL text 
    -> Scanner: tokenize
    -> Parser: recursive descent -> AST
    -> Binder: name resolution + semantic checks -> logical plan
    -> Planner: lower logical plan -> physical operators
    -> Executor: Volcano (pull based) iterators -> rows      
```
Execution operators sit on top of storage layers:
```
Executor 
    -> Catalog: table metadata, schemas, index roots
    -> HeapFileManager: slotted pages + page directory
       B+ Tree: primary and secondary indexes
    -> BufferPoolManager: in memory page cache (with ARC eviction)
    -> DiskManager: page-level IO to disk
```

## ferrobranch — agent isolation

Transactions isolate *operations*. They are the wrong unit for an LLM agent, whose task is long,
speculative, and frequently abandoned halfway through. ferrobranch makes the unit of isolation the
**agent task**: an agent opens a session, gets a private branch of the whole database, writes freely,
and either merges or is thrown away. Nothing it did is visible to anyone until it merges.

The load-bearing assumption is that **agents abandon their work and never tell you**. A design that
requires the client to call `close` is a design that leaks forever.

### The three layers

**1. Branch engine.** A branch *is* a root pointer. Forking sets `child.root_page_id =
parent.root_page_id` and appends a `fork_epoch` to the parent's sorted live-children array — one
durable metadata record, and **zero data pages read, written, or refcounted**.

Because the child's root *is* the parent's root at fork time, ordinary B+tree descent already
reaches parent data, so the read path never walks a parent chain. That is a hard rule, not an
optimisation: BranchBench (arXiv 2604.17180) measured the "not found here, ask my parent" overlay
pattern at up to **5400x read degradation** as branches accumulate.

Storage is a copy-on-write B+tree with shadow paging, fixed 4KB pages, and a self-describing page
header carrying `birth_epoch`. Reclamation is ZFS-style birth-time algebra generalised from a linear
chain to a tree:

> page `p` is reclaimable iff no live child has `fork_epoch` in `[birth(p), free(p))`

which is a range-emptiness query over a sorted array — O(log k), no global liveness question. Novel
pages come from per-branch ~1MB arenas, so reaping a childless branch is an extent-level free, and a
branch that dies before flushing its ~1MB write buffer has allocated **nothing at all**.

Every branch carries a `lease_deadline`, and a background reaper hard-reaps anything past it **with
no client cooperation whatsoever**. `BranchId` carries a generation counter, so a reaped id can
never be mistaken for a live one — reading a reaped branch is a hard error, never stale data.

**2. Provenance and read-sets.** The actor tuple (agent, run, model, model version, prompt hash) has
*run-level* cardinality — it is constant across every row a run writes — so storing it literally per
version is pure waste. **Measured, not asserted** (`store.rs::the_density_numbers_the_docs_quote_are_the_numbers_this_computes`):
the tuple is **101 bytes**, against a **1-byte** slot plus a one-entry page dictionary, so 200
versions cost **20,200 bytes literal against 204 interned — 99x**. The figure this section used to
quote (~3.4x) was the *row-inflation* number and depended on a row size nobody stated: the same
tuple inflates a 24-byte version by 5.2x, a 40-byte one by 3.5x and a 100-byte one by 2.0x. It is
interned: a page-local dictionary slot points at a reified `RunEntity`. Read-sets are retained too,
in a form chosen by **access shape** rather than size — point lookups keep exact version ids, scans
keep a predicate summary. Retaining reads is what makes causal rollback possible: reverting write A
can find the write B that *read* A. It halts and shows the tree by default; cascade is explicit.

**3. Typed Effect Log, merge, and verification gate.** Writes are logged as typed operations
(`RowCreate`, `RowDelete`, `Assign`, `Add`, `Max`, `Min`, `SetInsert`, `SetRemove`) alongside the **guards** that made them
legal. Guards are the part that genuinely cannot be reconstructed from a byte WAL — numeric deltas
can be, but `WHERE qty >= 5` cannot. Merge is three-way against the fork point, which is strictly
stronger than CRDT replication: no per-replica vectors that grow without bound.

Merge reports **four** outcomes, and the fourth is the point:

| Outcome | Meaning |
|---|---|
| `Clean` | main untouched |
| `Commuting` | both branches wrote, and the ops compose (`Add`+`Add`, `SetInsert`∪`SetInsert`) |
| `Conflict` | contradictory, or a guard failed when re-evaluated against merged state |
| `ResolvedWithLoss` | a policy succeeded **while discarding a write** |

Reporting `ResolvedWithLoss` as `Clean` is the most dangerous thing this system could do to an
agent, so it is a distinct outcome by construction. On `Conflict` the **violated predicate is handed
back**, so the agent retries with real feedback instead of a boolean.

### How it differs from Dolt and Neon

**Dolt** is git-for-data: content-addressed Merkle/prolly trees, commits and diffs aimed at humans.
Content addressing forces a *global* liveness question — you cannot free a chunk without a global
statement about who else references it — which is exactly why Dolt needs copying mark-and-sweep GC.
ferrobranch deliberately uses **no content addressing, no reference counts, and no immutable
segments**: birth-epoch algebra answers reclamation locally. Refcounts were rejected for the same
class of reason — one parent with 5000 children would put refcount 5001 on the most-shared page in
the database, which is btrfs's backref explosion.

**Neon** branches Postgres cheaply at the storage layer by copy-on-write over a page server at an
LSN. It is genuinely cheap to branch — but branches are a *service and recovery* feature: there is
no merge back, no semantic conflict story, and no notion of who wrote a row or why. ferrobranch is
built for the return path. Branching is the easy half; **merging, attributing, verifying and reaping
are the product.**

The nearest whole-system prior art is Write-Audit-Publish on Iceberg branches — with the difference
that the audit step here has retained read-sets to work from, including the `write-set \ read-set`
metric: rows an agent changed without ever looking at them.

### Status — what is actually demonstrated

Run it yourself: `cargo run --release --example agent_isolation_demo`. Every verdict below is
computed by a check inside the demo, not written into a table by hand — removing the code behind a
criterion makes its verdict change.

- `cargo test` — **699 passed, 0 failed**
- the demo reports **10 MET, 0 PARTIAL, 0 NOT MET** of the ten exit criteria, and exits non-zero
  if any self-check fails
- the thesis criterion is observed firing: 32 branches take a lease, write novel pages, and are
  **never closed** — no `close`, `commit`, `abort` or `ABANDON`. The lease scan reaps them and the
  allocated page count returns to baseline. The control is **temporal**: the identical scan run
  *before* the leases expire reaps 0, which is what shows the reaper frees on expiry rather than
  freeing whatever it is pointed at. (This bullet previously claimed a healthy long-lease branch
  sat in the baseline as the control. There is none — the only survivor is trunk, and trunk is
  excluded by an `is_trunk()` filter rather than by its lease, so it never tested what was claimed.)

Measured rather than asserted:

| Claim | Measurement |
|---|---|
| forking copies zero data pages | 44 pages at 10, 100 and 1000 branches |
| read latency does not degrade with branch count | descent p50 flat (x1.00) from 10 to 1000 *diverged* branches |
| a crash mid-merge leaves no torn state | process killed inside the publish loop at 3 points; database untouched every time |

The benchmark **calibrates before reporting** — growing the tree 20x moves descent p50 13.6 → 20.5µs
— and refuses to print numbers if the instrument cannot move, because "flat" from a gauge that
cannot respond would prove nothing. Raw output is committed at `bench/branch_scaling.txt`.

### What this does *not* do

Kept here deliberately; a fabricated pass would be worse than an admitted gap.

- **Table space is capped when the database is created.** The copy-on-write arena owns every page
  from a fixed floor upward and ordinary tables own everything below it, so tables stop growing at
  that floor even though the file can. `FERRODB_ARENA_HEADROOM` sets it (default 32736 pages, about
  128 MB of table space) and is read **once, at creation**, then persisted — raising it later cannot
  move an existing database's floor, because pages above it already belong to live branches. The
  error says so, and names the remedy, rather than reporting a page number and leaving the reader to
  set a variable that will not help. The default is a trade: a distant floor costs nothing on a
  filesystem with sparse files and materialises the whole gap on one without, which is what CI's
  Windows runner has.
- **The pgwire server is still map-backed.** It builds `Session::new()`, which sets
  `storage: None`, so an agent session there stages rows in memory. The CLI and the demo no longer
  do: `src/cli/cli.rs` builds a `LogBranchCatalog` and an `ArenaPageStore` and calls `with_storage`
  on create / `reopen_with_storage` on reattach, and `examples/agent_isolation_demo.rs` builds its
  runtime the same way. *This entry said until 2026-08-16 that nothing in `src/` constructed such a
  runtime and that `Session::with_runtime` had no caller; both were true when written and are now
  false, which is why the correction is recorded rather than the sentence quietly replaced.*
- **Trunk is heap-backed, so a branch tree holds a delta rather than a table.** Reads are served
  from the heap plus the workspace overlay, and a fork's copy-on-write tree carries its staged
  delta, not a copy of the base table. `tests/integration_trunk_tree_authority.rs` pins this by
  measurement — trunk's tree reads back empty while the branch's holds the staged row — and
  criterion 2 of the demo prints both counts rather than asserting the arrangement.
- **A guard must name the amount taken.** `qty >= 12` is refused correctly; written as the invariant
  `qty >= 0`, two agents each taking 12 from 20 both merge and the counter reaches **−4**. Guards are
  preconditions evaluated *before* the composed ops apply, so a precondition cannot see a post-op
  violation. Escrow (`EscrowLedger`) is the answer and is implemented — claim the slack at fork and
  the overdraw is refused at *write* time — with two scope limits worth stating plainly: it is
  **opt-in per cell**, and it governs **agent-session writes only**. A plain `UPDATE` outside a
  session never reaches the capture point and is not charged, so "the counter cannot go below its
  floor" is true of agents and not of direct SQL.
- **Crash safety means process death, not power loss.** The test kills the process with `abort()`;
  bytes already handed to `write()` survive in the OS page cache, so nothing here exercises a dead
  machine.
- **`psql` itself has not been run.** The Postgres wire subset is verified by an independently
  written client that speaks the same protocol, not by psql, which is not installed on the machine
  this was built on.
- **The verification gate reports; it does not decide.** The blind-write metric is a heuristic, and
  a heuristic's outcome is quarantine, so it never blocks a merge on its own.

## Change data capture

Physical replication ships *pages*, which keeps a replica byte-identical and tells a consumer
nothing about **what changed**. The same WAL also drives a logical change feed.

```
$ cargo run --example cdc_feed | jq -c '{op, table, after}'
{"op":"INSERT","table":"inventory","after":{"id":1,"item":"widget","qty":10}}
{"op":"UPDATE","table":"inventory","after":{"id":1,"item":"widget","qty":999}}
{"op":"DELETE","table":"inventory","after":null}
```

- **Only committed transactions, in commit order.** Changes buffer per transaction and release on
  `Commit`; an `Abort` discards them and an in-flight transaction is reported as withheld rather
  than emitted. A consumer shown an aborted transaction's rows has been told about data that never
  existed.
- **Resumable, from two positions rather than one.** A consumer persists where to resume *reading*
  and what it has already been *delivered*, and they are different numbers whenever a transaction is
  in flight. Persisting only a commit position loses data: a transaction that opened before that
  commit has records *below* it, so a restart reads past them and never sees that transaction at
  all. Measured, not reasoned — a consumer resuming from its highest `commit_lsn` lost an in-flight
  transaction's row, while one restoring both positions kept it.

  The read cursor advances only past a commit that was actually emitted, and never past the earliest
  record of a still-open transaction. Clamping it that way re-reads transactions that committed
  afterwards, which is why the delivered position exists to suppress them — read from the low-water
  mark, deliver past the high-water mark.
- **Initial snapshot with a handoff, at-least-once.** A consumer joining a database that already has
  rows reads the current contents as `READ` events, then streams from the LSN captured *before* the
  scan. That direction is deliberate: handing off after the scan silently loses concurrent changes,
  while handing off before re-delivers a few — and duplication is recoverable where loss is not.
  This is `snapshot_table`, kept for callers holding only a WAL.
- **…or exactly once, given a transaction manager.** `snapshot_table_exact` takes the read *inside a
  transaction*, so it knows precisely which transactions its rows already contain, and hands back a
  `SnapshotBoundary` the stream uses to skip exactly those
  (`FeedStreamer::resuming_after_snapshot`, paired with `Subscription::following` so the resume
  cursor comes from the boundary rather than from the caller). Every row then appears **exactly
  once** across the two feeds. Skipping by LSN cannot do this: the resume point has to reach back
  over any transaction that was already in flight — MVCC excludes its uncommitted work from the
  snapshot, and its records sit *below* the scan — and reaching back drags in commits the snapshot
  did contain. Those two sets interleave in the log, so no byte offset separates them; only the
  transaction id does. The boundary carries the *table set* as well, because a transaction id alone
  answers *when* and not *what*: a snapshot of `orders` says nothing about `shipments`, and a
  transaction-only filter would drop `shipments` rows that were in no snapshot at all.
  `tests/integration_cdc_cutover.rs` asserts exactly-once over a scenario holding one transaction
  open across the cutover, and its companion test asserts that `snapshot_table` both duplicates and
  drops on that same scenario.
- **Never ahead of durability.** No change is emitted from a WAL record the primary has not durably
  written, because a CDC consumer *acts* on events and a crash cannot un-send a webhook.

Two things the log says that a naive decoder gets wrong, both found by decoding real executor
output rather than hand-built records: a SQL `DELETE` is an MVCC `HeapUpdate` (so mapping record
kinds onto change kinds reports every delete as an update, and a consumer keeps a row forever), and
superseded row versions written to the time-travel heap are internal traffic (emitting them
double-counts every update as an insert).

### The consumer is a separate program, in a separate language

`cdc-consumer/` is a small Go program that shares no code with the database. It validates the feed
against the documented envelope using Go's `encoding/json` — which rejects `NaN` and `Infinity`
outright — and, in `follow` mode, materialises the stream into a local table:

```
$ go run . follow 127.0.0.1:5555 -key id
```

The tests judge the feed by comparing that materialised table against the source, so a feed that is
well-formed, correctly ordered and *wrong* still fails. An encoder validated only by its own
author's idea of the format agrees with itself about any shared misreading.

### Wide values ship as strings, on purpose

JSON has one number type and no stated precision, and the overwhelmingly common consumer
behaviour is to parse every JSON number into an **IEEE 754 double** — that is what JavaScript's
`JSON.parse` does, and what Go's `encoding/json` does into `interface{}`. A double carries a 53-bit
significand, so `9223372036854775807` comes back as `9223372036854775808`, `9007199254740993` comes
back as `9007199254740992`, and a decimal past 17 significant digits comes back rounded. **No error
is raised** for any of it: the parse succeeds and the number is simply wrong.

So `BIGINT`, `DECIMAL` and `TIMESTAMP` are emitted as JSON **strings**, which no parser coerces
(envelope fields elided here — a real line also carries `txn`, `lsn`, `commit_lsn`,
`commit_end_lsn` and `before`):

```json
{"op":"INSERT","table":"wide","after":{"id":1,"big":"9223372036854775807","dec":"1.50","ts":"1700000000123"}}
```

`INTEGER` deliberately stays a bare number — it is `i32`, three orders of magnitude inside what a
double holds exactly, and stringifying it would break every consumer reading that column today.
`FLOAT` stays a number too, since it *is* a double.

`cdc-consumer precision <feed.jsonl>` reports the JSON type of every column and flags any whose
digits a default float64 decode would corrupt. `tests/integration_cdc_wide_types.rs` runs it over a
feed produced by real SQL (expecting zero corrupted columns) and then over a hand-built feed
carrying the same values as bare numbers, requiring it to report the corruption — so a clean result
means the checker works rather than that it never fires.

**Limits:** there is no wire framing beyond newline delimiting, and the feed is JSON rather than a
compact binary format. `TIMESTAMP` is epoch milliseconds with no calendar formatting, and over
pgwire it is announced as `int8` rather than `timestamp` for that reason. `DECIMAL` supports no
arithmetic, and its text cannot exceed 65535 bytes (the row encoding's length prefix).
### Landing the feed: SQLite and DuckDB sinks

A change feed nobody lands anywhere is a demo. `cdc-consumer sink` writes it into a destination
database — SQLite for an operational replica, DuckDB for the analysts' copy:

```
$ go run . sink feed.jsonl -db out.sqlite -key id                  # default engine
$ go run . sink feed.jsonl -db out.duckdb -key id -engine duckdb
```

Both carry the same four properties, and they are the whole point, because the feed is
**at-least-once**: a sink will be handed the same event twice, and can be handed a stale one after a
newer one. Re-applying an old `UPDATE` overwrites current data with a previous value; re-applying an
`INSERT` after a `DELETE` resurrects a row the source no longer has. Both leave the destination
silently wrong *and self-consistent*, which is the worst failure a pipeline can have.

- Every destination row carries `_commit_lsn`, the commit that last wrote it.
- An event applies **only if its `commit_lsn` is strictly greater**. That test lives in the
  `ON CONFLICT … DO UPDATE … WHERE` clause, not in the program's control flow, so every write path
  inherits it — including one added later by someone who did not read the comment above it.
- Deletes are **soft**. A hard delete throws away the LSN, and with it the only evidence that would
  reject a stale re-insert arriving afterwards. The tombstone is what makes "gone" stick.
- `CREATE_TABLE` events drive the destination DDL, learned in band and in log order.

The DuckDB destination is checked with the **`duckdb` CLI** — a different binary and a different
build of DuckDB from the one the Go driver links — for the same reason the feed is validated by a
separate program. On a machine with no CLI the tests fall back to a second process through the Go
driver and say so; that fallback is the weaker check, and `both_readers_agree` pins the two together
wherever both exist. Because DuckDB is *typed*, its tests catch something SQLite's cannot: a sink
that declared every column `TEXT` would pass every SQLite assertion, and fails here.

CI installs a pinned `duckdb` CLI on all three runners and sets `FERRODB_REQUIRE_DUCKDB_CLI=1`,
which makes falling back to the Go reader a **failure** rather than a quiet degradation. That
variable is the point of the arrangement: without it, a CI run whose CLI install had stopped working
would report exactly the same green as one that compared against the CLI.

This corrects what this section said until recently — that the runners had no CLI, so the comparison
ran nowhere but a developer's laptop. That was accurate when written and is why it is recorded here
rather than quietly deleted: the comparison had been *written* and was *running nowhere*, and no
test failure would ever have said so. The fallback is held to the CLI's exact rendering — `NULL`
printed as four characters, a `DOUBLE` of 2 printed `2.0`, a `TIMESTAMP` printed without a zone —
and `both_readers_agree` compares those cases specifically, since queries returning plain non-null
scalars agree by accident and prove nothing.

`-engine duckdb` needs **cgo** (`github.com/marcboeker/go-duckdb` links DuckDB statically), so
`CGO_ENABLED=0` will not build the consumer at all — the cost is module-wide, not per-engine.

**Limits:** there is no wire framing beyond newline delimiting, and the feed is JSON rather than a
compact binary format. `ALTER TABLE` is not carried — `CREATE_TABLE` and `DROP_TABLE` are, so a
consumer learns a table's shape and its disappearance but not a column added later. The sinks
replace whole rows rather than merging, which is correct only because this feed always emits full
before/after images.

## Replication — what it gives you, and what it cannot

There is a working primary/replica pair: **asynchronous physical WAL log shipping** over TCP. A
replica restores a base backup, connects, says how far it has got, and follows the primary's log
until it converges. Convergence is judged in the tests by comparing page bytes on both disks, not
by asking either process whether it thinks it worked.

Two guarantees hold and are tested:

- **A primary never ships a record it has not durably written.** A replica holding records the
  primary loses on a crash is *ahead* of its primary — divergence, not lag, and nothing downstream
  can reconcile it. The source stops at `flushed_lsn`.
- **Applying is idempotent and all-or-nothing.** Redo goes through the same code path recovery
  uses, so a reconnect's re-sent overlap is absorbed rather than double-applied; a batch with one
  bad frame applies none of it, so the replica never sits at an LSN it cannot account for.

**Without consensus, this is not a highly-available cluster, and the gap is not a detail.** There
is no Raft, no leader election, no automatic failover, no split-brain protection. Two nodes that
both believed they were primary would diverge and nothing here would notice. Promoting a replica is
a manual act with no safety net. That is why the checklist below still has `Distributed
replication (Raft)` unchecked — log shipping is a real component of replication, and it is not the
hard part.

Three further limits, each found by a test rather than reasoned about:

- **A replica needs a base backup, and a base backup holds the primary's WAL open.** The primary
  checkpoints every 256 commits and truncates its log, so there is nothing for a bare replica to
  start from. A backup takes a *pin* that stops the next checkpoint discarding what it points into.
  This log cannot be truncated part-way, so a pin means keeping all of it: **a backup handle that
  is never dropped is a WAL that never shrinks.** PostgreSQL replication slots have the same
  hazard.
- **Only pages the WAL describes are replicated.** The catalog and the heap page directory are
  written outside the log, so a base backup carries them *as of the instant it ran* and nothing
  afterwards updates them. Measured directly: after a backup taken while the primary was still
  inserting, every WAL-described page matched byte-for-byte and every page outside the log did not.
  The practical consequence is that a backup taken while the primary is running does **not** by
  itself give a usable replica — take it when the schema is settled.
- **Synchronous commit is available and off by default.** With it on, commit waits for a replica to
  acknowledge the LSN, so a primary crash cannot lose work a client was told had committed. When no
  replica can acknowledge, it neither blocks forever nor commits silently: it returns an error
  naming the lsn it wanted, how far the furthest replica got, that the data is durable on the
  primary, and that nothing was rolled back. With one replica and no consensus that trade cannot be
  designed away, only stated.
- **Reconnect and catch-up works, and its ordering is the replica's half of the durability rule.**
  A replica records progress only *after* the pages it describes are durable, so a crash leaves its
  state file behind the pages and never ahead — behind is repaired by idempotent redo, ahead would
  be a replica claiming an LSN whose pages never reached disk. Tested by aborting a replica at a
  fixed batch count mid-stream and restarting it.

## Current progress

### ferrodb (the SQL database)

- [x] Disk Manager (page-level IO, bitmap-based page allocation)
- [x] Page layout and tuple serialization
- [x] Buffer pool manager
- [x] B+ tree indexing
- [x] SQL parser
- [x] Query execution engine
- [x] Cost-based query optimizer
- [x] Write-ahead logging with crash recovery
- [x] MVCC (tuple version chains, snapshot visibility)
- [x] Postgres wire protocol (v3 subset: startup, simple query, errors — see the caveat above)
- [x] Asynchronous physical replication: WAL log shipping over TCP, base backup, WAL pin
- [ ] Distributed replication (Raft) — no consensus, no failover; see the section above

### ferrobranch (agent isolation)

- [x] Branch records, ids with generation counters, fork epochs
- [x] CoW page header with `birth_epoch`
- [x] Typed Effect Log: ops, guards, three-way merge algebra with four outcomes
- [x] Read-set representations and revert/dependency structures
- [x] CoW B+tree and store, with a structural diff that prunes shared subtrees by page id
- [x] Per-branch arenas and write buffers
- [x] Non-cooperative lease reaper (**the thesis**) — observed firing, pages back to baseline
- [x] Provenance capture on the write path: a merge-published version names its agent, run and model
- [x] SQL surface: `BEGIN AGENT SESSION`, `AS OF BRANCH`, `DIFF`, `MERGE`, `REVERT ... CASCADE`
- [x] Verification gate tiers, ordered by cost ÷ rejection-probability, and the
      `write-set \ read-set` blind-write metric
- [x] Quarantine: a declined branch stays unmerged but still queryable
- [x] Escrow at fork, so a bounded-counter overdraw fails at write time
- [x] Depth guard + `COLLAPSE` at ancestry depth 8
- [ ] SQL statements writing directly to CoW pages (the largest remaining gap, above)

## Why I built it

I wanted to know how a database actually works and the best way to do that is to build a database from scratch. For example, how do bytes on disk become rows in a query result, how a database optimizes queries, etc.