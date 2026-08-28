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

ferrodb=> BEGIN AGENT SESSION AS 'pricing' RUN 'r_1' PROMPT 'reprice the slow movers';
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

To publish it the agent has to still be in its session — a branch abandoned with its connection
cannot be merged afterwards, and `MERGE;` on its own answers `no agent session in this connection`.
So this is a fresh session that redoes the write and merges it. The branch is `b2` rather than `b1`
because branch ids come from the durable catalog and do not restart:

```
ferrodb=> BEGIN AGENT SESSION AS 'pricing' RUN 'r_2';
agent session b_2 on b2@g0 (agent=pricing run=r_2)
ferrodb=> INSERT INTO inv VALUES (2, 20);
(1 row affected)
ferrodb=> MERGE;
m_1 b2@g0 -> b0@g0: Clean
  inv.row2: Clean
```

The result survives a restart — open the database once more and both rows are there:

```
ferrodb=> SELECT * FROM inv;
1 | 10
2 | 20
(2 rows)
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
CREATE FULLTEXT INDEX name ON table(col);

INSERT INTO table VALUES (...);
UPDATE table SET col = expr [, ...] [WHERE expr];
DELETE FROM table [where expr];

SELECT cols 
FROM table [AS] [alias] 
[ [INNER | LEFT [OUTER]] JOIN table2 [AS] [alias] ON expr] 
[WHERE expr];

SEARCH table (col) FOR 'search text' [TOP k];
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
- **Full-text search.** `CREATE FULLTEXT INDEX` on a VARCHAR column builds a posting list, which
  here is not a new structure at all: it is the same B+tree a secondary index uses, keyed
  `(token, primary key)` instead of `(column value, primary key)`. `SEARCH` reads it and returns the
  best-matching rows by BM25, each row followed by **one extra column holding its score**.
  - The tokenizer lowercases and splits on every non-alphanumeric character. There is no stemming
    and no stopword list, `don't` is two tokens, and a run of CJK is one token, because word
    segmentation for unspaced scripts is not attempted.
  - There is no `ORDER BY` and no `LIMIT` in this SQL, so **the operator supplies its own bound**:
    `SEARCH` returns at most 10 rows unless `TOP k` says otherwise. A query with 400 matches returns
    10 rows by design.
  - A search sees what a `SELECT` in the same transaction would: a deleted row drops out, and an
    entry left behind by an `UPDATE` cannot resurrect one, because the operator re-checks the text of
    the version it resolved.

### System views over the agent layer

Five read-only views, selectable like any table, with `WHERE` and projection:

| View | One row per | Lifetime |
|---|---|---|
| `ferro_branches` | every branch the catalog holds, live or reaped | durable |
| `ferro_runs` | live agent branch, with its agent / run / model | in memory, gone at `MERGE` |
| `ferro_row_authors` | published row, with the run that wrote it | in memory, survives the merge |
| `ferro_quarantine` | branch the verification gate is holding, **with the reason** | membership durable, reason in memory |
| `ferro_run_activity` | live agent branch, with what it has written and read | in memory, gone at `MERGE` |

```sql
SELECT branch_id, state, depth FROM ferro_branches WHERE depth > 0;
SELECT agent_id, run_id, staged_rows, rows_read_exact, blind_writes FROM ferro_run_activity;
SELECT branch_name, reason FROM ferro_quarantine;
```

They are **presentation, not bookkeeping**: every row is materialised on each read from an API the
agent layer already exposed, so there is no second record of what an agent did that could disagree
with the first. That also means they are outside MVCC — a view read inside `BEGIN` sees the runtime
as it is now, not as the transaction's snapshot saw it.

Three consequences worth knowing before you rely on them:

- A view's **lifetime is its source's**. `ferro_runs` answers from the branch's workspace, which
  `MERGE` and `ABANDON` drop, so a merged run leaves it. `ferro_row_authors` is the question that
  keeps answering afterwards. A `NULL` reason in `ferro_quarantine` means the branch is still held
  and the reason did not survive a restart — not that it was held for nothing.
- They are **read-only, and refuse by name**. `INSERT INTO ferro_runs` says so; it does not answer
  `unknown table`.
- `CREATE TABLE ferro_runs` is **refused**, because such a table's rows would be unreachable behind
  the view. A table that already carries one of these names — from a database written before the
  views existed — keeps its rows: the table wins, and the view yields.

Two names are `branch_name` and `model_name` rather than `branch` and `model` because both of the
shorter ones are reserved words here (`AS OF BRANCH`, `MODEL '...'`), and a column named after a
keyword can only be reached through `SELECT *`.

`u64` values that do not fit `i64` (a branch lease of `u64::MAX`, a `row_id` hashed from a
non-integer key) are `DECIMAL`, so they arrive as exact digits rather than as a negative `BIGINT`.

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

Full-text search over that same `posts` table. Note the trailing score column, and that the row
matching both words outranks the two that match one — the two of those tie, and the tie is broken by
primary key so the result is reproducible:

```
ferrodb=> CREATE FULLTEXT INDEX ptitle ON posts (title);
ok
ferrodb=> INSERT INTO posts VALUES (3, 1, 'hello world again');
(1 row affected)
ferrodb=> SEARCH posts (title) FOR 'hello world';
3 | 1 | hello world again | 0.7082246468086428
1 | 1 | hello | 0.561960861054684
2 | 1 | world | 0.561960861054684
(3 rows)
```

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
optimisation: BranchBench (arXiv:2604.17180) measured the "not found here, ask my parent" overlay
pattern at up to **4000x read degradation** as branches deepen, across Neon, DoltgreSQL, Xata and
Tiger Data.

*Corrected 2026-08-28: this said **5400x** in the README, in `src/branch/mod.rs`, in
`src/cow/btree.rs` and in `bench/branch_scaling.txt`. The paper's sentence is "up to **5-4000x**
slower reads as branches deepen" — a range whose top is 4000x. 5400x was that string with the hyphen
dropped, and it was never anyone's measurement. The figure is also **BranchBench's measurement of
other systems**, never ferrodb's: `bench/branch_scaling.txt` says so and reproduces nothing of it.*

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

### A publication decides what the feed may carry

The feed above carries every column of every table. A **publication** — an allowlist, read by both the
database and the independent Go consumer — says what may leave:

```
$ cat pub.txt
publication analytics
# ssn must never leave the database
customers: id, name
exclude audit_log

$ cargo run --example cdc_feed -- demo.db pub.txt > feed.jsonl
$ go run . validate ../feed.jsonl -publication ../pub.txt
OK 6
```

A column the file does not name is **withheld**: absent from every image, and named nowhere — the
`CREATE_TABLE` shape is projected by the same rule, so the consumer is told about exactly the columns
it will receive. A table it does not name at all is **refused**: the feed stops there rather than
stepping over it, and resumes when the policy decides, either by publishing the table or by excluding
it. `cdc_server`, `cdc_feed` and `table_dump` each take a publication file as their last argument;
`cdc-consumer` takes `-publication <file>` on `validate`, `sink`, `follow`, `diff` and `precision`, and
enforces its own copy of the rule rather than trusting the producer's.

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
- **Every change can carry its writer.** The event envelope names the agent run behind it — agent,
  run, model, `model_version` and a SHA-256 of the prompt — or `null` where no agent run produced it,
  and the commits that ship with no writer are **counted and reported** rather than assumed absent.
  A `MERGE` binds its branch's run to the publishing transaction (E79), so an agent's merged rows
  ship attributed over the ordinary SQL path, and the prompt half of that tuple is a real digest
  once the session declared one (E79b) — `integration_run_identity_feed.rs`'s
  `a_prompt_declared_over_sql_reaches_the_feed_as_a_digest` drives the whole chain. A plain SQL
  write still binds nothing and keeps its honest `null`.

Two things the log says that a naive decoder gets wrong, both found by decoding real executor
output rather than hand-built records: a SQL `DELETE` is an MVCC `HeapUpdate` (so mapping record
kinds onto change kinds reports every delete as an update, and a consumer keeps a row forever), and
superseded row versions written to the time-travel heap are internal traffic (emitting them
double-counts every update as an insert).

### The consumer is a separate program, in a separate language

`cdc-consumer/` is a small Go program that shares no code with the database. It validates the feed
against the documented envelope using Go's `encoding/json` — which rejects `NaN` and `Infinity`
outright — and, in `follow` mode, materialises the stream into a local table:

Start a source on that port first — `cdc_server` takes the address to bind, and announces it so a
script can wait for readiness rather than sleeping:

```
$ cargo run --example cdc_server -- follow.db 127.0.0.1:5555 20
LISTENING 127.0.0.1:5555
```

then, from `cdc-consumer/`:

```
$ go run . follow 127.0.0.1:5555 -key id
CURSOR 5162
COLUMNS id,item,qty
TABLE [{"id":1,"item":"item1","qty":10}, ... {"id":20,"item":"item20","qty":2000}]
```

The workload inserts 20 rows and updates every fifth, so `qty` is `id * 10` except at 5, 10, 15 and
20, where it is `id * 100` — which is what makes the materialised table checkable rather than merely
well-formed.

The tests judge the feed by comparing that materialised table against the source, so a feed that is
well-formed, correctly ordered and *wrong* still fails. An encoder validated only by its own
author's idea of the format agrees with itself about any shared misreading.

### Run identity: which agent wrote each row, after a restart and after the wire

Provenance answers *which agent + run + model wrote this row*. Two things used to end that answer
early, and both were silent.

**It did not survive the process.** `MemProvenanceStore` was the only implementation, and every
`AgentRuntime` constructor built one — including `reopen_with_storage`, whose whole job is to attach
to a tree another process wrote. So a database reopened with every row intact answered *nothing*
about any of them. `DurableProvenanceStore` is an append-only file replayed on open: one record per
interned run, one small record per stamped version, a torn tail healed and **reported** rather than
swallowed. It wraps the in-memory store rather than reimplementing it, so the same guards and the
same `footprint_bytes` / `literal_footprint_bytes` density instruments apply unchanged.

> **Not yet wired.** `AgentRuntime`'s three constructors still build a `MemProvenanceStore`, and
> nothing on the SQL path calls `TxnManager::bind_run`. So on every path a shipped binary takes,
> restarting still loses attribution and every feed event carries `"writer":null`. What is done is
> the store, the log record, the wire format and the consumer — each proven by tests — and the
> remaining hop is one line in each of `runtime.rs:276`, `:332`, `:369` plus a `bind_run` call where
> a session's transaction is opened. Stated here rather than left for a reader to infer from a
> feature that appears to be on.

**It stopped at the database boundary.** `ChangeEvent` carried no writer, so a consumer holding a
million rows from a model since found unsound could not ask which of them came from it. Now every
event carries one:

```json
{"op":"INSERT","table":"inventory","writer":{"prov_id":1,"agent":"restock-agent","run":"run-42",
 "model":"claude-opus","model_version":"2026-05","prompt_sha256":"e3b0c442…","started_at":"1700000000000",
 "branch":"b1@g0"},"after":{"id":1,"qty":10}}
```

The prompt travels as a digest and never as text — that is the field's purpose, so a prompt holding
customer data does not become a durable copy of it in every consumer's destination table. It is
hashed once, in `AgentRuntime::begin_session_as`, and the text is dropped there: nothing downstream
of that call — the interned `RunEntity`, the WAL identity record, the provenance file, the open
session, the row the statement returns — holds anything but the 32 bytes. The Go
consumer enforces it with an **allowlist** of the eight keys a `writer` object may carry, checked
against the raw JSON rather than the decoded struct, because `encoding/json` silently drops keys it
has no field for and a leak would decode cleanly.

**The hard part is where the record sits in the log.** The feed cursor may never advance past the
earliest *staged* record of a still-open transaction. An identity record written when the session
begins stages nothing, so it sits *below* that clamp: read once, stepped over, never read again —
and when the transaction finally commits, every one of its rows ships attributed to nobody while the
pump reports a clean run. It is therefore written in the append **immediately before the `Commit`
record**, where a clamp cannot separate the two.
`tests/integration_run_identity_feed.rs` streams the same workload under both placements; the early
one loses the attribution and the count catches it.

Retention needs its own answer because `checkpoint()` discards the WAL **whole** rather than by
prefix, exactly as it does for DDL. `TxnManager` re-declares its retained run table at the head of
the new log, so a reader starting at the new base can still name the database's writers.

#### Retract by model version

Every destination row the sink lands carries its writer, which makes the operational question
answerable at the destination — with no source database and no untruncated log:

```
$ cdc-consumer sink feed.jsonl -db dest.sqlite -key id
$ cdc-consumer retract dest.sqlite -table inventory -model-version 2026-07
RETRACTED 3 OF 6 table=inventory model_version=2026-07 mode=quarantine
$ cdc-consumer scan dest.sqlite -table inventory      # ground truth, from a full scan
ROW id=1 prov_id=1 agent=restock-agent model_version=2026-05 retracted=0 deleted=0
ROW id=2 prov_id=2 agent=restock-agent model_version=2026-07 retracted=1 deleted=0
…
```

`-mode delete` tombstones as well as marks, and `-engine duckdb` targets the analytical destination
— both sinks land the same attribution columns, pinned by
`TestBothSinksLandTheSameWriterColumnNames` because `retract` addresses them by name. A retraction
naming a version nothing wrote is **refused**, not reported as a clean run of zero rows — the likely
cause is a typo, and the error names the versions that are present. Rows with no writer at all are
never swept up, whatever string is passed, and a destination landed before attribution existed is
upgraded in place rather than refused. `tests/integration_cdc_retract_by_model.rs` runs the whole
pipeline and checks the 100%-of-one / 0%-of-any-other property from `scan`, which did not do the
retracting.

### Wide values ship as strings, on purpose

JSON has one number type and no stated precision, and the overwhelmingly common consumer
behaviour is to parse every JSON number into an **IEEE 754 double** — that is what JavaScript's
`JSON.parse` does, and what Go's `encoding/json` does into `interface{}`. A double carries a 53-bit
significand, so `9223372036854775807` comes back as `9223372036854775808`, `9007199254740993` comes
back as `9007199254740992`, and a decimal past 17 significant digits comes back rounded. **No error
is raised** for any of it: the parse succeeds and the number is simply wrong.

So `BIGINT`, `DECIMAL` and `TIMESTAMP` are emitted as JSON **strings**, which no parser coerces
(envelope fields elided here — a real line also carries `txn`, `lsn`, `commit_lsn`,
`commit_end_lsn`, `writer` and `before`):

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

Produce a feed first, then land it. Note the `../`: the feed is written at the repository root and
`go run .` runs inside `cdc-consumer/`, so the path a reader needs is not the bare `feed.jsonl` this
section used to show — that command fails with `open feed.jsonl: no such file or directory`.

```
$ cargo run --example cdc_feed > feed.jsonl        # at the repository root
$ cd cdc-consumer
$ go run . sink ../feed.jsonl -db out.sqlite -key id                  # default engine
applied 6, skipped 0 re-delivered
APPLIED 6 SKIPPED 0 CURSOR <byte offset> TABLE inventory
$ go run . sink ../feed.jsonl -db out.duckdb -key id -engine duckdb
```

Six events land three rows, because the workload updates rows it already inserted — a sink that
appended instead of upserting would leave six.

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

### Proving the feed reproduces the source, without a hardcoded expectation

Everything above checks the destination against rows written into a test as a literal. That verifies
the pipeline against what somebody typed: change the workload and the literal is what breaks, and a
literal cannot notice a difference nobody anticipated.

So the source states its own case. `table_dump` asks the source database `SELECT * FROM <table>` — the
same question a user would ask, MVCC visibility and all — and prints the answer as JSON.
`cdc-consumer diff` folds the feed with the same `Table.apply` the sink uses, then compares the two
per row and per column.

Run this after the sink commands above, from `cdc-consumer/`:

```
$ cargo run --example table_dump cdc_demo.db inventory > source.json
$ go run . diff ../feed.jsonl ../source.json -key id
MATCH 2 row(s) from 6 event(s)
```

Under a publication both sides take the same one — `table_dump <db> <table> [publication]` and
`diff ... -publication <file>` — because a dump is egress too, and a projected feed compared against an
unprojected source would report the withheld column as a data mismatch.

Two rows rather than the three the sink lands, because the sink keeps a **tombstone** for the deleted
row and the source simply does not have it — the diff compares live state to live state.

The comparison is semantic, not byte for byte. Both sides decode with `UseNumber()`, so an int64 past
2^53 keeps its exact digits; comparing the documents as bytes would fail on key order or on any
formatting difference between a Rust writer and a Go writer, neither of which is a data problem, and a
check that cries wolf gets switched off. The two renderers are shared rather than parallel —
`write_table_json` uses the same `value_into` as the feed writer — so a `DECIMAL` or `TIMESTAMP` is a
string on both sides without either tool having to know that.

What it refuses, because a diff that cannot fail is decoration: an empty feed, and both sides empty.
Two empty tables agree trivially, which is exactly the state a pipeline that delivered nothing leaves
behind. `tests/integration_cdc_diff.rs` forces all three real failures — a wrong value, a lost
`DELETE` leaving a row the source dropped, and a lost `INSERT` — and requires each to be named.

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

### Column-level schema evolution

The feed carries five schema ops, not two: `CREATE_TABLE`, `DROP_TABLE`, and — through
`ALTER TABLE ... ADD COLUMN` / `RENAME COLUMN` / `ALTER COLUMN ... TYPE` — `ADD_COLUMN`,
`RENAME_COLUMN` and `ALTER_COLUMN_TYPE`. Each arrives **in band and in log order**, at the position
the DDL actually occupied, so the events before it describe the old shape and the ones after it
describe the new one.

Every column-level event carries **both halves**: `after.columns` is the table's full shape
afterwards, which the sinks reconcile their destination against positionally; `after.alter` says
which change produced that shape. Both are needed, because a rename and a drop-plus-add leave
identical column lists and only one of them keeps the column's data.

Three things are worth knowing about the shape of the feature rather than the wire format:

- **The two whole-table ops are declarations; the three column-level ones are news.** A
  `CREATE_TABLE` is re-emitted at every checkpoint and a consumer may apply it any number of times.
  An `ALTER` is delivered exactly once, and a consumer that applied one twice would rename a column
  that no longer has the old name. An alter updates the source's *retained declaration* instead of
  being retained itself, which is also how the new shape survives a log truncation and a restart.
- **A column is added at the end, and there is no `DROP COLUMN`.** A column's ordinal is its
  identity below the parser — tuple bytes are positional, and every recorded effect, guard and
  merge policy holds an ordinal — so removing one, or inserting one mid-table, would silently
  re-point all of them at a different column.
- **Retypes are an allowlist of conversions that are total for every stored value**: `INTEGER` to
  `BIGINT` or `DECIMAL`, `BIGINT` to `DECIMAL`, and `VARCHAR(n)` to `VARCHAR(m)` where `m >= n`.
  Anything else would have to decide what to do with a value that does not fit, and every answer to
  that is data loss. Retyping the primary key is refused.

Two agents can evolve one schema concurrently. A branch's `ALTER` is **pending** — invisible to
main and to siblings until `MERGE`, and gone if the branch is abandoned — and at merge it is
three-way merged against the shape the target has *then*. Two branches adding different columns
compose (`Commuting`); two retyping one column to different types `Conflict`, and the agent is
handed back the violated predicate itself, e.g. `typeof(inventory.qty) = INTEGER`. An edit the
target already satisfies identically is absorbed rather than refused.

**Limits:** there is no wire framing beyond newline delimiting, and the feed is JSON rather than a
compact binary format. The sinks replace whole rows rather than merging, which is correct only
because this feed always emits full before/after images. An `ALTER` rewrites the table in place and
is refused while any transaction is open; like every other DDL here it is not crash-atomic, because
the catalog is written outside the WAL and recovery does not replay it. An alter also truncates the
table's MVCC version chains, which is unobservable only because it requires that quiesce.

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
- [x] SQL surface: `BEGIN AGENT SESSION ... [RUN] [MODEL] [PROMPT]`, `AS OF BRANCH`, `DIFF`,
      `MERGE`, `REVERT ... CASCADE`
- [x] Verification gate tiers, ordered by cost ÷ rejection-probability, and the
      `write-set \ read-set` blind-write metric
- [x] Quarantine: a declined branch stays unmerged but still queryable
- [x] Escrow at fork, so a bounded-counter overdraw fails at write time
- [x] Depth guard + `COLLAPSE` at ancestry depth 8
- [x] System views over the agent layer (`ferro_branches`, `ferro_runs`, `ferro_row_authors`,
      `ferro_quarantine`, `ferro_run_activity`), and structured agent results as typed columns on
      the wire rather than one `Debug` string
- [ ] SQL statements writing directly to CoW pages (the largest remaining gap, above)

## Why I built it

I wanted to know how a database actually works and the best way to do that is to build a database from scratch. For example, how do bytes on disk become rows in a query result, how a database optimizes queries, etc.