# ferrodb

A relational database built from scratch in Rust.

It is currently growing a second identity: **ferrobranch**, an *agent-isolation* database in which
the unit of isolation is an agent task rather than a transaction. See
[ferrobranch — agent isolation](#ferrobranch--agent-isolation) below, including an honest account
of what is and is not demonstrated yet.


## How to run

### From release

Prebuilt binaries (in zip files) for Linux, macOS, and Windows are in the releases page. Download the one for your OS, unzip, then run the executable. 

### From source

Requires Rust 1.85 or newer (this project uses edition 2024).

```
git clone https://github.com/RyanLin5967/ferrodb.git
cd ferrodb
cargo run
```
You can add an argument if you want a custom name. For example, `cargo run -- customname.db` will persist tables in `customname.db`.
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

- **Types:** INTEGER, FLOAT, BOOLEAN, VARCHAR(n)
- **Literals:** integers, floats, single quoted strings, TRUE, FALSE, NULL
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
*run-level* cardinality, so storing it per row version is ~3.4x density loss for nothing. It is
interned: a page-local dictionary slot points at a reified `RunEntity`. Read-sets are retained too,
in a form chosen by **access shape** rather than size — point lookups keep exact version ids, scans
keep a predicate summary. Retaining reads is what makes causal rollback possible: reverting write A
can find the write B that *read* A. It halts and shows the tree by default; cascade is explicit.

**3. Typed Effect Log, merge, and verification gate.** Writes are logged as typed operations
(`Assign`, `Add`, `Max`, `Min`, `SetInsert`, `SetRemove`) alongside the **guards** that made them
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

**Not yet demonstrated: any of it.** This section describes a design under construction, not a
working feature set. Concretely, on branch `agent-isolation` today:

- `cargo test` — **190 passed, 0 failed** (the SQL database below is real and works)
- `src/{branch,cow,tel,provenance}/` — ~3,094 lines of foundation with 41 tests: branch records and
  ids, CoW page headers, the typed op/guard/merge algebra, read-set and revert structures. No
  `todo!()` or `unimplemented!()` in them.
- The storage integration that would make the above *do* anything end-to-end — CoW B+tree and store,
  branch arenas, the lease reaper, provenance capture hooks, SQL surface syntax — is being built on
  unmerged `impl-*` branches and is **not in this tree yet**.
- None of the ten exit criteria has been demonstrated end-to-end. The thesis criterion — abandoned
  branches reaped with no client cooperation, page count returning to baseline — has **not** been
  observed firing.

Progress is tracked criterion by criterion, and any criterion not genuinely demonstrated is reported
as NOT MET with its reason. A fabricated pass would be worse than an admitted gap.

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
- [ ] Postgres wire protocol
- [ ] Distributed replication (Raft)

### ferrobranch (agent isolation)

Foundation types are in the tree; nothing below is demonstrated end-to-end yet.

- [x] Branch records, ids with generation counters, fork epochs
- [x] CoW page header with `birth_epoch`
- [x] Typed Effect Log: ops, guards, three-way merge algebra with four outcomes
- [x] Read-set representations and revert/dependency structures
- [ ] CoW B+tree and store wired under the executor
- [ ] Per-branch arenas and write buffers
- [ ] Non-cooperative lease reaper (**the thesis**)
- [ ] Provenance capture hooks in the write path
- [ ] SQL surface: `BEGIN AGENT SESSION`, `AS OF BRANCH`, `DIFF`, `MERGE`, `REVERT ... CASCADE`
- [ ] Verification gate tiers and the `write-set \ read-set` blind-write metric

## Why I built it

I wanted to know how a database actually works and the best way to do that is to build a database from scratch. For example, how do bytes on disk become rows in a query result, how a database optimizes queries, etc.