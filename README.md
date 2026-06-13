# ferrodb

A relational database built from scratch in Rust.


## How to run

### From release

Prebuilt binaries (in zip files) for Linux, macOS, and Windows are in the releases page. Download and run the one for your OS. 

### From source

Requires Rust 1.85 or newer (this project uses edition 2024).

```
git clone https://github.com/RyanLin5967/ferrodb.git
cd ferrodb
cargo run
```

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

## Current progress

- [x] Disk Manager (page-level IO, bitmap-based page allocation)
- [x] Page layout and tuple serialization
- [x] Buffer pool manager
- [x] B+ tree indexing
- [x] SQL parser
- [x] Query execution engine
- [ ] Cost-based query optimizer
- [ ] Write-ahead logging with crash recovery
- [ ] MVCC 
- [ ] Postgres wire protocol
- [ ] Distributed replication (Raft)

## Why I built it

I wanted to know how a database actually works and the best way to do that is to build a database from scratch. For example, how do bytes on disk become rows in a query result, how a database optimizes queries, etc.