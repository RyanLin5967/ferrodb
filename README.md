# ferrodb

A relational database built from scratch in Rust.


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

## Current progress

- [x] Disk Manager (page-level IO, bitmap-based page allocation)
- [x] Page layout and tuple serialization
- [x] Buffer pool manager
- [x] B+ tree indexing
- [x] SQL parser
- [x] Query execution engine
- [x] Cost-based query optimizer
- [x] Write-ahead logging with crash recovery
- [ ] MVCC 
- [ ] Postgres wire protocol
- [ ] Distributed replication (Raft)

## Why I built it

I wanted to know how a database actually works and the best way to do that is to build a database from scratch. For example, how do bytes on disk become rows in a query result, how a database optimizes queries, etc.