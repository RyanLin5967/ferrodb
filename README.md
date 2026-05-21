# ferrodb

A relational database built from scratch in Rust with zero external storage dependencies

### Status

Under active development.

## Planned Features

- Page-based disk storage engine with heap file organization
- Buffer pool manager with ARC eviction
- B+ tree indexing
- Recursive descent SQL parser
- Cost-based query optimizer
- MVCC concurrency controll with snapshot isolation
- Write-ahead logging with crash recovery
- Postgres wire protocol compatibility

## Current progress

- [x] Disk Manager (page-level IO, bitmap-based page allocation)
- [ ] Page layout and tuple serialization
- [ ] Buffer pool manager
- [ ] B+ tree indexing
- [ ] SQL parser
- [ ] Query execution engine
- [ ] Cost-based query optimizer
- [ ] Write-ahead logging
- [ ] MVCC 
- [ ] Postgres wire protocol