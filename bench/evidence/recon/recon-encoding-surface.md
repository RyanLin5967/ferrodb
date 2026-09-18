# `TxnFrame` encoding checklist — complete transitive type closure

Repo has **no serde and no external deps** (`/Users/idide/wt/ferrodb-F10-durable-tel/Cargo.toml`, dependencies section empty). Every codec in this repo is hand-rolled, big-endian, tag-per-variant.

---

## A. Root struct

**`TxnFrame`** — `/Users/idide/wt/ferrodb-F10-durable-tel/src/tel/frame.rs:20-33`, `#[derive(Debug, Clone, PartialEq)]`
| field | type | width/shape |
|---|---|---|
| `txn_id` | `TxnId` | newtype `u64` |
| `branch` | `BranchId` | `{id: u64, generation: u32}` = 12 bytes |
| `base` | `CommitHash` | `[u8; 32]` |
| `seq` | `u64` | 8 |
| `schema_ver` | `SchemaVer` = `pub type SchemaVer = u32` (frame.rs:12) | 4 |
| `ops` | `Vec<Op>` | length-prefixed list |
| `guards` | `Vec<Guard>` | length-prefixed list |
| `claims` | `Vec<EscrowClaim>` | length-prefixed list |

No `Copy`. No existing encoder: `grep -rn "TxnFrame" src/ tests/ | grep -i "serial\|encode\|decode\|bytes"` returns **zero hits**. `Command` (`src/consensus/mod.rs:88-170`) has **no** `TxnFrame` variant — TEL frames are not replicated today. `src/tel/log.rs` is `MemEffectLog`, in-memory only (`frames: Mutex<Vec<TxnFrame>>`, line 37).

`EffectLog` trait — `src/tel/mod.rs:43-52`: `append(&self, frame: &TxnFrame)`, `frames_for(&self, branch: BranchId, from_seq: u64) -> Vec<TxnFrame>`.

---

## B. Identifier newtypes — `src/tel/ids.rs`

- `RowId(pub u64)` — ids.rs:18. `const SCHEMA: RowId = RowId(u64::MAX)` (ids.rs:33) is a **reserved sentinel** meaning "the table's shape, not a row"; `is_schema()` ids.rs:36. A decoder must round-trip `u64::MAX` unchanged.
- `TableId(pub u32)` — ids.rs:52
- `ColId(pub u32)` — ids.rs:63
- `TxnId(pub u64)` — ids.rs:83
- `Dot` — ids.rs:98-101, `#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]` (**no `Default`**)
  - `branch: BranchId` (12 bytes)
  - `seq: u64`
  - → 20 bytes fixed

---

## C. `BranchId` — `src/branch/types.rs:23-26`

```rust
pub struct BranchId { pub id: u64, pub generation: u32 }   // Copy, Ord, Hash
```
`const TRUNK = BranchId { id: 0, generation: 0 }` (types.rs:29).

**Existing byte encodings (two, both `id` u64-BE then `generation` u32-BE, 12 bytes):**
1. `/Users/idide/wt/ferrodb-F10-durable-tel/src/provenance/durable.rs:493-494` (write):
   ```rust
   body.extend_from_slice(&run.parent_branch.id.to_be_bytes());
   body.extend_from_slice(&run.parent_branch.generation.to_be_bytes());
   ```
   decode at `src/provenance/durable.rs:335-345`: `take_u64` then `take_u32` then `BranchId::new(branch_id, generation)`.
2. `src/branch/record.rs:195-196` (branch record), and the `Option<BranchId>` form at `src/branch/record.rs:198-208` — **fixed-width optional**: tag `1` + id + gen when present, tag `0` + `0u64` + `0u32` when absent (so the field is always 13 bytes). Decode `src/branch/record.rs:271-276`.

---

## D. `CommitHash` — `src/branch/types.rs:108-120`

```rust
pub struct CommitHash(pub [u8; 32]);          // Copy, Eq, Ord, Hash
pub const ZERO: CommitHash = CommitHash([0u8; 32]);   // types.rs:111
pub fn to_hex(&self) -> String                        // types.rs:113, 64 chars
impl Display                                          // types.rs:122-126, prints FIRST 16 HEX CHARS only
```
**Exact width: 32 bytes.**

**Existing byte encode/decode for `CommitHash`: NOT FOUND.** Full census of `CommitHash` mentions in `src/` (grep `-rn CommitHash --include='*.rs' src/`): frame.rs:5/25/36; branch/types.rs:108/110/111/122; branch/mod.rs:33 (re-export); agent_sql/runtime.rs:49/783; and the rest are `CommitHash::ZERO` in tests (tel/log.rs:132/141, tel/engine.rs:1152/1199/1989/1991, tel/capture.rs:418/455, agent_sql/merge_engine.rs:466/536/583/588). **Every non-test construction in the repo is `CommitHash::ZERO`** — runtime.rs:783 builds every frame with `CommitHash::ZERO`. Nothing writes or reads its bytes.

Nearest precedent for a 32-byte field: `prompt_hash: [u8; 32]` in provenance, written raw (`durable.rs:491` `body.extend_from_slice(&run.prompt_hash);`) and read with `take_array::<32>` (`durable.rs:334`).

---

## E. `Op` and `OpKind` — `src/tel/op.rs`

**`Op`** — op.rs:147-160, `#[derive(Debug, Clone, PartialEq)]`
| field | type |
|---|---|
| `tbl` | `TableId` (u32) |
| `row` | `RowId` (u64) |
| `col` | `Option<ColId>` — `None` for whole-row ops (`RowCreate`/`RowDelete`) |
| `kind` | `OpKind` |
| `witness` | `Option<Value>` — the pre-op observed value |

Two `Option`s per `Op`, one of them an `Option<Value>` (variable-length).

**`OpKind`** — op.rs:87-106, 8 variants:
| tag | variant | payload |
|---|---|---|
| — | `RowCreate(Vec<Value>)` | full initial row image, variable count |
| — | `RowDelete` | none |
| — | `Assign(Value)` | one Value |
| — | `Add(Delta)` | one Delta |
| — | `Max(Value)` | one Value |
| — | `Min(Value)` | one Value |
| — | `SetInsert { elem: Value, dot: Dot }` | Value + 20 bytes |
| — | `SetRemove { elem: Value, dots: Vec<Dot> }` | Value + variable Dot list |

No tag numbers assigned anywhere yet. `OpKind::name()` (op.rs:131-142) is the only stringly-typed discriminator that exists.

**`Delta`** — op.rs:18-21, `#[derive(Debug, Clone, Copy, PartialEq)]`
- `Int(i64)`
- `Float(f64)`

No existing byte codec for `Delta` (grep for `Delta::Int|Delta::Float` intersected with serialize/encode/`to_be_bytes` → zero hits).

**`EscrowClaim`** — op.rs:192-202, `#[derive(Debug, Clone, PartialEq)]`
| field | type |
|---|---|
| `tbl` | `TableId` (u32) |
| `row` | `RowId` (u64) |
| `col` | `ColId` (u32) — **not** an `Option`, unlike `Op::col` |
| `amount` | `Delta` |
| `floor` | `Option<Value>` |
| `ceiling` | `Option<Value>` |

---

## F. `Guard` / `GuardExpr` — `src/tel/guard.rs`

**`Guard`** — guard.rs:318-326, `#[derive(Debug, Clone, PartialEq)]`
| field | type | note |
|---|---|---|
| `expr` | `GuardExpr` | recursive |
| `expected` | `Value` | normally `Value::Boolean(true)` (guard.rs:335) |
| `source_text` | `Option<String>` | verbatim SQL fragment; `None` when synthesised. Arbitrary length — a WHERE clause. |

**`GuardExpr`** — guard.rs:94-104, `#[derive(Debug, Clone, PartialEq)]`, **8 variants, recursive**:
| variant | payload |
|---|---|
| `Literal(Value)` | one Value |
| `Col { tbl: TableId, row: RowId, col: ColId }` | 4 + 8 + 4 = 16 bytes fixed |
| `Compare { left: Box<GuardExpr>, op: CmpOp, right: Box<GuardExpr> }` | 2 children |
| `Arith { left: Box<GuardExpr>, op: ArithOp, right: Box<GuardExpr> }` | 2 children |
| `And(Vec<GuardExpr>)` | n children |
| `Or(Vec<GuardExpr>)` | n children |
| `Not(Box<GuardExpr>)` | 1 child |
| `IsNull(Box<GuardExpr>)` | 1 child |

**`CmpOp`** — guard.rs:30-37, `Copy, Eq, Hash`, 6 nullary variants: `Eq, Ne, Lt, Le, Gt, Ge`.
**`ArithOp`** — guard.rs:69-74, `Copy, Eq, Hash`, 4 nullary variants: `Add, Sub, Mul, Div`.

### F.1 Maximum nesting the code creates

**Unbounded — it is a direct structural translation of an arbitrary SQL `WHERE` clause.** Two builders, both recursing once per operator node with no depth counter and no cap:

- `/Users/idide/wt/ferrodb-F10-durable-tel/src/tel/capture.rs:87-134` `to_guard_expr(tbl, row, map, e: &BoundExpr)` — recurses on `BoundExpr::BinaryOp` (capture.rs:106-108: `let l = to_guard_expr(...)?; let r = to_guard_expr(...)?;`) and `BoundExpr::UnaryOp` (capture.rs:121).
- `/Users/idide/wt/ferrodb-F10-durable-tel/src/agent_sql/runtime.rs:4441-4490` — the same shape over raw parser expressions.

Both emit `And`/`Or` as **strictly binary** `vec![l, r]` (capture.rs:109-110; runtime.rs:4482-4483), so `a AND b AND c AND d` produces a nested chain of depth ≈ number of terms, not a flat 4-element `And`. `GuardExpr::Not(Box::new(r))` at capture.rs:125 and runtime.rs:4458; unary minus expands to an extra `Arith` node wrapping a `Literal(Integer(0))` (capture.rs:126-131, runtime.rs:4459-4463) — one *extra* level per `-x`.

**No nesting cap in the parser either**: `grep -rni "nest|recursion|too deep|MAX_DEPTH" src/parser* src/binder*` returns only unrelated hits (`src/parser/parser.rs:211` is about `DEFAULT_TOP_K`; the binder hits are about nested agent sessions). So a `WHERE` of N stacked `NOT`s yields a `GuardExpr` of depth N.

**`GuardExpr::IsNull` is never constructed by any producer** — the only three mentions in the whole repo are the three match arms in guard.rs itself (guard.rs:140, 194, 272). An encoder must still handle it (it is a public variant).

Deepest hand-built literal instance in the repo: 4 levels — `src/tel/guard.rs:480` `Guard::holds(GuardExpr::Not(Box::new(or)))` over `Or([Compare(Col, Literal), Literal])`.

### F.2 Recursion-depth caps in existing decoders

**NOT FOUND — there are none, because no existing decoder in this repo decodes a recursive type.** Evidence:

- `grep -rni "depth|recursion|MAX_NEST" src/consensus/ src/wal/ src/replication/` → every hit is `Transport` **queue** depth (`src/consensus/transport.rs:981` `pub queue_depth: usize`, default 1024 at transport.rs:1016, enforced at transport.rs:1083, zero refused at transport.rs:1200-1203) plus its tests. Nothing about recursion.
- Repo-wide `grep -rni depth src/` adds only branch **ancestry** depth: `MAX_BRANCH_DEPTH: u8 = 8` (`src/branch/types.rs` — `pub const MAX_BRANCH_DEPTH: u8 = 8;`), `BranchError::DepthExceeded` (types.rs:280), `BranchRecord::depth: u8` (`src/branch/record.rs:42`), reaper sorting (`src/branch/reaper.rs:113`, `:267`).
- `read_command` / `decode_command` (`src/consensus/log.rs:1085-1230`) is entirely flat — every variant is fixed-width fields or a length-prefixed list of fixed-width items. No self-recursive call.

**The size caps that do exist** (the nearest thing to a bound, all byte-count not depth):
- `src/replication/mod.rs:101` `pub const MAX_FRAME_BYTES: usize = 8 * 1024 * 1024;`, enforced `src/replication/mod.rs:159-163`.
- `src/consensus/log.rs:105` `pub const MAX_ENTRY_BYTES: usize = crate::replication::MAX_FRAME_BYTES - 4096;`, enforced at `src/consensus/log.rs:1013-1015`; `MAX_FRAME` (log.rs:109) used at log.rs:961.

**Allocation-from-untrusted-count precedent worth copying** — `src/consensus/log.rs:1253-1281` `read_ids`, with a documented mutation-survival story: the claimed count never reaches `Vec::with_capacity`; the byte slice is proven present first (`n.checked_mul(4)`, then `bytes.get(*at..at+want)`), then capacity is derived from a slice that exists. Also `src/consensus/log.rs:1189` `Vec::with_capacity(n.min(1024))` for columns.

---

## G. `Value` — `/Users/idide/wt/ferrodb-F10-durable-tel/src/catalog/column.rs`

`#[derive(Debug, Clone)]` only (column.rs, at the `pub enum Value` declaration). `PartialEq`/`Eq`/`Ord`/`PartialOrd` are **hand-written** (column.rs:281-347 region: `impl Ord for Value`, `impl PartialOrd`, `impl PartialEq { fn eq(&self, other) { self.cmp(other) == Equal } }`, `impl Eq`). **No `Hash` derive**; `src/execution/hash_join.rs:44-56` hand-rolls a hash that must agree with that `Ord`. Not `Copy`.

**8 variants:**

| # | variant | exact Rust type | fixed/variable | notes |
|---|---|---|---|---|
| 1 | `Integer(i32)` | `i32` | fixed 4 | |
| 2 | `Float(f64)` | `f64` | fixed 8 | see G.2 — NaN/±Inf/`-0.0` are all distinct values under `total_cmp`; `-0.0 < 0.0` strictly (column.rs `impl Ord`, and `cmp_f64_decimal` comment at column.rs:200-206) |
| 3 | `Varchar(String)` | `String` (UTF-8) | **variable** | |
| 4 | `Boolean(bool)` | `bool` | fixed 1 | |
| 5 | `BigInt(i64)` | `i64` | fixed 8 | doc: extremes ±9.2e18 exceed 2^53, "which is the entire reason the change feed ships this type as a JSON string" |
| 6 | `Decimal(String)` | `String` (digit text) | **variable, unbounded digits** | **exact-bytes invariant, see G.1** |
| 7 | `Timestamp(i64)` | `i64` (unix epoch **millis**, signed for pre-1970) | fixed 8 | |
| 8 | `Null` | — | fixed 0 | |

`type_rank` (column.rs, after `impl Ord`): `Null=0, Boolean=1, Integer=2, BigInt=3, Float=4, Decimal=5, Timestamp=6, Varchar=7` — ranks 2..5 are a contiguous "numeric band" that compares by value; the layout is load-bearing for transitivity and is documented as such.

### G.1 The documented exact-bytes invariant on `Decimal`

From the `Value::Decimal` doc comment in `src/catalog/column.rs`, verbatim:

> The stored text is exactly what was written, scale included: `1.50` stays `1.50` and does not become `1.5`, because trailing zeros are significant to a consumer reading a price. Comparison is numeric, so `1.50` and `1.5` are still *equal*; only the bytes differ.

Consequences an encoder must respect:
- **A codec must be byte-preserving on `Decimal`, and cannot be tested by equality alone** — `Value::Decimal("1.50") == Value::Decimal("1.5")` is `true` (`decimal_cmp`, column.rs; pinned by the test `decimal_orders_numerically_not_lexicographically`, column.rs: `"trailing zeros change the bytes, not the number"`). A round-trip assertion written as `assert_eq!(before, after)` will pass on a codec that normalised `1.50` → `1.5`. Existing tests that get this right compare the *string*: `src/agent_sql/paged_rows.rs:340-341` `matches!(&back[10], Value::Decimal(d) if d == "1.50")`; `src/branch/record.rs:537-539` special-cases `(Value::Decimal(x), Value::Decimal(y)) => x == y` for exactly this reason.
- **Digit count is unbounded** — the doc explicitly rejects a scaled `i128` because "`i128` caps the significant digits at 38; text has no cap, so a 60-digit ledger amount survives", and the test `decimal_beyond_i128_still_compares_exactly` (column.rs) uses 60-digit literals. A `u16` length prefix caps this at 65535 bytes.
- Normalisation happens **only at parse time**, once, in `parse_decimal` (column.rs, `pub fn parse_decimal`): strips a leading `+`, fills `.5`→`0.5` and `5.`→`5.0`, and "Nothing else is changed — in particular trailing zeros are kept, because their presence is information."
- `-0.0` decimal text and `0` compare equal (`decimal_cmp`, and the test asserting `Value::Decimal("-0.0") == Value::Decimal("0")`), but the bytes differ.

`Varchar(String)` is likewise variable and its bytes must survive verbatim (it is compared with plain `a.cmp(b)`).

### G.2 Float encoding is bit-exact-required

`impl Ord for Value` uses `f64::total_cmp` throughout, and there are tests pinning NaN placement (`bigint_versus_non_finite_floats_matches_total_cmp_placement`, column.rs) and `-0.0` distinctness. So an encoder must preserve the **bit pattern**, including NaN sign and payload: a normalising or `f64::to_string`-based encoding changes the value.

---

## H. Existing self-describing (tag-per-variant, schema-free) byte encoder for `Value`

**FOUND — exactly one**, and it is the one the repo already treats as canonical.

`impl BTreeSerialize for Value` — `/Users/idide/wt/ferrodb-F10-durable-tel/src/storage/index_page.rs:159-239`
(trait at `src/storage/index_page.rs:155-158`: `fn serialize(&self, buf: &mut Vec<u8>)`, `fn deserialize(bytes: &[u8]) -> Result<(Self, usize), FerroError>` returning `(value, bytes_consumed)`.)

| tag | variant | layout | encode line | decode line |
|---|---|---|---|---|
| `0` | `Integer(i32)` | tag + `i32` BE = 5 B | index_page.rs:161-164 | :202-205 |
| `1` | `Varchar(String)` | tag + `u16` BE len + UTF-8 bytes = 3+n | :165-169 | :206-210 |
| `2` | `Float(f64)` | tag + `f64::to_be_bytes` = 9 B | :170-173 | :211-214 |
| `3` | `Boolean(bool)` | tag + `u8` = 2 B | :174-177 | :215-218 |
| `4` | `Null` | tag = 1 B | :178-180 | :219-221 |
| `5` | `BigInt(i64)` | tag + `i64` BE = 9 B | :183-186 | :222-225 |
| `6` | `Decimal(String)` | tag + `u16` BE len + digit bytes = 3+n | :187-191 | :226-230 |
| `7` | `Timestamp(i64)` | tag + `i64` BE = 9 B | :192-195 | :231-234 |

Documented tag-stability rule at index_page.rs:181-182: *"Tags 0..4 are fixed by every index page already on disk; the wide types take the next free numbers so an existing tree keeps deserialising."* Unknown tag → `FerroError::Io("invalid tag value")` (index_page.rs:237).

**Two known defects in this encoder that a new `TxnFrame` codec inherits if it reuses it:**
1. **`Value::deserialize` indexes unchecked and panics on a short slice.** Stated at `src/agent_sql/paged_rows.rs:76-80`: *"`Value::deserialize` indexes its input unchecked and **panics** on a short slice."* The mitigation is a separate pre-validator, `fn value_span(bytes) -> Result<usize, FerroError>` at `src/agent_sql/paged_rows.rs:81-120`, which duplicates the tag→span table and must be kept in step by hand. paged_rows.rs:93-106 records the incident: when the wide types were added to `index_page.rs`, `value_span` was not updated, so `encode_row` succeeded and every later `decode_row` failed — BIGINT/DECIMAL/TIMESTAMP cells were write-only on page-backed branches.
2. **`s.len() as u16` silently truncates** any `Varchar`/`Decimal` over 65535 bytes (index_page.rs:167 and :189 — a raw `as` cast, no `try_from`). `Decimal` has no documented digit cap, so this is reachable.

**Other `Value` byte encoders — all schema-directed, NOT self-describing:**
- `Tuple::serialize(values: &[Value], schema: &Schema, begin_ts: u64)` — `/Users/idide/wt/ferrodb-F10-durable-tel/src/storage/tuple.rs:22` (variants at tuple.rs:60-112), inverse `Tuple::deserialize(&self, schema: &Schema)` at tuple.rs:143. **Requires a `Schema`**; nullability lives in a null bitmap, not a tag. Unusable for `TxnFrame`, whose `Value`s are not schema-positional (`Guard::expected`, `Op::witness`, `EscrowClaim::floor/ceiling`).
- `src/pgwire/types.rs:123-131` — per-OID wire encoding, driven by the declared column type; `Value::Float(f) => f.to_be_bytes()`, `Value::Integer(i) => (*i as f64).to_be_bytes()`.
- Per-module survey requested: **`src/replication/` — NOT FOUND** (only `Value`→JSON *text*: `value_into` at `src/replication/jsonl.rs:163-186`; the one `Tuple::serialize` call at `src/replication/stream.rs:611` is a test fixture). **`src/consensus/` — NOT FOUND** (`grep -rn Value src/consensus/ | grep 'serialize|encode|to_be_bytes'` → zero hits; `Command` carries `DataType`, never a `Value`). **`src/branch/` — NOT FOUND** for `Value`; `src/branch/record.rs:236-245` encodes an envelope `floor` as `Option<i64>` (tag + `i64`), not a `Value`. **`src/storage/` — the two above.**

---

## I. `f64`-to-bytes precedent in this repo

**`to_be_bytes()` on the `f64` directly, everywhere. `to_bits()` is used only for hashing, never for a durable/wire format.**

Writes:
- `/Users/idide/wt/ferrodb-F10-durable-tel/src/storage/index_page.rs:173` — `buf.extend_from_slice(&f.to_be_bytes());` (the canonical `Value::Float`, tag 2)
- `/Users/idide/wt/ferrodb-F10-durable-tel/src/storage/tuple.rs:69` — `bytes.extend_from_slice(&f.to_be_bytes());`
- `/Users/idide/wt/ferrodb-F10-durable-tel/src/pgwire/types.rs:130` — `Value::Float(f) => f.to_be_bytes().to_vec()` (FLOAT8 binary wire)

Reads:
- `src/storage/index_page.rs:213` — `f64::from_be_bytes(bytes[1..9].try_into().unwrap())`
- `src/storage/tuple.rs:175` — `f64::from_be_bytes(float_bytes.try_into().unwrap())`
- `src/pgwire/types.rs:221` — `f64::from_be_bytes([b[0]..b[7]])`

`to_bits()` sites, all non-format:
- `src/agent_sql/runtime.rs:127` — `RowId(fnv64(&f.to_bits().to_be_bytes()))`, row-id hashing
- `src/execution/hash_join.rs:46-55` — join key hashing
- `src/replication/jsonl.rs:851-852` — a test comparing `back.to_bits()` to `v.to_bits()`

`f64::to_be_bytes()` is bit-preserving (identical to `to_bits().to_be_bytes()`), so NaN payload and sign survive either way — which G.2 requires.

**Not an f64:** `src/branch/record.rs:243` `Some(f) => b.extend_from_slice(&f.to_be_bytes())` is an `Option<i64>` floor (the `None` arm writes `&0i64.to_be_bytes()` at record.rs:238).

---

## J. Framing / helper precedent an encoder should reuse

Shared bounds-checked readers, `pub(crate)`, in `/Users/idide/wt/ferrodb-F10-durable-tel/src/wal/log.rs` (the header comment at wal/log.rs:194-196 states why: *"indexing past the end of a truncated record panics the whole process, which is a denial of service triggered by a corrupt log rather than a parse error"*):
- `write_str` — wal/log.rs:189-192. **`(s.len() as u16)` — silently truncates over 65535.**
- `take_u8` :197, `take_u16` :203, `take_u32` :210, `take_u64` :217, `take_array::<N>` :225, `take_str` :235, `short(at, want, have)` :283
- `crc32` — `src/wal/log.rs:855`

Guarded variants in `/Users/idide/wt/ferrodb-F10-durable-tel/src/consensus/log.rs`:
- `fits_u16` :991, `fits_u32` :996 (→ `LogError::Unrepresentable { what, len, limit }`)
- `put_str` :1002, whose doc reads *"`write_str` with the guard `wal::log`'s own `write_str` does not have."*
- `take_bytes` :1276
- `decode_command` :1162-1172 **refuses trailing bytes**, documented at log.rs:1156-1160: *"A decoder that stops at the end of the value it recognised accepts two encodings of one command, and two encodings mean two nodes can hold byte-different logs that decode identically."*
- `encode_frame` :1009 layout `total_len u32 | term u64 | round u64 | payload | crc32 u32`

`DataType` tag table (needed if a frame ever carries a type): `write_data_type` `src/wal/log.rs:249-264` / `read_data_type` `src/wal/log.rs:267-278` — `Integer=0, Float=1, Boolean=2, Varchar(u16)=3, BigInt=4, Decimal=5, Timestamp=6`. Note these tags **differ from the `Value` tags** in §H.

Provenance file framing (single-file append + fsync + crc, closest analogue to a durable TEL): `src/provenance/durable.rs` — `MAGIC: u32 = 0xF3_EE_50_01` :84, `VERSION: u32 = 1` :85, `TAG_RUN=1` :90, `TAG_STAMP=2` :91, header write :160-161, header check :198-204, per-record frame `total u32 | body | crc32 u32` at :379-382, decode :317-357, torn-tail handling :224-232.

Branch-record precedent for **append-a-field-without-a-version-byte** (tolerant read): `src/branch/record.rs:181-190` + the `c.at >= body_len` check at record.rs:296-300.

---

## K. Enumeration of every distinct encoding decision a `TxnFrame` byte encoder must make

1. 3 length-prefixed lists at the frame level (`ops`, `guards`, `claims`) + 3 more nested (`RowCreate(Vec<Value>)`, `SetRemove{dots}`, `And`/`Or` vecs).
2. 5 `Option` fields: `Op::col`, `Op::witness`, `Guard::source_text`, `EscrowClaim::floor`, `EscrowClaim::ceiling`.
3. 4 tag spaces to define fresh: `OpKind` (8), `GuardExpr` (8), `Delta` (2), plus reuse of the existing `Value` (8). `CmpOp` (6) and `ArithOp` (4) are nullary tag-only.
4. 1 recursive type (`GuardExpr`) with **no producer-side depth bound and no existing decoder precedent for one** — the decode side needs a depth cap invented here, since §F.2 shows the repo has none to copy.
5. 2 byte-exactness constraints: `Decimal` text (§G.1) and `f64` bit pattern (§G.2), neither of which a `PartialEq` round-trip test can detect.
6. 1 unbounded-length string with no cap in the domain (`Decimal`) meeting a `u16` length prefix in the only existing self-describing encoder (§H, defect 2).
7. 1 reserved sentinel to round-trip: `RowId(u64::MAX)` = `RowId::SCHEMA`.
8. `CommitHash`'s 32 bytes have never been written to disk or wire anywhere in this repo (§D).