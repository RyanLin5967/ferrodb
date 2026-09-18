# Measurement: where GuardExpr recursion overflows the stack

Instrument: `tests/zz_depth_probe.rs` (temporary, deleted after the run), debug build,
`cargo test --test zz_depth_probe -- --test-threads=1`, macOS 25.6, libtest thread stack.
`nest(d)` builds `d` nested `GuardExpr::Not` around a `Literal`.

## The codec under test (encode + fsync + reopen + decode through `DurableEffectLog`)

| depth | result |
|---|---|
| 128 | ok |
| 256 | ok |
| 384 | ok |
| 512 | ok |
| 768 | **stack overflow, process abort (SIGABRT)** |
| 1024 | **stack overflow** |

## The pre-existing guard operations, each on its own thread, with no codec involved

`Guard::clone` (recursive), `Guard::check` (recursive eval), `GuardExpr::to_string`
(recursive Display), and `Drop` (recursive) — all of which exist independently of F10:

| op | 400 | 600 | 800 |
|---|---|---|---|
| drop | — | ok | ok |
| clone + drop | — | ok | ok |
| check (eval) | ok | ok | — |
| to_string (Display) | ok | ok | — |
| clone + check + Display, combined | ok (384) | ok (512) | overflowed at 1024 in the first run |

## What this settles

`GuardExpr` is recursive in `Clone` and in `Drop`, so **no codec can remove the depth limit** — an
iterative encoder/decoder would only move the abort to the point where the decoded frame is cloned
or dropped. A depth cap is therefore the right mechanism rather than a workaround, and its value has
to sit below the measured abort point with margin for a smaller thread stack than libtest's
(`pgwire::serve` spawns one thread per connection) and for a build with larger frames.

`MAX_GUARD_DEPTH = 256`: half the deepest depth measured to work through the codec, and roughly a
third of where the pre-existing guard operations abort. A predicate nesting 256 operators is far
past anything `tel::capture::to_guard_expr` has produced from real SQL — and note that `And`/`Or`
are emitted as *binary* nodes there, so the depth of `a AND b AND c` is 3, not 1.
