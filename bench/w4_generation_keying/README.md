# D158 item 1 — `State::workspaces` keyed by the whole `BranchId`

Raw artifacts for W4's correctness item. Every file here is a run's own output, copied
unedited; the interpretation is in `bench/w4/DECISION.md` addendum 6.

| file | what it is |
|---|---|
| `failing-first-at-708822e.txt` | the new test run against the PARENT commit — both halves fail, with the values |
| `firecheck-sites.txt` | six mutants, one per call site, each reverting it to slot-blind |
| `firecheck-chunk.txt` | the chunk-boundary test, and what the pre-existing coverage does with the same mutant |
| `mutate.py` | the mutator, so every row above can be re-run |

## The verdict: LIVE, not latent

The brief asked for the deflation first — *if every path into those eleven sites re-validates
against the catalog inside the same statement lock, the window is closed by construction.* It
does not. `failing-first-at-708822e.txt` is the refutation, over the real statement path:

```
connection A's session was reaped and slot 1 now belongs to agent-b at generation 1, yet
A's SELECT on b1@g0 was answered: qty = Some(999). ... 999 is agent-b's staged, unmerged row.

assertion `left == right` failed: b_1 must still name agent-b's live branch b1@g1;
A's ABANDON of the dead b1@g0 unbound it
  left: None
 right: Some(BranchId { id: 1, generation: 1 })
```

A cross-agent read of unmerged rows, and a cross-agent delete of a live session.

## Fire-check — the baseline passes before any mutant is read

Six mutants, each reverting exactly ONE call site to the slot-blind lookup this change removed:

```
BASELINE unmutated                                  PASS
evict                --lib                              KILLED
overlay              w4_stale_branch_crosses_agents     KILLED
record_read          w4_stale_branch_crosses_agents     KILLED
blind_writes         w4_stale_branch_crosses_agents     KILLED
seal                 w4_stale_branch_crosses_agents     KILLED
forget               w4_sweep_slot_recycle              KILLED
```

`overlay` is the one worth naming. It SURVIVED the first draft of the test, because a `SELECT`
reaches `workspaces` twice and the assertion needed both blind before it failed — so a live leak
through the overlay alone would have shipped green. That is a defect in the instrument, not the
code, and it is why the test now asserts each lookup separately and asserts a VALUE (20, not 999)
where a value exists.

## The coverage gap this change created, measured rather than asserted

`forget_reaped_branches` walks in `FORGET_CHUNK` (1024) chunks and resumes from the last key.
Keying by `BranchId` changed how that resume works. `firecheck-chunk.txt`:

```
BASELINE the_reconciliation_crosses...   PASS
chunk_stop           the_reconciliation_crosses...   KILLED
chunk_stop           w4_sweep_slot_recycle           rc=0
chunk_stop           w4_forget_branches              rc=0
chunk_stop           adv_f6_dropped_capture          rc=0
```

The last three rows are the point: **every test that already exercised this function passes with
the resume broken**, because their fixtures hold a handful of sessions and never cross a chunk.
Without the new test, a sweep that forgets one chunk's worth and abandons the rest — the
unbounded growth the function exists to prevent — lands green.

The test is in `runtime.rs`'s own `mod tests` so its fixture is `FORGET_CHUNK + 1` by
construction. A literal `1025` in `tests/` stops crossing the boundary the day the constant
grows, and nothing would say so.
