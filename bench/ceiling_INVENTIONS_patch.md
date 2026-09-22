# APPEND-ONLY PATCH FOR `frontier/INVENTIONS.md`

⛔ **Not applied by me.** `frontier/INVENTIONS.md` lives in the `artie-research` repo, outside this
agent's worktree, and other sessions were editing that repo during this run (`INVENTION-TRIGGER.md`
mtime 2026-09-22 03:01). A worktree-isolated agent does not write into another live checkout. The
exact text is here so applying it is mechanical.

## Where

`frontier/INVENTIONS.md`, immediately after the block that currently reads:

```
> **Removing keys SATURATES.** `HOLD` falls **21x** from L0→L3 but throughput rises only **1.9x**,
> because the gap rises **12.6x** and lock utilisation collapses from **96% to 8.5%**.
```

…and its `⇒ **Section and handoff TRADE OFF**` paragraph. **Append, do not alter the above.**

## What to append

> ## ⚠ THE SATURATION IS REAL. ITS ATTRIBUTION TO "HANDOFF" IS NOT — MEASURED 2026-09-22.
>
> Pre-registered in `frontier/INVENTION-TRIGGER.md` ("IS THE 1.9× CEILING THE STRUCTURE, OR THE
> MUTEX?", Amendments 1–2), run as `examples/ceiling_lock_contention` on branch
> `CEILING-resume`. Evidence: `bench/ceiling_park_or_structure.txt` and `bench/ceiling_raw/`.
> **Pre-registered outcome 2 fired**: the remaining time is not scheduler wake-up.
>
> Measured at the `TableBranchCatalog` layer — the same layer as the quoted pair, beneath the
> outer statement mutex production holds (Amendment 1's fourth condition).
>
> * **The saturation reproduces.** Utilisation collapses 87.6% → 5.5% L0→L3 (15.9×), against the
>   recorded 96.1% → 8.5% (11.3×), and `HOLD` falls 23.3× against the recorded 21×.
> * **`throughput = 1/S` still fails, so the row's conclusion stands.**
> * ⛔ **But the gap is NOT the handoff.** Time spent BLOCKED ACQUIRING `logical` (`PH_WAIT`,
>   nanoseconds, same probe, same loop) falls **64×** across L0→L3 — *faster* than `HOLD` falls.
>   Time spent in `durable()` is **flat** (21.8 → 17.1 ms/fork summed over 64 threads) and is
>   **98% of the thread cycle at L3**. The closure identity holds at 98–99% at every rung.
> * ⇒ **The floor is the group-commit fsync, not park/unpark.** The section and the DISK trade
>   off. Nothing about scheduler wake-up is in this data.
>
> ⇒ **What changes for the retired "take work out of the critical section" family:** it stays
> retired, and the 21×/1.9× pair remains the number to state a candidate against — but a candidate
> must now state where it lands against **group-commit throughput**, not against a handoff floor.
> Those are different quantities on different axes, and only the second one was ever measured.
>
> ⇒ **What does NOT change:** "past some point the handoff, not the section, is what is left" is
> **withdrawn as written**. It should read "past some point the DURABILITY BARRIER, not the
> section, is what is left."

## And in `frontier/INVENTION-TRIGGER.md`

The line `⚠ **And the most load-bearing number in this file is itself unverified**` (above the
pre-registered entry) should gain: *"Verified 2026-09-22. The pair reproduces; the mechanism
attached to it does not. See `ferrodb:bench/ceiling_park_or_structure.txt`."*
