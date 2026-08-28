#!/usr/bin/env python3
"""Fire every mutant for F0b's round log and record what each printed.

A rule with no mutant is a rule nobody has shown the tests can see. This applies one deliberate
defect at a time to `src/consensus/log.rs`, runs the test that names that rule, and requires the
test to FAIL. A mutant that leaves the suite green is reported as SURVIVED and is a hole in the
test suite, not a success.

Run from the worktree root:  python3 scratchpad/f0b-mutants.py
"""

import subprocess
import sys
from pathlib import Path

SRC = Path("src/consensus/log.rs")
TESTS = Path("src/consensus/tests_log.rs")

# (mutant name, [(file, old, new)], test that must fail)
MUTANTS = [
    (
        "contiguity check removed from append",
        [(SRC, "            if e.round != expect {\n                return Err(LogError::NonContiguous { expected: expect, got: e.round });\n            }\n", "")],
        "rounds_are_contiguous_from_one_and_an_append_that_would_leave_a_hole_is_refused",
    ),
    (
        "batch validated as it writes, not before",
        [(SRC,
          "            if e.round != expect {\n                return Err(LogError::NonContiguous { expected: expect, got: e.round });\n            }\n",
          "            if e.round != expect {\n"
          "                let mut b = Vec::new();\n"
          "                for fr in &frames { b.extend_from_slice(fr); }\n"
          "                let off = self.end_offset;\n"
          "                let _ = self.write_batch(&b, off);\n"
          "                let mut at2 = off;\n"
          "                for (fr, ee) in frames.iter().zip(entries) {\n"
          "                    self.index.push(Frame { offset: at2, len: fr.len() as u32, term: ee.term });\n"
          "                    at2 += fr.len() as u64;\n"
          "                }\n"
          "                self.end_offset = at2;\n"
          "                return Err(LogError::NonContiguous { expected: expect, got: e.round });\n"
          "            }\n")],
        "a_refused_batch_leaves_the_log_exactly_as_it_was",
    ),
    (
        "term-goes-backwards check removed from append",
        [(SRC, "            if e.term < prev_term {\n                return Err(LogError::TermWentBackwards { last: prev_term, got: e.term });\n            }\n", "")],
        "terms_never_decrease_along_the_log",
    ),
    (
        "crc32 check removed from the recovery scan",
        [(SRC, "        if crc32(&frame[..total - 4]) != stored {\n            break;\n        }\n", "")],
        "a_torn_tail_is_trimmed_and_everything_before_it_survives",
    ),
    (
        "round-equals-position check removed from the scan",
        [(SRC, "        if round != expected_round || term < prev_term {", "        if term < prev_term {")],
        "a_frame_whose_round_does_not_match_its_position_ends_the_scan",
    ),
    (
        "term-monotonic check removed from the scan",
        [(SRC, "        if round != expected_round || term < prev_term {", "        if round != expected_round {")],
        "a_frame_whose_term_goes_backwards_ends_the_scan",
    ),
    (
        "frame length bound removed from the scan",
        [(SRC, "        if total < MIN_FRAME || total > MAX_FRAME || offset + total as u64 > file_len {", "        if total < MIN_FRAME {")],
        "a_frame_length_that_runs_past_the_file_ends_the_scan_without_allocating_for_it",
    ),
    (
        "snapshot floor check removed from every read",
        [(SRC, "        if round <= self.snapshot_round {\n            return Err(LogError::Compacted { asked: round, floor: self.snapshot_round });\n        }\n", "")],
        "a_read_below_the_snapshot_floor_refuses_with_compacted_and_names_the_floor",
    ),
    (
        "truncate_from skips the floor check",
        [(SRC, "        self.check_live()?;\n        self.check_readable(from)?;\n        let last = self.last_round();", "        self.check_live()?;\n        let last = self.last_round();")],
        "a_read_below_the_snapshot_floor_refuses_with_compacted_and_names_the_floor",
    ),
    (
        "checkpoint writes its header before syncing the data it describes",
        [(SRC, "        dst.sync_data().map_err(io)?;\n\n        // 3. The header, and", "        // 3. The header, and")],
        "a_checkpoint_syncs_its_data_before_it_writes_the_header_that_makes_it_live",
    ),
    (
        "checkpoint rewrites the live file instead of the spare",
        [(SRC, "        let spare = 1 - self.live;", "        let spare = self.live;")],
        "a_crash_at_any_point_during_a_checkpoint_loses_nothing",
    ),
    (
        "checkpoint accepts a term that disagrees with the log",
        [(SRC, "            let held = self.frame(through)?.term;\n            if held != term {\n                return Err(LogError::TermMismatch { round: through, held, claimed: term });\n            }\n", "")],
        "a_checkpoint_whose_term_disagrees_with_the_log_is_refused",
    ),
    (
        "append advances the durable frontier without an fsync",
        [(SRC, "        self.index.extend(placed);\n        self.end_offset = at;\n        Ok(())", "        self.index.extend(placed);\n        self.end_offset = at;\n        self.durable_round = self.last_round();\n        Ok(())")],
        "writes_that_were_never_synced_do_not_survive_a_crash",
    ),
    (
        "the zero terminator is not written after a batch",
        [(SRC, "        buf.extend_from_slice(bytes);\n        buf.extend_from_slice(&[0u8; 4]);", "        buf.extend_from_slice(bytes);")],
        "a_zero_terminator_stops_the_scan_before_a_frame_left_over_from_an_earlier_life",
    ),
    (
        "an unreadable header reinitializes over the entries",
        [(SRC, "                if lens[0] > HEADER_SIZE as u64 || lens[1] > HEADER_SIZE as u64 {", "                if false {")],
        "a_live_header_that_cannot_be_read_is_refused_rather_than_reinitialized_over",
    ),
    (
        "two files at one generation are guessed between",
        [(SRC, "                if x.generation == y.generation {", "                if false {")],
        "two_files_claiming_one_generation_are_refused",
    ),
    (
        "a newer on-disk format version is accepted",
        [(SRC, "        if version != VERSION {", "        if false {")],
        "a_log_written_at_a_newer_format_version_is_refused_rather_than_reinitialized",
    ),
    (
        "the entry size limit is removed",
        [(SRC, "    if total > MAX_ENTRY_BYTES {", "    if false {")],
        "an_entry_too_large_for_the_transport_is_refused_at_append",
    ),
    (
        "the command decoder accepts trailing bytes",
        [(SRC, "    if at != bytes.len() {", "    if false {")],
        "the_command_decoder_refuses_trailing_bytes",
    ),
    (
        # The bound on a node list is the slice itself, not a check beside the loop. An earlier
        # mutant that deleted a check beside the loop SURVIVED -- both versions returned an error
        # and nothing could see the allocation -- which is why the code was changed so the bound is
        # structural. This mutant removes the structure: an unchecked slice panics on a corrupt
        # length, which is a denial of service triggered by four bad bytes, and IS observable.
        "the node list bound becomes an unchecked slice",
        [(SRC,
          '    let slice = bytes.get(*at..at.saturating_add(want)).ok_or_else(|| {\n'
          '        LogError::Corrupt(format!(\n'
          '            "a node list claims {n} entries ({want} bytes) but only {} bytes remain",\n'
          '            bytes.len().saturating_sub(*at)\n'
          '        ))\n'
          '    })?;',
          '    let slice = &bytes[*at..*at + want];')],
        "a_node_list_claiming_more_entries_than_the_record_holds_is_refused_rather_than_allocated_for",
    ),
    (
        "the Timestamp column tag drifts from the wal's",
        [(SRC, "        DataType::Timestamp => out.push(6),", "        DataType::Timestamp => out.push(7),")],
        "the_column_type_tags_agree_with_the_wals",
    ),
    (
        "a frame is served without checking the position the index claims",
        [(SRC, "    if got_round != round || got_term != term {", "    if false {")],
        "a_frame_is_cross_checked_against_the_position_the_index_says_it_holds",
    ),
    (
        "a failed truncation is reported without poisoning the log",
        [(SRC, "            let why = format!(\"a truncation to round {} could not be made durable: {e}\", from - 1);\n            self.poisoned = Some(why.clone());\n            return Err(LogError::Poisoned(why));", "            return Err(LogError::Io(format!(\"{e}\")));")],
        "a_truncation_that_cannot_be_made_durable_poisons_the_log",
    ),
    (
        "a range may return nothing when one entry blows the budget",
        [(SRC, "            if !out.is_empty() && (out.len() >= max_entries || bytes + f.len as usize > max_bytes) {", "            if out.len() >= max_entries || bytes + f.len as usize > max_bytes {")],
        "a_range_is_bounded_by_both_limits_and_still_returns_one_entry_when_it_must",
    ),
    (
        "a checkpoint that fails at the header returns an ordinary error instead of poisoning",
        [(SRC,
          '                "a checkpoint at round {through} could not be made durable, so whether the \\\n'
          '                 generation-{} header is live is no longer knowable from this handle: {e}",\n'
          '                h.generation\n'
          '            );\n'
          '            self.poisoned = Some(why.clone());\n'
          '            return Err(LogError::Poisoned(why));',
          '                "unused {through} {e}", h.generation\n'
          '            );\n'
          '            let _ = why;\n'
          '            return Err(e);')],
        "a_checkpoint_that_cannot_be_made_durable_poisons_rather_than_returning_an_ordinary_error",
    ),
    (
        "a failed fsync returns an ordinary error instead of poisoning",
        [(SRC,
          '                "an fsync through round {} failed, and a second fsync cannot be trusted to report \\\n'
          '                 the same failure twice: {e}",\n'
          '                self.last_round()\n'
          '            );\n'
          '            self.poisoned = Some(why.clone());\n'
          '            return Err(LogError::Poisoned(why));',
          '                "unused {}", self.last_round()\n'
          '            );\n'
          '            let _ = why;\n'
          '            return Err(io(e));')],
        "a_failed_fsync_poisons_rather_than_letting_the_next_one_report_success",
    ),
    (
        "the superseded file is retired best-effort and unsynced",
        [(SRC,
          "        let retire = self.files[stale]\n            .set_len(0)\n            .and_then(|()| self.files[stale].sync_all());",
          "        let retire: std::io::Result<()> = { let _ = self.files[stale].set_len(0); Ok(()) };")],
        "a_superseded_file_is_retired_so_a_damaged_header_cannot_rewind_the_log",
    ),
    (
        "the reinitialize refusal keys on any bytes rather than on room for a frame",
        [(SRC,
          "                if lens[0] > HEADER_SIZE as u64 || lens[1] > HEADER_SIZE as u64 {",
          "                if lens[0] > 0 || lens[1] > 0 {")],
        "a_torn_first_header_write_does_not_brick_a_log_that_has_nothing_in_it",
    ),
    (
        "the MAX_FRAME allocation bound is removed from the scan",
        [(SRC,
          "        if total < MIN_FRAME || total > MAX_FRAME || offset + total as u64 > file_len {",
          "        if total < MIN_FRAME || offset + total as u64 > file_len {")],
        "the_scan_refuses_a_frame_longer_than_the_maximum_before_allocating_for_it",
    ),
    (
        "the live file is chosen by the LESSER generation",
        [(SRC,
          "                if x.generation > y.generation { 0 } else { 1 }",
          "                if x.generation < y.generation { 0 } else { 1 }")],
        "the_live_file_is_the_one_at_the_greater_generation",
    ),
    (
        "the header magic is not checked",
        [(SRC,
          "        if u32::from_be_bytes(bytes[0..4].try_into().unwrap()) != MAGIC {\n            return Ok(None);\n        }\n",
          "")],
        "a_header_with_the_wrong_magic_is_not_a_header",
    ),
    (
        "a string longer than its length field is written anyway",
        [(SRC,
          "fn put_str(out: &mut Vec<u8>, s: &str, what: &'static str) -> Result<(), LogError> {\n    fits_u16(s.len(), what)?;",
          "fn put_str(out: &mut Vec<u8>, s: &str, what: &'static str) -> Result<(), LogError> {\n    let _ = what;")],
        "a_value_the_length_fields_cannot_express_is_refused_before_it_becomes_durable",
    ),
    (
        "a column count wider than its length field is truncated",
        [(SRC,
          '            out.extend_from_slice(&fits_u16(columns.len(), "column count")?.to_be_bytes());',
          "            out.extend_from_slice(&(columns.len() as u16).to_be_bytes());")],
        "a_value_the_length_fields_cannot_express_is_refused_before_it_becomes_durable",
    ),
    (
        "a device error reading log bytes is reported as corruption",
        [(SRC,
          "fn read_at(file: &dyn Storage, buf: &mut [u8], offset: u64) -> Result<(), LogError> {\n    pread_all(file, buf, offset).map_err(io)\n}",
          "fn read_at(file: &dyn Storage, buf: &mut [u8], offset: u64) -> Result<(), LogError> {\n    pread_all(file, buf, offset).map_err(LogError::from)\n}")],
        "a_device_error_reading_the_log_is_reported_as_io_and_not_as_corruption",
    ),
]


def run(test):
    p = subprocess.run(
        ["cargo", "test", "--lib", f"consensus::log::tests_log::{test}", "--", "--exact"],
        capture_output=True,
        text=True,
        timeout=900,
    )
    return p


def main():
    originals = {f: f.read_text() for f in {SRC, TESTS}}
    results = []
    try:
        for name, patches, test in MUTANTS:
            for f, old, new in patches:
                text = f.read_text()
                if text.count(old) != 1:
                    print(f"!! mutant '{name}': anchor found {text.count(old)} times, skipping")
                    results.append((name, test, "ANCHOR-MISSING", ""))
                    break
                f.write_text(text.replace(old, new))
            else:
                p = run(test)
                out = p.stdout + p.stderr
                if "error[" in out or "error:" in out and "test result" not in out:
                    verdict = "DID-NOT-COMPILE"
                    line = next((l for l in out.splitlines() if l.startswith("error")), "")
                elif p.returncode != 0:
                    verdict = "KILLED"
                    line = next(
                        (
                            l.strip()
                            for l in out.splitlines()
                            if "assertion" in l or "panicked at" in l
                        ),
                        "",
                    )
                    detail = [l.strip() for l in out.splitlines() if l.strip().startswith(("left:", "right:", "assertion"))]
                    line = " | ".join(detail[:3]) or line
                else:
                    verdict = "SURVIVED"
                    line = ""
                results.append((name, test, verdict, line))
                print(f"{verdict:16} {name}")
                if line:
                    print(f"                 -> {line[:400]}")
            for f in originals:
                f.write_text(originals[f])
    finally:
        for f, text in originals.items():
            f.write_text(text)

    print()
    survived = [r for r in results if r[2] != "KILLED"]
    for name, test, verdict, line in results:
        print(f"{verdict:16} {name}  [{test}]")
    print()
    print(f"{len(results) - len(survived)}/{len(results)} mutants killed")
    return 1 if survived else 0


if __name__ == "__main__":
    sys.exit(main())
