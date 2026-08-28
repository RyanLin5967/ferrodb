# F7 — signed inter-node traffic: the mutant table

Referenced from `src/consensus/tests_signing.rs`'s header. Every rule in `src/consensus/signing.rs`
has a test named against it, and every one of those tests has been **seen to fail** against a
deliberate defect in the rule it names. A rule with no mutant is a rule nobody has shown matters;
a test nobody has seen fail is not evidence.

Method: assert the tree is clean and **committed**, apply the defect, run only the named test(s),
record what they printed, `git checkout --` the file, and assert the tree is clean again. The
drivers are `/tmp/mutants.py` and `/tmp/mutants3.py`; their output is reproduced below verbatim.

Eleven chosen mutants, killed by fifteen test-kills. A twelfth was not chosen: an adversarial pass
found it as a **live bug**, and it is in its own section below along with two further findings.
Every run ended `GIT STATUS: (clean)`.

Run on 2026-08-28, macOS (darwin 25.6.0), `rustc 1.97.1`, under CI's own flags
`RUSTFLAGS="-D duplicate_macro_attributes -D dead_code"`.

| # | The defect | Test that killed it | What it printed |
|---|---|---|---|
| M1 | The term is left out of the authenticated region (sign `from\|to\|kind\|body`, skipping bytes 8..16) | `the_term_is_inside_the_mac` | `a raised term must not authenticate: [0,0,0,2, 0,0,0,1, 0,0,0,0,0,0,0,99, 2, ...]` — the frame carrying term 99 verified |
| M1 | as above | `every_byte_of_the_message_is_inside_the_mac` | `flipping a bit of body byte 8 left the frame verifying` |
| M2 | `constant_time_eq` becomes `a == b` | `constant_time_eq_does_not_short_circuit` | `a difference in the FIRST byte was answered in 0ns and one in the LAST in 18.334µs, over 1048576 bytes` |
| M3 | The key file's mode is not checked (`if false && mode & 0o077 != 0`) | `a_key_file_readable_by_group_or_other_is_refused` | `mode 0640 is readable beyond its owner and must be refused` |
| M4 | `MIN_KEY_BYTES` drops from 32 to 1 | `a_key_file_under_thirty_two_bytes_is_refused` | `a 1-byte key must be refused` |
| M5 | The MAC becomes `sha256(key \|\| message)` — the length-extendable construction | `hmac_sha256_agrees_with_rfc_4231` | `left: "9000e06f7d262eac…"` vs `right: "b0344c61d8db3853…"` (RFC 4231 test case 1) |
| M5 | as above | `the_tag_is_not_sha256_of_the_key_followed_by_the_message` | `assertion left != right failed` — the two are byte-identical |
| M6 | The transport stops verifying inbound frames (`let verified = &body[..]`) | `a_peer_that_does_not_sign_is_refused_by_a_node_that_does` | `an unsigned frame must be refused and counted` |
| M6 | as above | `an_attacker_raising_the_term_on_the_wire_never_reaches_the_state_machine` | `the forged term must be refused and counted: left: 0, right: 1` |
| M7 | The tag's 32 bytes are not charged against `MAX_FRAME_BYTES` | `a_message_that_fits_unsigned_is_refused_signed_rather_than_framed_over_the_limit` | `one byte over must refuse: [104,121,120,…]` — an 8 MiB + 33 byte frame was produced |
| M8 | The domain prefix is dropped from the authenticated region | `a_tag_is_domain_separated_from_a_bare_hmac_over_the_same_bytes` | `assertion left != right failed` — the two are byte-identical |
| M9 | The unverifiable platform falls through to allowed (`Enforce => Ok(())`) | `the_platform_that_cannot_read_its_own_file_protection_refuses_rather_than_allowing` | `a build that cannot inspect the file must NOT report it protected: ()` |
| M10 | The directory holding the key is not checked | `a_key_in_a_group_or_world_writable_directory_is_refused_unless_it_is_sticky` | `a key in a world-writable directory must be refused: Key { bytes: <32 redacted>, source: Some(".../open/k") }` |
| M11 | `Key`'s `Debug` prints the bytes, as a derive would | `the_key_never_appears_in_a_debug_rendering` | `the key's bytes must not be printable: Key { bytes: "0c131a21282f363d…", source: None }` |

## The timing detector, and why its threshold is not arbitrary

`constant_time_eq_does_not_short_circuit` is the one test here whose instrument is a clock, so its
margin is measured rather than asserted. Over a 1 MiB buffer, minimum of 15 repetitions, debug
build, on the machine above:

| Implementation | difference in the FIRST byte | difference in the LAST byte | ratio |
|---|---|---|---|
| the real one (5 consecutive runs) | 5.489 ms, 5.562, 5.535, 5.630, 5.634 | 5.448 ms, 5.488, 5.600, 5.639, 5.444 | **1.00** |
| M2, `a == b` | 0 ns | 18.334 µs | unbounded |

The assertion is `first * 4 >= last`. A measured ratio of 1.00 on one side and an unbounded one on
the other means the threshold sits in an enormous gap, not near either. **25 consecutive runs of the
unmutated test passed, 0 failed**, on a machine running an agent fleet — the statistic is the
*minimum* over repetitions precisely because scheduling noise only ever adds time.

Why 1 MiB and not the 32 bytes a tag actually is: at 32 bytes a short-circuiting `memcmp` and a full
scan are both a handful of nanoseconds, so the test would pass against the very implementation it
exists to reject. That is the vacuous-detector shape this project keeps meeting, and it was avoided
by choosing a size at which the two are three orders of magnitude apart.

## The two rules the adversarial pass added

The eleven above were mutants I chose. An adversarial review in fresh contexts then found two
things I had not, and both became rules with their own tests. They are listed separately because
the distinction matters: the first eleven test rules I already believed; these two exist because
somebody attacked the work.

| # | The defect | Test that killed it | What it printed |
|---|---|---|---|
| M12 | The empty parent is filtered out as "nothing to check" — the code as originally written | `the_spelling_of_the_path_does_not_decide_whether_the_directory_is_checked` | `two spellings of one path must reach one verdict; bare=None dotted=Some("io error: the directory holding the consensus signing key, ., has mode 0777 ...")` |

**M12 was a live bug, not a hypothetical.** `Path::parent()` of a bare relative name is `Some("")`,
which means the *current* directory and not "there is no directory". Filtering it out meant
`Key::load("cluster.key")` skipped the directory-permission check entirely while
`Key::load("./cluster.key")` performed it — the same file, in the same directory, with the spelling
of the path deciding whether a security check ran at all. The reviewer demonstrated it by replacing
the key in a `0777` directory and showing the loader accepted the attacker's key.

**M13 — and a claim I made about it that was wrong.** The same review found that a failed `stat` of
the directory fell through to `Ok(())`; a guard that cannot read its own input must refuse, and it
now does.

I first recorded that branch here as *unreachable from a test*, arguing that `File::open` resolves
the path before the directory is stat'd, so anything that breaks the stat breaks the open. **That
was wrong and a later adversarial pass refuted it by racing the condition**: renaming the parent
directory back and forth in a second thread while loading a 0600 key from a 0777 directory produced

```
loaded=46895  refused_for_the_directory=105012  refused_otherwise=877831   (6s)
```

46,895 successful loads of a key the rule refuses — every one of them the check being skipped. The
original claim is left standing above rather than edited away, because the reversal is the point:
"I could not think of a way to fire it" is not the same statement as "it cannot be fired", and this
file had recorded the first as the second. What is genuinely missing is a *deterministic* test, not
reachability; the branch refuses now, and `a_key_whose_directory_is_gone_is_refused` says so in its
own body.

| # | The defect | Test that killed it | What it printed |
|---|---|---|---|
| M14 | Only the NAME's directory is checked, never the resolved inode's — the symlink hole as shipped | `a_symlink_cannot_launder_a_key_out_of_a_world_writable_directory` | `the same inode, in the same 0777 directory, loaded because it was named through a link in a 0700 one — the directory rule was bypassed by spelling` |
| M15 | Only the RESOLVED directory is checked, never the name's | `a_symlink_whose_own_directory_is_open_is_also_refused` | panicked at `a repointable link is a replaceable key` |

**M14 was the review's best find and it was a total bypass, not a wrong verdict.** The mode check
reads the open descriptor, so it sees the target's mode and was always right. The *directory* check
read `path.parent()` — the symlink's parent — and never the directory the inode sits in. A link in
a 0700 directory pointing at a key in a 0777 one therefore passed. The reviewer drove it to the end:
with the real key loaded through the link, an attacker with write access to the target's directory
renamed their own key over it, `Key::load` returned `Ok` with no complaint, and `verify_frame` then
accepted frames the **attacker** had signed. Total compromise of this module's guarantee, reached
without ever reading the operator's key.

Both directories are checked now, and M15 exists because checking only the resolved one would be
the mirror-image hole: whoever can write to the link's directory repoints the link. Note that
M15 does **not** kill `the_spelling_of_the_path_does_not_decide_whether_the_directory_is_checked`
(that test still passes under it, because canonicalisation happens to cover the bare-relative case
too) — which is why the symlink test is the one named against it.

**A third finding needed no mutant because it was an absence.** The review showed that
`NodeOptions` had no way to express a key at all, so `examples/consensus_node.rs` built an unsigned
node and a keyless peer set its term to 500 — the whole row was unreachable from the surface a
product uses. `NodeOptions::signed_with` closes it, and
`a_keyless_peer_cannot_raise_the_term_of_a_node_built_through_the_driver` carries both halves: the
signed node's term does not move, and the identical bytes at an unsigned node do set it to 500.

## A correction to this file's own method

The first run of the M12/M13 harness was made against a tree with **uncommitted** work in it. The
harness restores with `git checkout --`, which discarded the fix before it was committed — so a
commit whose message described the fix contained only its tests. At the same time, review agents
were installing their own mutants in this same worktree, and one of them reasonably reported the
resulting failure as a live defect.

Both are recorded here rather than tidied away, because the method is part of the evidence:

* **Mutate only a committed tree.** The harness now asserts `git status --porcelain` is empty before
  it starts, and M12's re-run above was made under that assertion and ended `GIT STATUS: (clean)`.
* **One writer per worktree.** Mutation-running reviewers get their own tree from
  `~/.claude/bin/wt new <branch>`; two agents mutating one checkout produced results neither could
  attribute.

Every number in this file was re-measured after that correction.

## What has no mutant, and why

* **Verify-before-decode ordering.** `conn_loop` verifies and then parses, and the two orders are not
  behaviourally separable from outside: both refuse the frame, both close the connection, both leave
  the counter at 1. What M6 proves is the stronger observable — that an unverifiable frame reaches
  neither the parser nor the caller. The ordering itself is one expression's position and is argued
  in the code, not measured.
* **Zeroization on drop.** No assertion available in-process can observe freed heap memory, and one
  that read it back would be reading memory it does not own. Stated as hygiene in the module header
  rather than claimed as a defence.
* **The Windows wiring.** `accept_unverifiable`'s *decision* is a plain function with no `cfg`, so
  M9 fires on every platform. What only the Windows runner can execute is the single line
  `if !PROTECTION_IS_CHECKABLE { return accept_unverifiable(check, path) }`, covered by
  `on_this_platform_load_refuses_and_the_operator_must_say_so_explicitly` (`cfg(not(unix))`) and by
  `protection_is_checkable_exactly_where_std_exposes_the_mode_bits` everywhere. This machine is
  darwin and could not run the first of those; CI's `windows-latest` runner is its instrument.
