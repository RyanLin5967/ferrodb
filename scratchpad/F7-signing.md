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

## Four more, from a second adversarial pass with its own worktree

Two attackers were given their own trees and told to break the thing rather than review it. One
(`forge-a-tag`) could not forge anything — 914 expected values from CPython's `hmac`, 576 single-bit
flips, 157 frame splices, 256 near-keys, and a working length-extension forgery demonstrated
against `sha256(key||msg)` and then failed against this HMAC. That is a clean result and it is
recorded as one. The other (`load-a-bad-key`) found three defects and one correction.

| # | The defect | Test that killed it | What it printed |
|---|---|---|---|
| M16 | The shape is not checked by name, so a FIFO blocks the open for ever | `a_fifo_named_as_a_key_is_refused_rather_than_hanging_the_node` | `Key::load blocked on a FIFO instead of refusing it — a node pointed at one hangs: Timeout` |
| M17 | A file of zeros is accepted as a key | `a_key_file_of_zeros_is_refused_because_that_is_what_a_failed_generator_leaves` | `a 32-byte file of zeros must be refused` |

**M16 hung the node rather than refusing it.** Opening a FIFO for reading blocks until a writer
appears, and the `is_file()` check sat on the descriptor — so control never reached it and a node
configured with a FIFO as its key path hung at startup with no message. The shape is now checked by
*name* first. That lookup is deliberately not a security check (the authoritative mode check is
still `fstat` on the descriptor), so its race with the open does not matter. Note the test carries
its own 10-second deadline: without one it would *hang* under the mutant rather than fail, and a CI
job that times out with no message is barely better than the bug.

**M16's reachability is the argument that makes it worth fixing**, and it came from the reviewer
rather than from me. Sticky-plus-`0777` is a layout this module's directory rule *accepts* — that is
the whole point of the sticky exception — and **sticky prevents replacing or removing a file you do
not own, but not creating a name**. So any unprivileged user on the machine can plant a FIFO at the
operator's configured key path *before* the operator writes the key there, and the header's "a node
unable to load its key refuses to start" becomes a silent hang with nothing in any log.

The same pass inventoried every other shape, which is what makes the fix a shape check rather than
a device-type enumeration — **the FIFO is the only one that blocks**:

```text
fifo           BLOCKED (no answer in 1200ms)
unix socket    Err("... could not be opened: Operation not supported on socket (os error 102)")
regular file   Ok(32)
directory      Err("... is not a regular file ...")
/dev/null      Err("... is not a regular file ...")
/dev/zero      Err("... is not a regular file ...")
/dev/random    Err("... is not a regular file ...")
/dev/urandom   Err("... is not a regular file ...")
/dev/tty       Err("... could not be opened: Device not configured (os error 6)")
/dev/console   Err("... is not a regular file ...")
```

**M17 was a key nobody chose, passing every rule.** A file of exactly 32 zero bytes is what
`truncate -s 32`, a sparse copy, or a generation script that wrote nothing and exited 0 leaves
behind. Every node given it agrees with every other, so the cluster comes up, signs, verifies, and
reads healthy — on a key an attacker guesses first. Its limit is stated in the code: this is a
"the generator produced nothing" test, not an entropy test, and 32 identical `0xff` bytes still
load.

**Two facts about key identity, from the same pass, that are RFC 2104 consequences rather than
defects** — both now in the module header, because an operator rotating a key needs them: a key and
the same key zero-padded to at most 64 bytes are the *same key*; and a key longer than 64 bytes and
its own SHA-256 are the *same key*. Appending NULs to a key file is not a rotation.

## The chain walk: a fix that was itself broken, and what replaced it

The symlink fix above (M14/M15) checked the path as given and the canonicalised path. **A second
adversarial pass broke it deterministically**, and the mechanism is worth recording because the fix
looked obviously right:

```text
safe/cluster.key   parent 0700   <- the path as given: checked
  -> open/k        parent 0777   <- an intermediate name: checked by NOBODY
    -> known       parent 0700   <- the canonicalised path: checked
```

`canonicalize` **collapses** the chain, so the two things being checked are its two endpoints and
every hop between them is invisible. The attacker renames a symlink over the middle name, pointing
it at any file the node can already read — a rotated key, a fixture, a log. **They never author a
key file at all**, so the mode rule passes on the target's own `0600` and nothing about ownership
catches them. The lever is redirection, not authorship, which is exactly why a fix aimed at
"symlinks" missed it.

The reviewer also corrected their own earlier finding in the same message: the original substitution
needed a *same-uid* attacker, which they had not said. A different-uid attacker cannot get a file
they authored past the mode rule. That correction makes the chain finding the more serious of the
two, and it is recorded here because volunteering it is what made the rest credible.

**This is not the stated ancestry limit, and the distinction is why the endpoint pair could not be
patched into correctness.** `open/` is an ancestor of neither endpoint — not of the configured path,
not of the canonical one. It exists only *inside* the resolution. Checking every ancestor of both
endpoints, all the way to the root, would still never see it. That is what forced a walk.

`directories_to_check` then walked the chain hop by hop, checking the directory each *name* sits in,
bounded at 40 hops. **That was still not enough — see the next section.**

| # | The defect | Test that killed it | What it printed |
|---|---|---|---|
| M18 | The walk stops at the first hop — i.e. the endpoints-only fix that was broken | `no_hop_of_the_symlink_chain_escapes_the_directory_rule` | `the middle hop sits in a 0777 directory ... : Key { bytes: <32 redacted>, source: Some(...) }` — it loaded |
| M19 | A relative link target resolves against the cwd, not the link's directory | `a_relative_link_target_resolves_against_the_links_own_directory` | panicked at the 0777 assertion |
| M20 | The hop bound is removed | **SURVIVED** — see below |

**M20 survived, and that is recorded rather than hidden.** Removing `MAX_HOPS` did not make the
cycle test fail, because a cycle is refused earlier: `fs::metadata` in the shape check returns
`ELOOP` (`os error 62`). The test was passing for a reason other than the one in its name, which is
the vacuous shape this project keeps meeting — so it was renamed to
`a_symlink_cycle_is_refused_by_the_shape_check_before_the_walk_begins` and now asserts that exact
cause. A non-cyclic chain longer than the bound cannot be built either: every OS here caps symlink
resolution near 32, below the bound. The bound is belt-and-braces against an edit that removes the
shape check, and it has no killing test *here, by this mechanism, today* — phrased that way because
this file has already been wrong once about calling a branch untestable.

Two further shapes were reasoned about and then pinned rather than left as arguments: a symlinked
directory *component* is judged by what it points at (`check_directory` uses `fs::metadata`, which
follows — the opposite of the walk, where not following is the point), and a relative target with a
`..` traversal resolves against the link's own directory.

## Third version of the directory rule, and the two that were wrong

The chain walk was broken too, by the same pass, with the same lever one level up.
`symlink_metadata(name)` asks whether that final *name* is a link — and the kernel has already
silently resolved every directory above it to get there. So a symlink among the **directory**
components is a name in the resolution the walk never saw:

```text
a/k -> b/dl/real      and      b/dl -> c
```

`b` holds the link `dl`, and `b` is an ancestor of **neither** endpoint — not of the configured
path, not of the file finally opened. So it is not the stated ancestry limit either. The attacker
repoints `dl` at a directory the node already owns holding a file it can already read (a rotated
key, a backup): they author no key and own nothing in the chain.

The reviewer's sweep, one directory flipped to 0777 at a time:

```text
plain file          a=refused
link, absolute      a=refused  b=refused
link, relative ..   a=refused  b=refused
three hops          a=refused  mid=refused  b=refused
leave and re-enter  a=refused  mid=refused
dir-component link  a=LOADED   b=refused
dir-component mid   a=refused  b=LOADED     c=refused
```

**All three versions of this rule were the same mistake at a different depth**, which is why the
third stops patching and resolves the path the way the kernel does: one component at a time from the
root, checking the directory each component sits in.

| # | The defect | Test that killed it | What it printed |
|---|---|---|---|
| M21 | Only the LAST component's directory is checked — the hop walk, one level up | `a_directory_component_symlink_cannot_redirect_the_key` | `repointing it redirects the key without touching either endpoint: Key { bytes: <32 redacted>, source: ... }` — it loaded |
| M21 | as above | `every_directory_above_the_key_is_inspected_not_only_its_parent` | `a world-writable grandparent must be refused: Key { bytes: <32 redacted>, ... }` |
| M22 | An absolute link target does not reset the resolved prefix | **SURVIVED — and the line was deleted** |

**M22 is why running a mutant you expect to die is worth it.** Deleting
`if target.is_absolute() { resolved = PathBuf::new() }` changed no test, because an absolute path's
first step *is* `Step::Root`, whose arm already resets the prefix. It was one rule stated twice — the
kind that goes stale. The mutant did not reveal a missing test; it revealed redundant code, and the
code was removed rather than a test invented to justify it.

**The stated ancestry limit is gone, deliberately.** Every directory above the key is inspected now,
because a symlink one level up redirects the key just as well as one at the end. That is OpenSSH's
`StrictModes` rule, for the same reason, and the cost is real: a key under a group-writable prefix
is refused rather than warned about. Checked before committing to it — every ancestor of a
`tempfile::tempdir()` and of this repo is `0755` or tighter here, and Linux `/tmp` is sticky, which
the rule accepts.

## A correction to a number in this file

The 32,000-per-6-seconds rate quoted for M13 above came from the reviewer and **they withdrew it
themselves while answering a question about flakiness**: their counter matched a phrase
`directories_to_check` also emits, so most of those hits were the hop walk rather than
`check_directory`. Split by `check_directory`'s own wording, the true rate at the fixed HEAD is
**320–402 per 50,000 iterations** across three runs. The larger number is left visible above with
this correction beside it rather than edited out — it was correct for `a585c57`, and the reversal is
the useful part.

## A file this document destroyed, and how

`scratchpad/F7-signing.md` — this file — was committed **empty** as `ae76c6f`, and the content above
was recovered from `11edc55`. The cause was one line in an editing script:

```python
open(p,'w').write(open(p).read().replace(old, new, 1))
```

`open(p,'w')` truncates the file when it is evaluated, which happens *before* the `open(p).read()`
inside the argument. So the read returned an empty string and the file was rewritten with nothing.
Recorded here rather than quietly fixed because it is the same class as the earlier harness bug: an
edit that assumes it is the only thing touching a file, when it is not — in this case, itself.

## The ancestry rule's cost, measured rather than argued

The change to inspect every ancestor is the one I most wanted a second opinion on, and the reviewer
gave a measurement instead of one. On this machine `/opt/homebrew/etc` is `drwxrwxr-x idide:admin` —
group-writable and not sticky — so a key at `<prefix>/etc/ferrodb/cluster.key` is refused even
though the operator made its own directory `0755`. `/usr/local` has the same shape on Intel Macs.

Their judgement, which I took: **keep the rule, fix the message.** The refusal is correct — anyone
in `admin` can rename the `ferrodb` directory and swap the key, which the old
immediate-parent-only rule was quietly accepting. What was wrong was the advice. It said
`chmod go-w /opt/homebrew/etc`, naming a directory a package manager owns and resets on its next
operation, and nothing in the text told the operator the problem was three levels above their key
rather than in it. **A security refusal an operator cannot act on is one they work around.**

| # | The defect | Test that killed it | What it printed |
|---|---|---|---|
| M23 | Every refusal uses the immediate-parent wording | `an_ancestor_refusal_does_not_tell_the_operator_to_chmod_somebody_elses_directory` | `it must say the offender is an ancestor: io error: the directory holding the consensus signing key, .../homebrew/etc, has mode 0775 ...` |

The reviewer also confirmed the rule does not refuse the ordinary layouts: `0755` all the way down,
a private tree, a group-readable-but-not-writable parent, and a sticky shared parent all still load.

## Two mutants aimed at the socket tests, one of which found a vacuous test of mine

A review agent I stopped had left an uncommitted mutant in its worktree — `dial` never connects —
aimed at the question "would these refusal tests pass if the transport simply never talked?". It is
the right question and the answer was partly no.

| # | The defect | Effect |
|---|---|---|
| M24 | `dial` never connects (outbound is dead) | 3 tests fail. **`a_signed_peer_is_refused_by_a_node_with_no_key` PASSED** — it was vacuous |
| M25 | `conn_loop` never delivers to the caller (inbound is dead) | 4 tests fail, including the rewritten one — the positive controls fire |

**M24 found a real hole in my own test.** `a_signed_peer_is_refused_by_a_node_with_no_key` asserted
`received() == 0` and `unauthenticated() == 0` on a pair of `Transport`s. Both of those hold when
nothing ever connects, so two zeros were being read as a refusal when they are equally a dead
network. Three sibling tests failed under the mutant, exactly as they should; this one did not.

Rewritten to drive a raw socket with its own positive control: an **unsigned** frame over the same
address must arrive first, proving the path is live, before a **signed** frame over that same path
is required not to. It is correctly insensitive to M24 now (it never dials), and M25 — breaking the
inbound path instead — makes its positive control fire, which is the evidence that the control is
real rather than decorative.

## The one red mark in the suite, and why it is not this row's

`integration_base_backup::synchronous_commit_waits_for_a_replica_and_says_so_when_there_is_none`
failed twice during full-suite runs of this branch. **Settled by measurement rather than by
argument**, because "it looks unrelated" is exactly the claim that should not be taken on trust.

Instrument: a fresh worktree at `af77d8a` — the commit immediately before any F7 work — examples
rebuilt, then the target run three times while the agent fleet and this branch's suite loaded the
machine.

```text
baseline af77d8a run 1: FAILED. 4 passed; 1 failed;  finished in 215.92s
baseline af77d8a run 2: ok.     5 passed; 0 failed;  finished in  23.19s
baseline af77d8a run 3: ok.     5 passed; 0 failed;  finished in  23.21s
```

Same failure mode as on this branch: `examples/repl_replica.rs:118` gets `ConnectionRefused`, and
the test's own diagnostic reports the primary's status as `Some(ExitStatus(0))` — it had already
exited. A process race between the primary exiting and the replica dialling it, which widens under
contention: 216 seconds of wall clock for the same five tests against 23.

Corroborating structurally, though the measurement is what settles it: this row's diff is six files
— `PROGRESS.md`, `scratchpad/F7-signing.md`, and `src/consensus/{node,signing,tests_signing,
transport}.rs`. Nothing in `src/replication/`, nothing in `examples/`, nothing in `tests/`. The
failing test drives `examples/repl_primary.rs` and `examples/repl_replica.rs` over
`src/replication/`, none of which this row can reach.

It is a real flake in the existing suite and worth someone's attention; it is not F7's, and it is
not fixed here.

## The final mutant tally

Twenty-five fired. **Twenty-two killed** by the test named against each. **Two survived**, and both are
recorded above rather than quietly re-aimed: M20 showed a cycle test was passing for the wrong
reason (renamed, and it now pins the real cause), and M22 showed a line of code was redundant (the
line was deleted). **One — M13 — has no deterministic test in this suite**: the reviewer demonstrated
the branch is reachable by racing a rename, at a measured 320–402 hits per 50,000 iterations against
the fixed code, but a test that needs a race to land is not one this suite will carry. The branch
refuses; the claim that it is *detected* is not made.

## A misattribution of the reviewer's work, corrected

Twice I told the reviewer they had measured an old commit. Once that was true. Once it was not:
their directory-component finding was made against a merge of my chain-walk commit, at the commit I
had asked for, and was a real hole in it. Recorded because this file spends most of its length
holding a reviewer's numbers to a standard, and the same standard applies to claims made *about*
their numbers.

They also independently found this file empty in the committed tree and reported it with two of
their own shell loops giving sizes that meant the opposite conclusion. They said so, declined to
assert a mechanism for the discrepancy they had not checked, and gave five independent
confirmations of the part they were sure of — including git's canonical empty-blob hash, which is
the one that settles it. That is why their findings were acted on rather than argued with.

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
