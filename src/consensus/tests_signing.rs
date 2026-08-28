//! F7's tests. Every rule in `signing.rs` has a test that names it, and every one of those tests
//! has been seen to **fail** against a deliberate defect in the rule it names.
//!
//! The mutant table — which defect, which test killed it, and what each printed — is in
//! `scratchpad/F7-signing.md`. A rule with no mutant is a rule nobody has shown matters, and a test
//! nobody has seen fail is not evidence.
//!
//! # Where the expected values come from
//!
//! **Not from this crate.** `hmac_sha256` is checked against RFC 4231's seven published HMAC-SHA256
//! test cases, cross-read against CPython's `hmac`/`hashlib` (which is OpenSSL's) — two sources
//! that agree and neither of which is the code under test. That is the same instrument
//! `provenance/sha256.rs` uses for FIPS 180-4, and for the same reason: a MAC that agrees with
//! itself proves nothing at all.

use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::*;
use crate::consensus::transport::{
    decode, decode_verified, encode, encode_signed, Transport, TransportOptions,
};
use crate::consensus::config::Config;
use crate::consensus::node::{Node, NodeOptions, RecordingApplier};
use crate::consensus::{Body, Message, NodeId};
use crate::provenance::sha256::{from_hex, sha256, to_hex};
use crate::replication::{read_handshake, write_handshake, CONSENSUS_TAG, MAX_FRAME_BYTES};

// ---------------------------------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------------------------------

/// 32 distinguishable bytes. Not random: a test that draws a key cannot report which key failed.
fn key_bytes(seed: u8) -> Vec<u8> {
    (0..MIN_KEY_BYTES as u8).map(|i| i.wrapping_mul(7).wrapping_add(seed)).collect()
}

fn a_key(seed: u8) -> Key {
    Key::from_bytes_for_test(key_bytes(seed)).expect("32 bytes is the minimum")
}

/// A message with a term a test can move. `RequestVote` because raising the term of one is exactly
/// the attack this row exists to close.
fn vote_at(term: u64) -> Message {
    Message {
        from: NodeId(2),
        to: NodeId(1),
        term,
        body: Body::RequestVote { last_term: 3, last_round: 9 },
    }
}

/// Write a key file, owner-readable only where that is expressible.
fn write_key_file(dir: &Path, name: &str, bytes: &[u8]) -> PathBuf {
    let p = dir.join(name);
    std::fs::write(&p, bytes).unwrap();
    #[cfg(unix)]
    chmod(&p, 0o600);
    p
}

#[cfg(unix)]
fn chmod(path: &Path, mode: u32) {
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode)).unwrap();
}

/// Load in the way that reaches the rule a test names, on every platform.
///
/// On Unix this is exactly [`Key::load`]. On Windows `Key::load` refuses at
/// [`PROTECTION_IS_CHECKABLE`] before any other rule is reached, so a test of the *length* rule
/// there would otherwise be asserting the platform refusal instead of the rule in its own name.
/// The platform refusal has its own tests below; this is how the others stay about their subject.
fn load_for_rule_under_test(path: &Path) -> Result<Key, FerroError> {
    let check = if PROTECTION_IS_CHECKABLE {
        PermissionCheck::Enforce
    } else {
        PermissionCheck::AcceptUnverifiable
    };
    Key::load_with(path, check)
}

// ---------------------------------------------------------------------------------------------
// The primitive, against published vectors
// ---------------------------------------------------------------------------------------------

/// RFC 4231 §4, test cases 1-7. Key lengths 4, 20, 25 and 131 bytes — the last two of those
/// straddle SHA-256's 64-byte block, which is where the two branches of RFC 2104's key
/// preparation live.
///
/// `(name, key hex, data, expected tag hex)`.
fn rfc_4231() -> Vec<(&'static str, String, Vec<u8>, &'static str)> {
    vec![
        (
            "4.2 test case 1",
            "0b".repeat(20),
            b"Hi There".to_vec(),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7",
        ),
        (
            "4.3 test case 2 (a key shorter than the tag)",
            to_hex(b"Jefe"),
            b"what do ya want for nothing?".to_vec(),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843",
        ),
        (
            "4.4 test case 3",
            "aa".repeat(20),
            vec![0xdd; 50],
            "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe",
        ),
        (
            "4.5 test case 4",
            (1u8..=25).map(|b| format!("{b:02x}")).collect::<String>(),
            vec![0xcd; 50],
            "82558a389a443c0ea4cc819899f2083a85f0faa3e578f8077a2e3ff46729665b",
        ),
        (
            "4.6 test case 5",
            "0c".repeat(20),
            b"Test With Truncation".to_vec(),
            "a3b6167473100ee06e0c796c2955552bfa6f7c0a6a8aef8b93f860aab0cd20c5",
        ),
        (
            "4.7 test case 6 (key longer than the block)",
            "aa".repeat(131),
            b"Test Using Larger Than Block-Size Key - Hash Key First".to_vec(),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54",
        ),
        (
            "4.8 test case 7 (key and data both longer than the block)",
            "aa".repeat(131),
            b"This is a test using a larger than block-size key and a larger than block-size data. \
              The key needs to be hashed before being used by the HMAC algorithm."
                .to_vec(),
            "9b09ffa71b942fcb27635fbcd5b0e944bfdc63644f0713938a7f51535c3a35e2",
        ),
    ]
}

#[test]
fn hmac_sha256_agrees_with_rfc_4231() {
    // THE instrument for this module. Every other test here checks a *relation* between two things
    // this file computed; only this one checks the absolute value, and it checks it against
    // numbers published outside this repository. The mutant that matters is any change to the
    // construction at all — swapping the pads, dropping the outer hash, forgetting to pad a short
    // key to the block — and all of them fail here.
    //
    // Test case 7's data has an embedded line continuation, so its exact bytes are asserted first:
    // a vector whose input is not what the RFC says is a vector that proves nothing about the
    // digest it names.
    let cases = rfc_4231();
    assert_eq!(cases.len(), 7, "RFC 4231 §4 has seven HMAC-SHA-256 cases");
    assert_eq!(cases[6].2.len(), 152, "RFC 4231 test case 7's data is 152 bytes");
    assert_eq!(cases[5].2.len(), 54, "RFC 4231 test case 6's data is 54 bytes");

    for (name, key_hex, data, want) in cases {
        let key = from_hex(&key_hex).expect("the vector's key is hex");
        let got = to_hex(&hmac_sha256(&key, &data));
        assert_eq!(got, want, "RFC 4231 {name}: key {} bytes, data {} bytes", key.len(), data.len());
    }
}

#[test]
fn a_key_longer_than_the_block_is_replaced_by_its_own_digest() {
    // RFC 2104's key-preparation rule, as an identity rather than as a constant: for any key over
    // 64 bytes, HMAC(k, m) is HMAC(sha256(k), m). Test cases 6 and 7 above pin the absolute values
    // for one such key; this pins the *rule* for keys the RFC does not enumerate, including the
    // 65-byte one just over the boundary where an off-by-one lives.
    for len in [65usize, 64, 63, 100, 200] {
        let long: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let folded = sha256(&long);
        let m = b"a message";
        if len > 64 {
            assert_eq!(
                hmac_sha256(&long, m),
                hmac_sha256(&folded, m),
                "a {len}-byte key must be folded to its digest before use"
            );
        } else {
            assert_ne!(
                hmac_sha256(&long, m),
                hmac_sha256(&folded, m),
                "a {len}-byte key fits the block and must NOT be folded"
            );
        }
    }
}

#[test]
fn key_tag_agrees_with_an_independent_hmac_over_the_same_bytes() {
    // **The whole tag, cross-checked against something that is not this crate.**
    //
    // Everything else about `Key::tag` is a relation between two values this file computed:
    // `hashing_the_pieces_is_hashing_their_join` proves the streamed form equals the joined form,
    // but both sides of that are ferrodb's own SHA-256, so a bug shared by both would satisfy it.
    // `hmac_sha256_agrees_with_rfc_4231` pins the primitive against published vectors, but says
    // nothing about the domain prefix or about how `Key::tag` assembles its input.
    //
    // These digests come from CPython's `hmac`/`hashlib` — OpenSSL's — computed over the exact
    // byte string `DOMAIN || body`:
    //
    //     python3 -c "import hmac,hashlib
    //     D=b'ferrodb/consensus/mac/1\x00C\x02'
    //     k=bytes(((i*7)+60)%256 for i in range(32))
    //     b=bytes(i%251 for i in range(N))
    //     print(hmac.new(k, D+b, hashlib.sha256).hexdigest())"
    //
    // The lengths straddle SHA-256's 64-byte block, which is where a streaming bug in the
    // multi-part update would live: 63/64/65 and 127/128/129 sit either side of the two boundaries
    // the domain prefix shifts the body across.
    let k = Key::from_bytes_for_test(key_bytes(60)).unwrap();
    assert_eq!(
        to_hex(&key_bytes(60)),
        "3c434a51585f666d747b828990979ea5acb3bac1c8cfd6dde4ebf2f900070e15",
        "the key these digests were computed over"
    );
    let cases: [(usize, &str); 9] = [
        (0, "34f5383437fb09372131512f63085339f6bb0bfb159695f4a3106ec7e481e2a5"),
        (37, "bec233790ac961983893581f26bb79402a50744e722813526aa7a37d9ff9d554"),
        (63, "56ab9acacd305cc2bdae3db02d022a1b8b4a932281a935603c8de45bd28d616e"),
        (64, "5ac6bd970f7810de6d4832752f37b3c99c5d258fc55f2de24fe36ea7c255bdb9"),
        (65, "4e8b7ca6dabfc97fbd5683227a637e6236503539522ed976c7173c8dffe49ce3"),
        (127, "c6fd98b74c706dfdbda90ce1f9cc3159c17955c9e6eab001d67e6a9c7be4a97b"),
        (128, "39a6aaadba03ad5e996325ef97f1da4acf54872c1cefaf0d9e5ecddabd9dbec6"),
        (129, "7af0fe1ecea97284327d56335d4f89268c495b2f043430bf183c3dd46b4f03e5"),
        (1000, "8178b27652b8109ecc637879db92e879720413766433435f5c86dbf8be0b164e"),
    ];
    for (n, want) in cases {
        let body: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
        assert_eq!(to_hex(&k.tag(&body)), want, "Key::tag over a {n}-byte body");
    }
}

#[test]
fn hashing_the_pieces_is_hashing_their_join() {
    // `Key::tag` streams `DOMAIN` and then the body into one hash rather than joining them, to
    // avoid copying the whole frame on every sign and every verify. This is the assertion that the
    // optimisation changed nothing: the streamed value is the joined value, at every length that
    // straddles SHA-256's 64-byte block.
    let k = key_bytes(29);
    for n in [0usize, 1, 37, 63, 64, 65, 127, 128, 129, 1000] {
        let body: Vec<u8> = (0..n).map(|i| (i % 251) as u8).collect();
        let mut joined = DOMAIN.to_vec();
        joined.extend_from_slice(&body);
        assert_eq!(
            Key::from_bytes_for_test(k.clone()).unwrap().tag(&body),
            hmac_sha256(&k, &joined),
            "a {n}-byte body: streaming the domain and the body must equal hashing their join"
        );
    }
}

#[test]
fn the_tag_is_not_sha256_of_the_key_followed_by_the_message() {
    // The construction, pinned as a rule and not merely as a set of vectors. `sha256(key || m)` is
    // the MAC somebody writes when they have a hash and want a tag, and it is length-extendable:
    // its output IS the internal state, so a tag over `m` yields a tag over `m || padding || suffix`
    // with no key at all. HMAC's nesting is what stops that, and this is the assertion that fails
    // if a later edit "simplifies" the two hashes into one.
    let k = key_bytes(1);
    for m in [b"".as_slice(), b"x".as_slice(), b"a longer consensus frame body".as_slice()] {
        let mut naive = Vec::new();
        naive.extend_from_slice(&k);
        naive.extend_from_slice(m);
        assert_ne!(hmac_sha256(&k, m), sha256(&naive), "the tag must not be sha256(key || message)");
    }
}

#[test]
fn flipping_one_bit_of_the_message_changes_the_tag() {
    let k = key_bytes(2);
    let base = b"append prev_round=7 prev_term=3 commit=6".to_vec();
    let want = hmac_sha256(&k, &base);
    for byte in 0..base.len() {
        for bit in 0..8 {
            let mut m = base.clone();
            m[byte] ^= 1 << bit;
            assert_ne!(hmac_sha256(&k, &m), want, "bit {bit} of byte {byte} did not change the tag");
        }
    }
}

#[test]
fn flipping_one_bit_of_the_key_changes_the_tag() {
    // The half that says the key is load-bearing. A construction that used the key only to seed
    // something it then discarded would pass the message-avalanche test above and fail here.
    let base = key_bytes(3);
    let m = b"a consensus frame";
    let want = hmac_sha256(&base, m);
    for byte in 0..base.len() {
        for bit in 0..8 {
            let mut k = base.clone();
            k[byte] ^= 1 << bit;
            assert_ne!(hmac_sha256(&k, m), want, "bit {bit} of key byte {byte} did not change the tag");
        }
    }
}

// ---------------------------------------------------------------------------------------------
// Domain separation
// ---------------------------------------------------------------------------------------------

#[test]
fn the_domain_names_this_protocol_its_frame_tag_and_its_wire_version() {
    assert_eq!(&DOMAIN[..24], b"ferrodb/consensus/mac/1\0", "the domain label");
    assert_eq!(DOMAIN[24], CONSENSUS_TAG, "the frame tag is inside the domain");
    assert_eq!(
        DOMAIN[25], crate::replication::REPL_VERSION as u8,
        "the wire version is inside the domain, so a v2 tag is not a v3 tag"
    );
}

#[test]
fn a_tag_is_domain_separated_from_a_bare_hmac_over_the_same_bytes() {
    // Without this, a key file shared with any other ferrodb protocol that also HMACs its frames
    // would let a tag from one be presented as a tag for the other.
    let k = a_key(4);
    let m = b"from|to|term|kind";
    assert_ne!(
        k.tag(m),
        hmac_sha256(&key_bytes(4), m),
        "the tag must cover the domain prefix, not the message alone"
    );

    // And the version byte inside the domain must participate, or "a v2 tag is not a v3 tag" is
    // a comment rather than a property.
    let mut other = DOMAIN;
    other[25] = other[25].wrapping_add(1);
    let mut primed = other.to_vec();
    primed.extend_from_slice(m);
    assert_ne!(k.tag(m), hmac_sha256(&key_bytes(4), &primed), "the version byte must change the tag");
}

// ---------------------------------------------------------------------------------------------
// Constant time
// ---------------------------------------------------------------------------------------------

#[test]
fn constant_time_eq_answers_correctly_at_every_difference_position() {
    let a = [0x5au8; MAC_LEN];
    assert!(constant_time_eq(&a, &a), "equal inputs must compare equal");
    for i in 0..MAC_LEN {
        let mut b = a;
        b[i] ^= 0x01;
        assert!(!constant_time_eq(&a, &b), "a difference at byte {i} must not compare equal");
    }
}

#[test]
fn constant_time_eq_refuses_different_lengths() {
    assert!(!constant_time_eq(b"abc", b"abcd"));
    assert!(!constant_time_eq(b"abcd", b"abc"));
    assert!(constant_time_eq(b"", b""));
}

#[test]
fn constant_time_eq_does_not_short_circuit() {
    // **The detector, and it has been fired.** Replacing the body of `constant_time_eq` with
    // `a == b` makes this test fail by three orders of magnitude — see the mutant table.
    //
    // Why a large buffer rather than the 32 bytes a tag actually is: at 32 bytes a short-circuiting
    // memcmp and a full scan are both a handful of nanoseconds and the difference is noise, so a
    // timing test there would pass against the very implementation it exists to reject — vacuous,
    // in the exact way this project keeps warning about. At 1 MiB an early exit is microseconds and
    // a full scan is milliseconds, and the gap is not something load can manufacture.
    //
    // The statistic is the MINIMUM over repetitions, not the mean: noise on a loaded machine only
    // ever adds time, so the minimum is the robust estimator here and an unlucky scheduling slice
    // cannot make this flaky. The threshold is 4x against an expected ~1000x.
    const N: usize = 1 << 20;
    const REPS: usize = 15;

    let a = vec![0x11u8; N];
    let mut differs_first = a.clone();
    differs_first[0] ^= 0xff;
    let mut differs_last = a.clone();
    differs_last[N - 1] ^= 0xff;

    let mut best_first = Duration::MAX;
    let mut best_last = Duration::MAX;
    for _ in 0..REPS {
        let t = Instant::now();
        assert!(!constant_time_eq(&a, &differs_first));
        best_first = best_first.min(t.elapsed());

        let t = Instant::now();
        assert!(!constant_time_eq(&a, &differs_last));
        best_last = best_last.min(t.elapsed());
    }

    assert!(
        best_first * 4 >= best_last,
        "a difference in the FIRST byte was answered in {best_first:?} and one in the LAST in \
         {best_last:?}, over {N} bytes. That is an early exit: the comparison's duration reports \
         how many leading bytes of a guess were right, which turns forging a {MAC_LEN}-byte tag \
         from 2^256 work into {MAC_LEN} x 256."
    );
}

#[test]
fn verify_is_the_constant_time_comparison_and_not_a_slice_equality() {
    // The wiring: `Key::verify` must go through `constant_time_eq`. Behaviourally it can only be
    // observed as correctness, which is what is asserted; the timing property belongs to the
    // function above and is asserted there. Stated rather than implied, so a reader does not
    // mistake this for a timing test.
    let k = a_key(5);
    let m = b"a frame body";
    let good = k.tag(m);
    assert!(k.verify(m, &good));
    for i in 0..MAC_LEN {
        let mut bad = good;
        bad[i] ^= 0x80;
        assert!(!k.verify(m, &bad), "a tag differing at byte {i} must not verify");
    }
    assert!(!k.verify(m, &good[..MAC_LEN - 1]), "a short tag must not verify");
    assert!(!k.verify(b"another body", &good), "a tag over other bytes must not verify");
}

// ---------------------------------------------------------------------------------------------
// The key file
// ---------------------------------------------------------------------------------------------

#[test]
fn a_key_file_under_thirty_two_bytes_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    for n in [0usize, 1, 16, 31] {
        let p = write_key_file(dir.path(), &format!("k{n}"), &vec![0xabu8; n]);
        let err = load_for_rule_under_test(&p)
            .err()
            .unwrap_or_else(|| panic!("a {n}-byte key must be refused"));
        let text = err.to_string();
        assert!(text.contains(&format!("is {n} byte(s)")), "the error must name the length: {text}");
        assert!(text.contains("32"), "the error must name the minimum: {text}");
        // The bytes themselves must never reach an error message, a log or a panic.
        assert!(!text.contains("ab"), "the error must not carry the key's bytes: {text}");
    }
    let p = write_key_file(dir.path(), "ok", &key_bytes(6));
    assert_eq!(load_for_rule_under_test(&p).unwrap().len(), 32, "exactly 32 bytes is accepted");
}

#[test]
fn a_key_file_of_zeros_is_refused_because_that_is_what_a_failed_generator_leaves() {
    // **Found by an adversarial pass, and the length rule alone could not see it.** A file of
    // exactly 32 zero bytes is what `truncate -s 32`, a sparse copy, or a key-generation script
    // that wrote nothing and exited 0 leaves behind. It passes every other check here, and every
    // node given it agrees with every other one — so the cluster comes up, signs, verifies, and
    // looks healthy while its key is a value nobody chose and anybody can guess.
    let dir = tempfile::tempdir().unwrap();
    for n in [32usize, 33, 64, 100] {
        let p = write_key_file(dir.path(), &format!("z{n}"), &vec![0u8; n]);
        let err = load_for_rule_under_test(&p)
            .err()
            .unwrap_or_else(|| panic!("a {n}-byte file of zeros must be refused"));
        let text = err.to_string();
        assert!(text.contains("zero bytes"), "the error must name what it saw: {text}");
        assert!(text.contains("/dev/urandom"), "the error must name the fix: {text}");
    }

    // The anti-vacuity half: one non-zero byte anywhere makes it a key again, so this rule refuses
    // the failed-generator shape and not "keys that contain zeros".
    for at in [0usize, 1, 16, 31] {
        let mut bytes = vec![0u8; 32];
        bytes[at] = 1;
        let p = write_key_file(dir.path(), &format!("nz{at}"), &bytes);
        load_for_rule_under_test(&p)
            .unwrap_or_else(|e| panic!("a key with a non-zero byte at {at} must load: {e}"));
    }
}

#[cfg(unix)]
#[test]
fn a_fifo_named_as_a_key_is_refused_rather_than_hanging_the_node() {
    // **Found by an adversarial pass.** Opening a FIFO for reading blocks until a writer appears,
    // so a node configured with one as its key path used to hang at startup for ever — the
    // `is_file()` check sat on the descriptor, and control never reached it. Refusing by name
    // before the open is what makes this a refusal instead of a silent hang.
    //
    // The test would hang rather than fail if the rule were removed, so it carries its own
    // deadline: the load runs on a thread and this asserts it finished. A test that hangs is a CI
    // job that times out with no message, which is barely better than the bug.
    let dir = tempfile::tempdir().unwrap();
    let fifo = dir.path().join("k");
    let status = std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .expect("mkfifo must be runnable to test this");
    assert!(status.success(), "mkfifo failed");

    let p = fifo.clone();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(Key::load(&p).is_err());
    });
    let refused = rx
        .recv_timeout(Duration::from_secs(10))
        .expect("Key::load blocked on a FIFO instead of refusing it — a node pointed at one hangs");
    assert!(refused, "a FIFO is not a regular file and must be refused");
}

#[test]
fn a_missing_key_file_is_refused_rather_than_leaving_the_node_unsigned() {
    let dir = tempfile::tempdir().unwrap();
    let err = load_for_rule_under_test(&dir.path().join("nothing-here")).expect_err("must refuse");
    assert!(err.to_string().contains("nothing-here"), "the error must name the path: {err}");
}

#[test]
fn a_directory_named_where_a_key_was_expected_is_refused() {
    // Not pedantry: reading a directory or a device would produce a key whose bytes nobody chose,
    // so the shape is refused before anything is read.
    //
    // The two platforms refuse it at different points and the assertion says so rather than
    // pretending one message: `File::open` on a directory SUCCEEDS on Unix and fails on Windows, so
    // on Unix the refusal comes from the `is_file` check on the open handle and on Windows from the
    // open itself. Both refuse; only Unix can name the reason.
    let dir = tempfile::tempdir().unwrap();
    let sub = dir.path().join("sub");
    std::fs::create_dir(&sub).unwrap();
    let err = load_for_rule_under_test(&sub).expect_err("a directory is not a key");
    let text = err.to_string();
    assert!(text.contains("sub"), "the error must name the path it refused: {text}");
    if cfg!(unix) {
        assert!(text.contains("not a regular file"), "{text}");
    }
}

#[cfg(unix)]
#[test]
fn a_key_file_readable_by_group_or_other_is_refused() {
    // **The rule, and it has been fired**: deleting the `mode & 0o077` branch lets every mode in
    // the second loop below load, and this test fails on the first of them. See the mutant table.
    let dir = tempfile::tempdir().unwrap();
    let p = write_key_file(dir.path(), "k", &key_bytes(8));
    for mode in [0o600u32, 0o400, 0o700] {
        chmod(&p, mode);
        Key::load(&p).unwrap_or_else(|e| panic!("mode {mode:04o} is owner-only and must load: {e}"));
    }
    for mode in [0o640u32, 0o604, 0o644, 0o660, 0o666, 0o777, 0o601, 0o610] {
        chmod(&p, mode);
        let err = Key::load(&p)
            .err()
            .unwrap_or_else(|| panic!("mode {mode:04o} is readable beyond its owner and must be refused"));
        let text = err.to_string();
        assert!(
            text.contains(&format!("mode {mode:04o}")),
            "the error must name the mode it saw, so an operator can act on it: {text}"
        );
        assert!(text.contains("chmod 600"), "the error must name the fix: {text}");
    }
}

#[cfg(unix)]
#[test]
fn a_key_in_a_group_or_world_writable_directory_is_refused_unless_it_is_sticky() {
    // A key an attacker can REPLACE is a key they hold: the file's own mode does not help, because
    // the attacker never needs to read it. The sticky bit is the exception and not a special case —
    // on a sticky directory only a file's owner may rename or remove it, which is the property
    // being checked for. This is OpenSSH's StrictModes rule.
    let outer = tempfile::tempdir().unwrap();

    let open = outer.path().join("open");
    std::fs::create_dir(&open).unwrap();
    let p = write_key_file(&open, "k", &key_bytes(9));
    chmod(&open, 0o777);
    let err = Key::load(&p).expect_err("a key in a world-writable directory must be refused");
    let text = err.to_string();
    assert!(text.contains("writable by group or other"), "{text}");
    assert!(text.contains("chmod go-w"), "the error must name the fix: {text}");

    chmod(&open, 0o1777);
    Key::load(&p).expect("a sticky world-writable directory protects the file from replacement");

    chmod(&open, 0o700);
    Key::load(&p).expect("a private directory is fine");
}

#[cfg(unix)]
#[test]
fn the_spelling_of_the_path_does_not_decide_whether_the_directory_is_checked() {
    // **Found by an adversarial pass, and it was a real hole.** `Path::parent()` of a bare relative
    // name is `Some("")`, which means the CURRENT directory — not "there is no directory". The
    // guard filtered the empty parent out as "nothing to check", so `Key::load("cluster.key")`
    // skipped the directory check entirely while `Key::load("./cluster.key")` performed it, for the
    // same file in the same directory. A guard that quietly declines to run returns the same `Ok`
    // as one that ran and passed, which is the failure shape this module warns about elsewhere.
    //
    // Asserted as an EQUIVALENCE rather than as two verdicts, so it cannot be satisfied by making
    // both spellings skip the check: the third assertion pins that the shared verdict is a refusal.
    assert_eq!(Path::new("cluster.key").parent(), Some(Path::new("")), "the mechanism");
    assert_eq!(Path::new("./cluster.key").parent(), Some(Path::new(".")), "the mechanism");

    let dir = tempfile::tempdir().unwrap();
    let dirp = dir.path().canonicalize().unwrap();
    write_key_file(&dirp, "cluster.key", &key_bytes(40));
    chmod(&dirp, 0o777);

    // `set_current_dir` is process-global and cargo runs tests in threads, so the CWD is restored
    // before anything else can observe it and no assertion happens while it is moved.
    let restore = std::env::current_dir().unwrap();
    std::env::set_current_dir(&dirp).unwrap();
    let bare = Key::load("cluster.key");
    let dotted = Key::load("./cluster.key");
    std::env::set_current_dir(&restore).unwrap();
    chmod(&dirp, 0o700);

    assert_eq!(
        bare.is_err(),
        dotted.is_err(),
        "two spellings of one path must reach one verdict; bare={:?} dotted={:?}",
        bare.as_ref().err().map(|e| e.to_string()),
        dotted.as_ref().err().map(|e| e.to_string())
    );
    let err = bare.err().expect("a key in a world-writable non-sticky directory must be refused");
    assert!(err.to_string().contains("writable by group or other"), "{err}");
}

#[cfg(unix)]
#[test]
fn a_key_whose_directory_is_gone_is_refused() {
    // What this proves: the disappearance of the key's directory produces a refusal and never an
    // `Ok`. The refusal comes from the `File::open`, which is the first thing that fails.
    //
    // **A claim that was made here and was wrong, corrected rather than quietly dropped.** This
    // comment used to say the other half of the same defect — the directory `stat` failing and
    // falling through to "allowed" — could have no killing test, on the argument that `File::open`
    // resolves the path first so anything breaking the stat breaks the open. An adversarial pass
    // refuted that by *racing* it: renaming the parent directory back and forth in a second thread
    // while loading a 0600 key from a 0777 directory produced **46,895 successful loads** of a key
    // the rule refuses. The branch is reachable; a deterministic test for it is what is missing,
    // not the reachability. `signing.rs` now refuses there, and the reversal is logged in
    // `scratchpad/F7-signing.md` beside the original claim.
    let outer = tempfile::tempdir().unwrap();
    let inner = outer.path().join("gone");
    std::fs::create_dir(&inner).unwrap();
    let p = write_key_file(&inner, "k", &key_bytes(41));
    Key::load(&p).expect("it loads while its directory is there");

    std::fs::remove_file(&p).unwrap();
    std::fs::remove_dir(&inner).unwrap();
    let err = Key::load(&p).expect_err("a key whose directory is gone must be refused");
    assert!(err.to_string().contains("gone"), "the error must name the path: {err}");
}

#[cfg(unix)]
#[test]
fn a_symlink_cannot_launder_a_key_out_of_a_world_writable_directory() {
    // **The best finding of this row's adversarial review, reproduced.** The mode check reads the
    // OPEN DESCRIPTOR, so it sees the target's mode and is correct. The directory check read
    // `path.parent()` — the *symlink's* parent — and never the directory the inode actually sits
    // in. So a link in a 0700 directory pointing at a key in a 0777 one passed, and the whole
    // directory rule was bypassed by naming the key differently.
    //
    // Driven to the end, because "the verdict is wrong" understates it: the consequence is that an
    // attacker who can write to the target's directory renames their own key over it, the node
    // reloads happily, and then verifies frames the ATTACKER signed. That is total compromise of
    // this module's guarantee, reached without ever reading the operator's key.
    let root = tempfile::tempdir().unwrap();
    let open = root.path().join("open");
    let safe = root.path().join("safe");
    std::fs::create_dir(&open).unwrap();
    std::fs::create_dir(&safe).unwrap();

    let real = write_key_file(&open, "k", b"THE-OPERATORS-REAL-CLUSTER-KEY!!");
    let link = safe.join("k");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    chmod(&open, 0o777);
    chmod(&safe, 0o700);

    // The direct path is refused, and that half always worked.
    let direct = Key::load(&real).expect_err("a key in a 0777 directory is refused by name");
    assert!(direct.to_string().contains("writable by group or other"), "{direct}");

    // The link must reach the SAME verdict. Before the fix it returned `Ok`.
    let through_link = Key::load(&link);
    assert!(
        through_link.is_err(),
        "the same inode, in the same 0777 directory, loaded because it was named through a link \
         in a 0700 one — the directory rule was bypassed by spelling"
    );

    // The anti-vacuity half: with the target's directory closed, the link loads. Otherwise this
    // test would pass against an implementation that simply refused every symlink.
    chmod(&open, 0o700);
    let ok = Key::load(&link).expect("a link to a key in a protected directory is fine");
    assert_eq!(ok.len(), 32);

    // And the attack the rule exists to stop, shown to be a real substitution rather than a
    // theoretical one: with the directory open, an attacker's rename replaces the key under the
    // operator's configured path.
    chmod(&open, 0o777);
    let theirs = open.join(".theirs");
    std::fs::write(&theirs, b"ATTACKER-CHOSEN-CLUSTER-KEY!!!!!").unwrap();
    chmod(&theirs, 0o600);
    std::fs::rename(&theirs, &real).unwrap();
    assert!(
        Key::load(&link).is_err(),
        "after an attacker renamed their own key over the target, loading through the configured \
         path must still refuse — otherwise the node signs and verifies with the attacker's key"
    );
    chmod(&open, 0o700);
}

#[cfg(unix)]
#[test]
fn no_hop_of_the_symlink_chain_escapes_the_directory_rule() {
    // **This broke my first fix, deterministically, and an adversarial pass found it.** Checking
    // the path as given and the canonicalised path checks the two ENDPOINTS of resolution and no
    // hop in between — and `canonicalize` collapses the chain, so the middle is invisible to it:
    //
    //     safe/cluster.key   parent 0700   <- the path as given: checked
    //       -> open/k        parent 0777   <- checked by NOBODY
    //         -> known       parent 0700   <- the canonicalised path: checked
    //
    // What makes it worse than a wrong verdict is that **the attacker never authors a key file**.
    // They rename a symlink over the middle name, pointing it at a file the node can already read,
    // and the mode check passes because it inspects that file's own 0600. Nothing about ownership
    // or mode can catch that; only the directory rule can, and it was not running on `open/`.
    let root = tempfile::tempdir().unwrap();
    let safe = root.path().join("safe");
    let open = root.path().join("open");
    let other = root.path().join("other");
    for d in [&safe, &open, &other] {
        std::fs::create_dir(d).unwrap();
    }

    // `known` is any file the node can already read: node-owned, 0600, in a private directory.
    let known = write_key_file(&other, "known", &key_bytes(53));
    let middle = open.join("k");
    std::os::unix::fs::symlink(&known, &middle).unwrap();
    let entry = safe.join("cluster.key");
    std::os::unix::fs::symlink(&middle, &entry).unwrap();

    chmod(&safe, 0o700);
    chmod(&other, 0o700);

    chmod(&open, 0o700);
    let ok = Key::load(&entry).expect("every hop closed: the chain loads");
    assert_eq!(ok.tag(b"probe"), a_key(53).tag(b"probe"), "and it is the key at the end of it");

    // The only change is the MIDDLE hop's directory. Both endpoints are still 0700.
    chmod(&open, 0o777);
    let err = Key::load(&entry).expect_err(
        "the middle hop sits in a 0777 directory: an attacker who can write there repoints it at \
         any file this node can read, without ever authoring a key",
    );
    assert!(err.to_string().contains("writable by group or other"), "{err}");
    assert!(
        err.to_string().contains("open"),
        "the error must name the directory that is open, not one of the safe endpoints: {err}"
    );
    chmod(&open, 0o700);
}

#[cfg(unix)]
#[test]
fn a_symlinked_directory_component_is_judged_by_what_it_points_at() {
    // A DIRECTORY in the path being a symlink, rather than the key itself. `check_directory` uses
    // `fs::metadata`, which follows, so it judges the directory the component resolves to and not
    // the link. Asserted rather than assumed: "metadata follows symlinks" is exactly the kind of
    // thing that is true until somebody reaches for `symlink_metadata` for consistency with the
    // walk above, where NOT following is the point.
    let root = tempfile::tempdir().unwrap();
    let real = root.path().join("real");
    std::fs::create_dir(&real).unwrap();
    let via = root.path().join("via");
    std::os::unix::fs::symlink(&real, &via).unwrap();
    write_key_file(&real, "k", &key_bytes(54));
    let through = via.join("k");

    chmod(&real, 0o700);
    Key::load(&through).expect("a link to a closed directory is fine");

    chmod(&real, 0o777);
    let err = Key::load(&through).expect_err("the directory it POINTS AT is 0777");
    assert!(err.to_string().contains("writable by group or other"), "{err}");
    chmod(&real, 0o700);
}

#[cfg(unix)]
#[test]
fn a_relative_link_target_resolves_against_the_links_own_directory() {
    // A relative target must resolve against the directory the LINK sits in, never the process's
    // current directory — otherwise the walk inspects a directory that has nothing to do with the
    // chain, and reports a verdict about the wrong filesystem location entirely. `..` in the target
    // is included because that is how a real relative link reaches a sibling tree.
    let root = tempfile::tempdir().unwrap();
    let links = root.path().join("links");
    let store = root.path().join("store");
    std::fs::create_dir(&links).unwrap();
    std::fs::create_dir(&store).unwrap();
    write_key_file(&store, "k", &key_bytes(55));

    let entry = links.join("k");
    std::os::unix::fs::symlink("../store/k", &entry).unwrap();
    chmod(&links, 0o700);

    chmod(&store, 0o700);
    Key::load(&entry).expect("a relative link into a closed directory is fine");

    // Only the directory the RELATIVE target lands in changes.
    chmod(&store, 0o777);
    let err = Key::load(&entry).expect_err("the directory the relative target lands in is 0777");
    assert!(err.to_string().contains("writable by group or other"), "{err}");
    assert!(err.to_string().contains("store"), "the error must name where the target lands: {err}");
    chmod(&store, 0o700);
}

#[cfg(unix)]
#[test]
fn a_symlink_cycle_is_refused_rather_than_followed() {
    // The walk is bounded, so a cycle is a refusal instead of a hang or an ELOOP from somewhere
    // deeper. Named here because the bound is the thing that makes the walk safe to write at all.
    let dir = tempfile::tempdir().unwrap();
    let a = dir.path().join("a");
    let b = dir.path().join("b");
    std::os::unix::fs::symlink(&b, &a).unwrap();
    std::os::unix::fs::symlink(&a, &b).unwrap();
    let err = Key::load(&a).expect_err("a symlink cycle must be refused");
    let text = err.to_string();
    assert!(
        text.contains("cycle") || text.contains("could not be inspected"),
        "the refusal must say what it met: {text}"
    );
}

#[cfg(unix)]
#[test]
fn a_symlink_whose_own_directory_is_open_is_also_refused() {
    // The other direction of the same rule, and the reason both directories are checked rather
    // than just the resolved one: whoever can write to the LINK's directory can repoint the link
    // at a key they chose, without ever touching the target's directory.
    let root = tempfile::tempdir().unwrap();
    let target_dir = root.path().join("target");
    let link_dir = root.path().join("links");
    std::fs::create_dir(&target_dir).unwrap();
    std::fs::create_dir(&link_dir).unwrap();

    let real = write_key_file(&target_dir, "k", &key_bytes(52));
    let link = link_dir.join("k");
    std::os::unix::fs::symlink(&real, &link).unwrap();
    chmod(&target_dir, 0o700);

    chmod(&link_dir, 0o700);
    Key::load(&link).expect("both directories closed: fine");

    chmod(&link_dir, 0o777);
    let err = Key::load(&link).expect_err("a repointable link is a replaceable key");
    assert!(err.to_string().contains("writable by group or other"), "{err}");
    chmod(&link_dir, 0o700);
}

#[test]
fn the_platform_that_cannot_read_its_own_file_protection_refuses_rather_than_allowing() {
    // **Both arms of the Windows rule, run on every platform.** `accept_unverifiable` carries no
    // `cfg` of its own precisely so this test is not one that only one CI runner executes; what is
    // platform-shaped is its single-line wiring to `PROTECTION_IS_CHECKABLE`, asserted below.
    let path = Path::new("/some/key");

    accept_unverifiable(PermissionCheck::AcceptUnverifiable, path)
        .expect("an operator who says they have protected the file is taken at their word");

    let err = accept_unverifiable(PermissionCheck::Enforce, path)
        .expect_err("a build that cannot inspect the file must NOT report it protected");
    let text = err.to_string();
    assert!(text.contains("access control list"), "the error must say what it could not read: {text}");
    assert!(text.contains("icacls"), "the error must name the fix: {text}");
    assert!(
        text.contains("AcceptUnverifiable"),
        "the error must name the explicit opt-out, or an operator has no way forward: {text}"
    );
}

#[test]
fn protection_is_checkable_exactly_where_std_exposes_the_mode_bits() {
    assert_eq!(PROTECTION_IS_CHECKABLE, cfg!(unix), "Unix has mode bits; Windows has an ACL std cannot read");
}

#[cfg(unix)]
#[test]
fn accepting_the_unverifiable_does_not_weaken_the_platform_that_can_verify() {
    // The escape hatch for the platform that cannot check must not become an escape hatch for the
    // platform that can. Where the evidence exists it decides.
    let dir = tempfile::tempdir().unwrap();
    let p = write_key_file(dir.path(), "k", &key_bytes(10));
    chmod(&p, 0o644);
    Key::load_with(&p, PermissionCheck::AcceptUnverifiable)
        .expect_err("on Unix the mode is visible, so no assertion by the operator may override it");
}

#[cfg(not(unix))]
#[test]
fn on_this_platform_load_refuses_and_the_operator_must_say_so_explicitly() {
    // The one thing only the Windows runner can prove: that `check_protection` is wired to
    // `accept_unverifiable`. The decision itself is tested everywhere, above.
    let dir = tempfile::tempdir().unwrap();
    let p = write_key_file(dir.path(), "k", &key_bytes(11));
    let err = Key::load(&p).expect_err("this build cannot show the file is owner-only");
    assert!(err.to_string().contains("access control list"), "{err}");
    Key::load_with(&p, PermissionCheck::AcceptUnverifiable)
        .expect("an explicit acceptance is the documented way forward");
}

#[test]
fn the_key_never_appears_in_a_debug_rendering() {
    // `TransportOptions` derives `Debug` and `Transport` has one; both are printed by failing
    // assertions across this crate's tests. A derived `Debug` on `Key` would put the cluster key
    // into every one of those strings, and into any log line that ever renders one.
    let k = a_key(12);
    let rendered = format!("{k:?}");
    let hex = to_hex(&key_bytes(12));
    assert!(!rendered.contains(&hex), "the key's bytes must not be printable: {rendered}");
    for b in key_bytes(12).chunks(4) {
        assert!(!rendered.contains(&to_hex(b)), "no run of the key may appear: {rendered}");
    }
    assert!(rendered.contains("redacted"), "the redaction must be visible rather than silent: {rendered}");
    assert!(rendered.contains("32"), "the length is the useful part and is kept: {rendered}");
}

// ---------------------------------------------------------------------------------------------
// The frame layer
// ---------------------------------------------------------------------------------------------

/// Sign, then take the frame apart the way a socket reader does.
fn signed_frame(key: &Key, m: &Message) -> Vec<u8> {
    let frame = encode_signed(m, Some(key)).unwrap();
    assert_eq!(frame[0], CONSENSUS_TAG);
    let len = u32::from_be_bytes(frame[1..5].try_into().unwrap()) as usize;
    assert_eq!(frame.len(), 5 + len, "the length covers the body only");
    frame
}

#[test]
fn a_signed_frame_round_trips_and_carries_exactly_one_tag_of_overhead() {
    let k = a_key(20);
    for term in [0u64, 1, 7, u64::MAX] {
        let m = vote_at(term);
        let plain = encode(&m).unwrap();
        let signed = signed_frame(&k, &m);
        assert_eq!(signed.len(), plain.len() + MAC_LEN, "the tag is the only overhead");
        let body = &signed[5..];
        let inner = verify_frame(&k, body).expect("its own tag must verify");
        assert_eq!(inner, &plain[5..], "the authenticated region is the unsigned body, unchanged");
        assert_eq!(decode(inner).unwrap(), m, "and it decodes to the message that was signed");
    }
}

#[test]
fn the_term_is_inside_the_mac() {
    // **THE test for this row.** An attacker who can write to the wire but cannot forge a tag takes
    // a genuine frame and raises its term; every node that accepts it steps down, and a healthy
    // leader is demoted by somebody who holds no key at all.
    //
    // Both halves are here, because the refusal alone would prove nothing if the attack did not
    // work in the first place:
    //   1. the raised term is refused on a signed frame, and
    //   2. the SAME mutation of the SAME frame, unsigned, is accepted and reports the raised term.
    //
    // Fired: signing `from|to|kind|body` with the term left out makes (1) pass verification and
    // this test fail on the first assertion. See the mutant table.
    let k = a_key(21);
    let m = vote_at(7);

    // Frame layout: tag(1) len(4) | mac(32) from(4) to(4) term(8) kind(1) ...
    const TERM_AT: usize = 5 + MAC_LEN + 4 + 4;
    let mut attacked = signed_frame(&k, &m);
    assert_eq!(
        u64::from_be_bytes(attacked[TERM_AT..TERM_AT + 8].try_into().unwrap()),
        7,
        "the term is where this test believes it is"
    );
    attacked[TERM_AT..TERM_AT + 8].copy_from_slice(&99u64.to_be_bytes());

    let err = verify_frame(&k, &attacked[5..]).expect_err("a raised term must not authenticate");
    assert!(err.to_string().contains("did not authenticate"), "{err}");

    // The anti-vacuity half: the identical mutation on the identical message, with no tag, lands.
    let mut plain = encode(&m).unwrap();
    const PLAIN_TERM_AT: usize = 5 + 4 + 4;
    plain[PLAIN_TERM_AT..PLAIN_TERM_AT + 8].copy_from_slice(&99u64.to_be_bytes());
    let got = decode(&plain[5..]).expect("without a tag the raised term is simply believed");
    assert_eq!(got.term, 99, "the attack is real: unsigned, the term is whatever the wire says");
}

#[test]
fn every_byte_of_the_message_is_inside_the_mac() {
    // The general form of the test above: not "the term is covered" but "no field is outside".
    // Written as a sweep rather than a list of fields so that adding a field to `Message` cannot
    // quietly leave it unauthenticated — there is no list here to forget to update.
    let k = a_key(22);
    let m = Message {
        from: NodeId(9),
        to: NodeId(4),
        term: 12,
        body: Body::AppendResp { success: true, matched: 5, hint: 6, digest: 0xdead_beef },
    };
    let frame = signed_frame(&k, &m);
    let body_at = 5 + MAC_LEN;
    for i in body_at..frame.len() {
        let mut bad = frame.clone();
        bad[i] ^= 0x01;
        assert!(
            verify_frame(&k, &bad[5..]).is_err(),
            "flipping a bit of body byte {} left the frame verifying",
            i - body_at
        );
    }
    // And the tag itself is not ignored.
    for i in 5..body_at {
        let mut bad = frame.clone();
        bad[i] ^= 0x01;
        assert!(verify_frame(&k, &bad[5..]).is_err(), "flipping a bit of tag byte {} verified", i - 5);
    }
}

#[test]
fn decode_verified_authenticates_before_it_parses() {
    // The composition `transport` exposes for a caller that holds a whole frame body. With a key it
    // is verify-then-decode; without one it is exactly `decode`, for a transport that was not given
    // a key. Both halves asserted, because "without a key it is exactly decode" is the sentence a
    // later edit could quietly make false.
    let k = a_key(39);
    let m = vote_at(11);
    let signed = signed_frame(&k, &m);
    let plain = encode(&m).unwrap();

    assert_eq!(decode_verified(&signed[5..], Some(&k)).unwrap(), m);
    assert_eq!(decode_verified(&plain[5..], None).unwrap(), m);

    // Crossed over, both directions refuse.
    assert!(decode_verified(&plain[5..], Some(&k)).is_err(), "an unsigned body must not verify");
    assert!(decode_verified(&signed[5..], None).is_err(), "a tag is not a message prefix");

    // And a body that would NOT decode is refused for the authentication, not for the parse: the
    // tag is checked first, so `decode` never runs on it.
    let payload = vec![0xffu8; 8];
    let mut junk = k.tag(&payload).to_vec();
    junk.extend_from_slice(&payload);
    let err = decode_verified(&junk, Some(&k)).expect_err("the payload is not a message");
    assert!(
        !err.to_string().contains("did not authenticate"),
        "a correctly tagged frame must reach the parser: {err}"
    );
}

#[test]
fn a_frame_signed_with_another_key_is_refused() {
    let mine = a_key(23);
    let theirs = a_key(24);
    let frame = signed_frame(&theirs, &vote_at(4));
    let err = verify_frame(&mine, &frame[5..]).expect_err("another key's tag must not verify");
    assert!(err.to_string().contains("did not authenticate"), "{err}");
    verify_frame(&theirs, &frame[5..]).expect("its own key still verifies, so the frame is well-formed");
}

#[test]
fn a_frame_too_short_to_hold_a_tag_is_refused_rather_than_indexed_into() {
    let k = a_key(25);
    for n in 0..MAC_LEN {
        let err = verify_frame(&k, &vec![0u8; n])
            .expect_err("a frame shorter than a tag cannot carry one");
        assert!(err.to_string().contains("cannot hold"), "{err}");
    }
    // Exactly MAC_LEN is a tag over an empty body: well-formed, and refused for the tag, not the
    // length. `decode` would then refuse the empty body — but this layer must get there first.
    let empty = k.tag(&[]);
    assert_eq!(verify_frame(&k, &empty).unwrap(), &[] as &[u8]);
}

#[test]
fn truncating_or_extending_a_signed_frame_is_refused() {
    // The frame's `u32` length header is not itself inside the tag, and does not need to be: the
    // receiver hands `verify_frame` exactly the bytes that length named, so changing it changes the
    // authenticated region. This is the assertion behind that sentence in the module header.
    let k = a_key(26);
    let frame = signed_frame(&k, &vote_at(5));
    let body = &frame[5..];

    for cut in 1..=8 {
        assert!(verify_frame(&k, &body[..body.len() - cut]).is_err(), "a frame short by {cut} verified");
    }
    let mut longer = body.to_vec();
    longer.push(0);
    assert!(verify_frame(&k, &longer).is_err(), "a frame with a byte appended verified");
}

#[test]
fn a_message_that_fits_unsigned_is_refused_signed_rather_than_framed_over_the_limit() {
    // The tag costs 32 bytes of the frame budget, so the largest signed message is 32 bytes smaller
    // than the largest unsigned one. Without this check the sender frames a body of
    // MAX_FRAME_BYTES + 32 and writes a length its peer refuses on sight — a message that vanishes
    // with the error raised on the wrong side of the wire.
    let k = a_key(27);
    let boundary = MAX_FRAME_BYTES - MAC_LEN;

    let ok = sign_frame(&k, &vec![0u8; boundary]).expect("the largest signed body must be accepted");
    assert_eq!(ok.len(), MAX_FRAME_BYTES, "and it is exactly the frame limit");

    for over in [1usize, 2, MAC_LEN] {
        let err = sign_frame(&k, &vec![0u8; boundary + over]).expect_err("one byte over must refuse");
        let text = err.to_string();
        assert!(text.contains("authentication tag"), "the error must name what pushed it over: {text}");
        assert!(text.contains("Send fewer entries"), "the error must name the caller's remedy: {text}");
    }
}

#[test]
fn replay_of_a_valid_frame_is_accepted_because_there_is_no_freshness_check() {
    // **This test asserts a LIMITATION, on purpose.** The module header says a tag proves possession
    // of the key and not that the message is new, and this is the assertion that keeps that
    // sentence true: the same bytes verify twice, and nothing here notices.
    //
    // The day replay protection lands, this test fails. That is the point — it fails at the exact
    // moment the header stops being accurate, so the claim and the code cannot drift apart
    // silently. Whoever makes it fail should delete it and rewrite the header, not weaken it.
    let k = a_key(28);
    let frame = signed_frame(&k, &vote_at(6));
    for attempt in 0..5 {
        verify_frame(&k, &frame[5..])
            .unwrap_or_else(|e| panic!("replay {attempt} was refused, so freshness IS checked now: {e}"));
    }
}

// ---------------------------------------------------------------------------------------------
// The transport hook, over real sockets
// ---------------------------------------------------------------------------------------------

/// The default options with the three timings shortened, written as an update rather than a fresh
/// literal so that a field added to `TransportOptions` later does not have to be repeated here.
fn fast() -> TransportOptions {
    TransportOptions {
        queue_depth: 64,
        poll_interval: Duration::from_millis(5),
        reconnect_delay: Duration::from_millis(5),
        ..Default::default()
    }
}

/// Two transports, each optionally signing. Both listeners are bound before either transport
/// starts, because each peer map needs the other's address.
fn pair(a_key_seed: Option<u8>, b_key_seed: Option<u8>) -> (Transport, Transport) {
    let la = TcpListener::bind("127.0.0.1:0").unwrap();
    let lb = TcpListener::bind("127.0.0.1:0").unwrap();
    let aa = la.local_addr().unwrap();
    let ab = lb.local_addr().unwrap();
    let start = |id: u32, l: TcpListener, peer: u32, addr: SocketAddr, seed: Option<u8>| {
        let peers = BTreeMap::from([(NodeId(peer), addr)]);
        match seed {
            None => Transport::from_listener(NodeId(id), l, peers, fast()).unwrap(),
            Some(s) => Transport::from_listener_with_key(
                NodeId(id),
                l,
                peers,
                fast(),
                Arc::new(a_key(s)),
            )
            .unwrap(),
        }
    };
    (start(1, la, 2, ab, a_key_seed), start(2, lb, 1, aa, b_key_seed))
}

fn expect_recv(t: &Transport, within: Duration) -> Message {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if let Some(m) = t.recv_timeout(Duration::from_millis(20)) {
            return m;
        }
    }
    panic!("no message arrived within {within:?}; received={} unauthenticated={}", t.received(), t.unauthenticated());
}

/// Nothing may arrive within the window, and the reason must be authentication.
fn expect_nothing(t: &Transport, within: Duration) {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if let Some(m) = t.recv_timeout(Duration::from_millis(20)) {
            panic!("a message reached the state machine: {m:?}");
        }
    }
}

#[test]
fn signs_its_traffic_reports_the_configuration() {
    let (signed, unsigned) = pair(Some(30), None);
    assert!(signed.signs_its_traffic(), "a transport built with a key signs");
    assert!(!unsigned.signs_its_traffic(), "one built without a key does not");
    assert_eq!(signed.unauthenticated(), 0, "nothing has been refused yet");
    assert!(
        !format!("{signed:?}").contains(&to_hex(&key_bytes(30))),
        "the transport's Debug must not carry the key"
    );
    signed.shutdown();
    unsigned.shutdown();
}

#[test]
fn two_nodes_holding_the_same_key_exchange_messages() {
    // The anti-vacuity half of every refusal test below: signing must not merely refuse everything.
    let (a, b) = pair(Some(31), Some(31));
    for term in [1u64, 2, 3] {
        a.send(&Message { from: NodeId(1), to: NodeId(2), term, body: Body::PreVoteResp { granted: true } })
            .unwrap();
        let got = expect_recv(&b, Duration::from_secs(5));
        assert_eq!(got.term, term);
        assert_eq!(got.from, NodeId(1));
    }
    assert_eq!(b.unauthenticated(), 0, "a peer with the right key is never refused");
    a.shutdown();
    b.shutdown();
}

#[test]
fn a_node_holding_a_different_key_is_refused_and_counted() {
    let (a, b) = pair(Some(32), Some(33));
    a.send(&Message { from: NodeId(1), to: NodeId(2), term: 9, body: Body::PreVoteResp { granted: true } })
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while b.unauthenticated() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(b.unauthenticated() >= 1, "the refusal must be counted, not silent");
    expect_nothing(&b, Duration::from_millis(200));
    assert_eq!(b.received(), 0, "and nothing reached the caller");
    a.shutdown();
    b.shutdown();
}

#[test]
fn a_peer_that_does_not_sign_is_refused_by_a_node_that_does() {
    // The mixed-cluster direction that matters: a node configured to sign must not accept an
    // unsigned peer, or an attacker's whole job is to omit the tag.
    let (unsigned_sender, signed_receiver) = pair(None, Some(34));
    unsigned_sender
        .send(&Message { from: NodeId(1), to: NodeId(2), term: 4, body: Body::RequestVoteResp { granted: true } })
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    while signed_receiver.unauthenticated() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert!(signed_receiver.unauthenticated() >= 1, "an unsigned frame must be refused and counted");
    assert_eq!(signed_receiver.received(), 0);
    unsigned_sender.shutdown();
    signed_receiver.shutdown();
}

#[test]
fn a_signed_peer_is_refused_by_a_node_with_no_key() {
    // The other direction of the same misconfiguration. It fails too — as the header says it must,
    // because there is no negotiation and no fallback-to-unsigned. Asserted so that "a mixed
    // cluster cannot talk to itself, in both directions" is a tested claim and not a hope.
    let (signed_sender, unsigned_receiver) = pair(Some(35), None);
    signed_sender
        .send(&Message { from: NodeId(1), to: NodeId(2), term: 4, body: Body::RequestVoteResp { granted: true } })
        .unwrap();
    expect_nothing(&unsigned_receiver, Duration::from_millis(500));
    assert_eq!(unsigned_receiver.received(), 0, "a tag it cannot strip is not a message it can read");
    assert_eq!(
        unsigned_receiver.unauthenticated(),
        0,
        "and it is not counted as an authentication failure, because this node checks nothing"
    );
    signed_sender.shutdown();
    unsigned_receiver.shutdown();
}

/// Speak the handshake and hand over one frame, as a peer that is not this crate's transport.
fn raw_send(addr: SocketAddr, frame: &[u8]) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut hs = Vec::new();
    write_handshake(&mut hs).unwrap();
    s.write_all(&hs).unwrap();
    let mut theirs = [0u8; 6];
    s.read_exact(&mut theirs).unwrap();
    read_handshake(&mut &theirs[..]).expect("the node answers a valid handshake");
    s.write_all(frame).unwrap();
    s.flush().unwrap();
    // Held open briefly so the receiving thread reads the frame before the socket closes.
    std::thread::sleep(Duration::from_millis(100));
}

#[test]
fn an_attacker_raising_the_term_on_the_wire_never_reaches_the_state_machine() {
    // The end-to-end form of `the_term_is_inside_the_mac`: a real socket, a real handshake, a real
    // captured frame with one field changed. This is the attack in DISTRIBUTED.md §F7 — "an
    // unauthenticated peer that can speak this protocol can claim a later term and demote a healthy
    // leader" — run against a node that signs.
    let k = a_key(36);
    let genuine = Message {
        from: NodeId(2),
        to: NodeId(1),
        term: 5,
        body: Body::RequestVote { last_term: 4, last_round: 20 },
    };
    let mut frame = encode_signed(&genuine, Some(&k)).unwrap();
    const TERM_AT: usize = 5 + MAC_LEN + 4 + 4;
    frame[TERM_AT..TERM_AT + 8].copy_from_slice(&500u64.to_be_bytes());

    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let victim = Transport::from_listener_with_key(
        NodeId(1),
        listener,
        BTreeMap::new(),
        fast(),
        Arc::new(a_key(36)),
    )
    .unwrap();

    raw_send(addr, &frame);
    let deadline = Instant::now() + Duration::from_secs(5);
    while victim.unauthenticated() == 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(victim.unauthenticated(), 1, "the forged term must be refused and counted");
    expect_nothing(&victim, Duration::from_millis(200));
    assert_eq!(victim.received(), 0, "nothing decoded, so nothing could be stepped");
    victim.shutdown();

    // **The anti-vacuity half.** The same bytes, at a node with no key, ARE accepted and DO carry
    // term 500. Without this the test above would pass against a transport that dropped every
    // frame for any reason at all.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let unguarded =
        Transport::from_listener(NodeId(1), listener, BTreeMap::new(), fast()).unwrap();
    let mut plain = encode(&genuine).unwrap();
    const PLAIN_TERM_AT: usize = 5 + 4 + 4;
    plain[PLAIN_TERM_AT..PLAIN_TERM_AT + 8].copy_from_slice(&500u64.to_be_bytes());
    raw_send(addr, &plain);
    let got = expect_recv(&unguarded, Duration::from_secs(5));
    assert_eq!(got.term, 500, "unsigned, the demotion lands: this is the attack the key closes");
    unguarded.shutdown();
}

/// Drive one `Node` until a condition holds or the deadline passes.
fn poll_until(node: &mut Node<RecordingApplier>, within: Duration, done: impl Fn(&Node<RecordingApplier>) -> bool) {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline && !done(node) {
        node.poll(Duration::from_millis(20)).unwrap();
    }
}

#[test]
fn a_keyless_peer_cannot_raise_the_term_of_a_node_built_through_the_driver() {
    // **The row's claim, at the surface a product actually uses.** Everything above tests
    // `Transport` directly; this tests `Node` — the driver that owns the clock, the socket and the
    // disk, and the thing `examples/consensus_node.rs` constructs. An adversarial pass pointed out
    // that a `Node` had no way to express a key at all, so the whole of F7 was unreachable from the
    // product surface and a keyless attacker set a real node's term to 500. `NodeOptions::signed_with`
    // is the answer, and this is its evidence.
    //
    // Both halves, because the refusal proves nothing unless the attack works without the key.
    let forged = Message {
        from: NodeId(2),
        to: NodeId(1),
        term: 500,
        body: Body::RequestVote { last_term: 400, last_round: 9000 },
    };
    let frame = encode(&forged).unwrap();

    // 1. SIGNED: the attacker's unsigned frame never reaches the state machine.
    let dir = tempfile::tempdir().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let cfg = Config::new([NodeId(1), NodeId(2), NodeId(3)], 1, 0);
    let opts = NodeOptions::new(dir.path(), BTreeMap::new(), 7)
        .tick_of(Duration::from_millis(20))
        .signed_with(Arc::new(a_key(50)));
    let mut node = Node::start(NodeId(1), cfg.clone(), listener, opts, RecordingApplier::default()).unwrap();
    let before = node.term();

    raw_send(addr, &frame);
    poll_until(&mut node, Duration::from_secs(3), |n| n.term() >= 500);
    let after = node.term();
    node.shutdown();
    assert_eq!(
        after, before,
        "a peer holding no key moved a signed node's term from {before} to {after}"
    );
    assert!(after < 500, "the forged term must never be adopted");

    // 2. UNSIGNED, the anti-vacuity half: the identical bytes at a node with no key DO land, and
    // the term becomes 500. This is the attack DISTRIBUTED.md §F7 names, and it is what the key
    // closes — asserted here so the refusal above cannot be a node that was simply deaf.
    let dir2 = tempfile::tempdir().unwrap();
    let listener2 = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr2 = listener2.local_addr().unwrap();
    let opts2 = NodeOptions::new(dir2.path(), BTreeMap::new(), 7).tick_of(Duration::from_millis(20));
    let mut bare = Node::start(NodeId(1), cfg, listener2, opts2, RecordingApplier::default()).unwrap();

    raw_send(addr2, &frame);
    poll_until(&mut bare, Duration::from_secs(5), |n| n.term() >= 500);
    let landed = bare.term();
    bare.shutdown();
    assert_eq!(landed, 500, "unsigned, a keyless peer sets this node's term: that is the attack");
}

#[test]
fn a_connection_that_sends_one_unverifiable_frame_is_closed_rather_than_left_open() {
    // A peer that cannot produce a tag is not a peer having a bad moment. Leaving the connection
    // open would let it hold one of `max_inbound_conns` and keep trying for ever.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    let node = Transport::from_listener_with_key(
        NodeId(1),
        listener,
        BTreeMap::new(),
        fast(),
        Arc::new(a_key(37)),
    )
    .unwrap();

    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let mut hs = Vec::new();
    write_handshake(&mut hs).unwrap();
    s.write_all(&hs).unwrap();
    let mut theirs = [0u8; 6];
    s.read_exact(&mut theirs).unwrap();

    // A frame signed with the wrong key.
    let frame = encode_signed(&vote_at(3), Some(&a_key(38))).unwrap();
    s.write_all(&frame).unwrap();
    s.flush().unwrap();

    // The node closes, so the next read returns EOF rather than blocking to the timeout.
    let mut buf = [0u8; 1];
    let n = s.read(&mut buf).expect("the socket must close cleanly, not error out");
    assert_eq!(n, 0, "the connection must be closed after an unverifiable frame");

    let deadline = Instant::now() + Duration::from_secs(5);
    while node.live_inbound_conns() > 0 && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(node.live_inbound_conns(), 0, "and the connection slot must be released");
    assert_eq!(node.unauthenticated(), 1);
    node.shutdown();
}
