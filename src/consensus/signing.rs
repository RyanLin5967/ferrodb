//! F7 — authenticating node-to-node traffic.
//!
//! **OWNER: agent F7.** An unauthenticated peer that can speak this protocol can claim a later term
//! and demote a healthy leader, so the transport must refuse a message it cannot authenticate
//! rather than pass it to the state machine.
//!
//! # What this proves, and what it does not
//!
//! **It proves the sender holds the cluster key. It does NOT prove the message is new.**
//!
//! That sentence is the whole contract and it is stated first because the second half is the part a
//! reader would otherwise have to discover. Every frame carries an HMAC-SHA256 tag over its own
//! bytes and nothing else — no counter, no nonce, no timestamp — so a passive attacker who records
//! a frame can re-send it, unchanged, at any later moment, and it will verify. **Replay protection
//! is a follow-on row and it is absent here.**
//!
//! Being precise about what that costs, because "no replay protection" sounds worse and is also
//! worse in a different place than it first appears:
//!
//! * **Replay *within* the current term is already inside the protocol's model.** Consensus is
//!   specified against a network that drops, reorders and duplicates — that is why `replicate.rs`
//!   backs a peer up by `hint` rather than assuming delivery, and why a re-delivered `Append` is
//!   idempotent. A replayed heartbeat is a duplicated heartbeat, which the state machine already
//!   has to survive.
//! * **Replay *from an earlier* term is refused by the term rules**, not by this module.
//!   `Consensus::on_message` drops any message whose term is below its own before a handler sees
//!   it, so a recording of last term's election cannot be re-run against a node that has moved on.
//! * **What is genuinely open** is replay across an *incarnation*: a cluster rebuilt on the same
//!   key file, with its term counters back at zero, will accept a recording of the previous
//!   cluster's traffic. So will a node restored from a backup taken before the recording. Neither
//!   is exotic — restoring from a snapshot is a supported operation — and neither is defended
//!   against here.
//! * **Also open:** an attacker who can reach the port may splice frames it recorded from one
//!   connection into another, since the tag binds nothing about the connection carrying it. The
//!   addressee *is* bound (`to` is inside the tag), so a frame cannot be reflected back at its
//!   sender or redirected at a third node — but the same frame may arrive twice, on two
//!   connections, out of order.
//!
//! The follow-on row closes all four the same way: a per-connection epoch and a monotone sequence
//! number, both established inside the handshake and both bound into the tag, so a frame from
//! another connection or another incarnation authenticates against a key the receiver is no longer
//! using. That is a protocol change, not an additional check, which is why it is a row of its own
//! rather than a paragraph here.
//!
//! # What is inside the tag, and why the term must be
//!
//! The tag is taken over the **whole encoded frame body** — which is exactly
//! `(from, to, term, kind, kind-specific fields)`, in that order, because that is the layout
//! `transport::encode` writes and `transport::decode` reads.
//!
//! **The term has to be in there or the module is pointless.** The attack this row exists to close
//! is a peer asserting a later term to demote a healthy leader; if the term travelled outside the
//! authenticated region, an attacker who could not forge a tag could still take a genuine frame off
//! the wire, raise its term field, and forward it. Every node that received it would step down.
//! Signing the body rather than a digest of "the interesting fields" is what makes that
//! impossible to get wrong by omission: there is no field of the message that is outside the tag.
//!
//! The frame's own header — the `'C'` tag byte and the `u32` length — is *not* inside the MAC, and
//! does not need to be. The length is bound implicitly, because the receiver hands
//! [`verify_frame`] exactly the bytes the length named and the tag covers all of them: a truncated
//! or extended frame changes the authenticated region and fails. The `'C'` and the protocol
//! version are bound explicitly, through [`DOMAIN`] below.
//!
//! ## Domain separation
//!
//! The authenticated region is prefixed with a fixed [`DOMAIN`] string naming this protocol, its
//! frame tag and its wire version. It costs nothing and it means a tag minted here can never be
//! mistaken for a tag over some later ferrodb protocol that happens to share the key file, nor for
//! a version-2 frame replayed at a version 3 that gave a field a new meaning. The prefix is not on
//! the wire; both ends know it statically.
//!
//! # Why HMAC-SHA256, and why not the two cheaper things
//!
//! `Cargo.toml`'s `[dependencies]` is empty and that is a product claim, so the MAC is implemented
//! here. It is built on [`crate::provenance::sha256`], which already exists and is already pinned
//! against FIPS 180-4's published vectors — reusing the validated implementation rather than
//! introducing a second primitive that would need its own evidence.
//!
//! * **Not `sha256(key || message)`.** SHA-256 is a Merkle–Damgård construction, so its digest *is*
//!   its internal state: an attacker holding a tag for one message can extend that message with
//!   padding and a suffix of their choice and compute the tag for the result **without the key**.
//!   Against this protocol that is not academic — a valid `Append` frame with bytes appended is a
//!   frame [`transport::decode`](super::transport::decode) refuses for trailing bytes, but the
//!   general shape (forge a longer message from a shorter one) is exactly what a MAC must not
//!   allow, and the fix is a construction rather than a rule about lengths. HMAC's nested
//!   `H(K^opad || H(K^ipad || m))` is not extendable.
//! * **Not SipHash.** Its tag is 64 bits. Consensus messages arrive over a socket an attacker can
//!   write to as fast as the network allows, so a forgery probability of 2^-64 *per attempt* is a
//!   budget, not a bound. A 256-bit tag costs 32 bytes on a frame whose limit is 8 MiB.
//!
//! # Two key files can be one key, which matters when rotating
//!
//! Consequences of RFC 2104's key preparation, not defects, but an operator changing a key should
//! know them because neither is a change:
//!
//! * **A key and the same key zero-padded to at most 64 bytes are the same key.** Appending NULs to
//!   a 32-byte key file mints identical tags, and each file verifies the other's frames. (A
//!   trailing *newline* is not in this class — `0x0a` is not padding.)
//! * **A key longer than 64 bytes and its own SHA-256 are the same key**, because that is exactly
//!   the substitution RFC 2104 specifies for an over-long key. Exactly 64 bytes is not in the class;
//!   the rule is strictly greater than the block.
//!
//! # The key file
//!
//! Read from a **file**, never from a command-line value: an argument is in the process list, which
//! is world-readable on every platform this builds for. [`Key::load`] refuses a key under
//! [`MIN_KEY_BYTES`], refuses one whose file is readable by group or other, and refuses one whose
//! directory is writable by group or other without the sticky bit — a key an attacker can *replace*
//! is a key they hold.
//!
//! **The permission check has a blind spot and the guard names it rather than passing quietly.**
//! Unix mode bits are readable through `std`; Windows ACLs are not, and reading them needs a crate
//! this build may not have. So on Windows [`Key::load`] **refuses**, and an operator who has
//! protected the file by other means says so explicitly with
//! [`PermissionCheck::AcceptUnverifiable`]. Falling through to "allowed" on the platform where the
//! check cannot run would make this a guard that reports a file protected when it inspected
//! nothing. On Unix that value changes nothing: where the evidence exists it decides, so a
//! group-readable key is refused under either.
//!
//! The decision lives in `accept_unverifiable`, a plain function with no `cfg` of its own, called
//! by `check_protection` when [`PROTECTION_IS_CHECKABLE`] is false. That is deliberate: both of its
//! arms are then exercised by the ordinary test run on every platform, and the only thing the
//! Windows runner alone can prove is a one-line wiring. A guard whose evidence arrives from one CI
//! runner is a guard nobody here has seen work.
//!
//! Two further dimensions are **not** checked, stated here rather than left to be discovered:
//!
//! * **Ownership.** A file with mode `0600` owned by another user is unreadable by this process
//!   anyway, unless this process is root — and `std` exposes no `geteuid`, so the comparison cannot
//!   be made without a dependency. A cluster running its nodes as root and keeping its key in
//!   another user's home directory is outside what this can detect.
//!
//! The ancestry above the immediate parent **used** to be an unchecked dimension and is no longer:
//! every component of the resolution is walked and every directory in it is inspected, because an
//! adversarial pass showed that a symlink one level up redirects the key just as well as one at the
//! end. What remains true is that the walk is by *name* — `std` cannot say which directory holds
//! the inode behind a descriptor — while the *mode* check is `fstat` on the descriptor and cannot
//! be raced.
//!   A world-writable grandparent lets an attacker swap the whole directory. Checking every
//!   ancestor was considered and not done: it refuses ordinary layouts for a threat that already
//!   implies control of the filesystem.
//!
//! # Constant time
//!
//! [`constant_time_eq`] compares every byte before it answers. A comparison that stops at the first
//! difference leaks, through its own duration, how many leading bytes of a guess were right, which
//! turns forging a 32-byte tag from 2^256 work into 32 × 256. `==` on two slices is exactly that
//! comparison, which is why this function exists instead. The property is pinned by
//! `constant_time_eq_does_not_short_circuit` in `tests_signing.rs`, which times a difference in the
//! first byte against a difference in the last over a buffer large enough that a short-circuiting
//! implementation is off by three orders of magnitude rather than by noise.
//!
//! The claim is bounded: this is constant-time **in the position of the first differing byte**,
//! which is the leak that matters for a tag comparison. It is not a defence against an attacker who
//! can measure cache state or run on the same core.
//!
//! # What is not authenticated
//!
//! * **Connection establishment.** The six-byte handshake is unauthenticated, so anything that can
//!   reach the port can still open a connection and occupy one of `max_inbound_conns` slots. What
//!   it cannot do is have a message reach the state machine: the first frame that fails to verify
//!   closes the connection, so the cost of an unauthenticated peer is bounded by the connection
//!   cap that already exists, and never by the protocol.
//! * **The identity of the sending node.** The key is **cluster-wide**, so a tag proves its author
//!   is inside the cluster and says nothing about *which* member it is. `from` is inside the
//!   authenticated region, so nobody outside can forge it — but any node holding the key can put
//!   any other node's id there, so a single compromised node can impersonate every other one. Fixing
//!   that means per-node or per-pair keys and therefore key distribution, which is a larger row than
//!   this one; it is named here so the property is not mistaken for something this provides.
//! * **The courtesy `Error` frame sent to a peer whose handshake was refused.** It is unsigned, of
//!   necessity: a peer that failed the version handshake is by definition speaking a protocol that
//!   does not know about tags. It carries this node's version string and nothing else.
//! * **A cluster where only some nodes hold a key.** There is no negotiation. A node with a key
//!   refuses an unsigned frame (the last 32 bytes of the body are not a valid tag over the rest); a
//!   node without one refuses a signed frame (the leading 32 bytes decode as nonsense). Mixed
//!   configuration therefore presents as a cluster that cannot talk to itself, in both directions,
//!   which is the safe way for that mistake to fail and is why no fallback-to-unsigned path exists.

use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};

use crate::error::FerroError;
use crate::provenance::sha256::Sha256;
use crate::replication::{CONSENSUS_TAG, MAX_FRAME_BYTES, REPL_VERSION};

/// Bytes of tag on every signed frame. SHA-256's full output; not truncated.
pub const MAC_LEN: usize = 32;

/// The shortest key this will load.
///
/// 32 bytes because the tag is 32 bytes: a shorter key is the weaker half of the pair and makes the
/// tag length a decoration. HMAC accepts any key length, so nothing here *breaks* with a 16-byte
/// key — which is the reason it is refused at the door rather than left to a reader to reason
/// about.
pub const MIN_KEY_BYTES: usize = 32;

/// SHA-256's block size, and therefore HMAC's.
const BLOCK: usize = 64;

/// The domain-separation prefix, mixed in before the frame body.
///
/// Names the protocol, the frame tag it travels under and the wire version, so a tag is valid for
/// exactly one meaning of the bytes it covers. Built at compile time from the constants themselves
/// rather than written out, so a version bump in `replication` cannot leave a stale literal here.
pub const DOMAIN: [u8; 26] = {
    let mut d = [0u8; 26];
    let label = b"ferrodb/consensus/mac/1\0";
    let mut i = 0;
    while i < label.len() {
        d[i] = label[i];
        i += 1;
    }
    d[24] = CONSENSUS_TAG;
    // The wire version is a `u16` and every version this code can be compiled against is small;
    // asserting rather than truncating, because a silent narrowing here would make two protocol
    // versions share a domain.
    assert!(REPL_VERSION <= u8::MAX as u16, "REPL_VERSION no longer fits the MAC domain byte");
    d[25] = REPL_VERSION as u8;
    d
};

// -------------------------------------------------------------------------------------------
// The primitive
// -------------------------------------------------------------------------------------------

/// HMAC-SHA256, RFC 2104, over ferrodb's own SHA-256.
///
/// Free-standing and accepting **any** key length, because that is what the published test vectors
/// exercise: RFC 4231 uses keys of 4, 20, 25 and 131 bytes, and a function that refused short keys
/// could not be checked against them. The ≥ [`MIN_KEY_BYTES`] rule belongs to [`Key`], which is
/// what a cluster actually signs with — the primitive and the policy are separable and are
/// separated.
pub fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; 32] {
    hmac_sha256_parts(key, &[message])
}

/// The same, over a message given in pieces rather than as one slice.
///
/// The reason this exists rather than a `concat`: [`Key::tag`] authenticates [`DOMAIN`] followed by
/// the frame body, and joining them would allocate and copy the **whole body** — up to
/// [`MAX_FRAME_BYTES`] — for every frame signed and every frame verified. SHA-256 is a streaming
/// hash and this is what streaming it is for. The pieces are concatenated *in the hash*, which is
/// the same value the joined slice would have produced and is asserted to be in `tests_signing.rs`.
///
/// The pieces are **not** length-prefixed and do not need to be: every caller uses a
/// fixed-length prefix, so the split point is not something an attacker can move. A caller that
/// passed two variable-length pieces would be building an ambiguous encoding, which is why there is
/// no public caller that can.
fn hmac_sha256_parts(key: &[u8], parts: &[&[u8]]) -> [u8; 32] {
    // RFC 2104: a key longer than the block is replaced by its own digest; a shorter one is
    // zero-padded to the block. Both branches produce exactly `BLOCK` bytes, which is what makes
    // the two pads below well-defined.
    let mut k0 = [0u8; BLOCK];
    if key.len() > BLOCK {
        k0[..32].copy_from_slice(&crate::provenance::sha256::sha256(key));
    } else {
        k0[..key.len()].copy_from_slice(key);
    }

    let mut ipad = [0x36u8; BLOCK];
    let mut opad = [0x5cu8; BLOCK];
    for i in 0..BLOCK {
        ipad[i] ^= k0[i];
        opad[i] ^= k0[i];
    }

    let mut inner = Sha256::new();
    inner.update(&ipad);
    for part in parts {
        inner.update(part);
    }
    let inner = inner.finish();

    let mut outer = Sha256::new();
    outer.update(&opad);
    outer.update(&inner);
    let out = outer.finish();

    // The padded key and both pads are key-equivalent; wiped before this frame's stack space is
    // reused. See the header's note on the limits of this.
    wipe(&mut k0);
    wipe(&mut ipad);
    wipe(&mut opad);
    out
}

/// Overwrite a buffer and make the compiler believe the result is observed.
///
/// `black_box` rather than a volatile write, because this crate has no `unsafe` anywhere and adding
/// the first block of it for a hygiene measure is the wrong trade. The barrier is what stops the
/// zeroing being deleted as a store to memory that is about to die.
fn wipe(bytes: &mut [u8]) {
    for b in bytes.iter_mut() {
        *b = 0;
    }
    std::hint::black_box(bytes);
}

/// Whether two byte strings are equal, in time that does not depend on **where** they first differ.
///
/// The length comparison is not constant time and does not need to be: the length of a tag is
/// fixed, public and on the wire already. What must not leak is the position of the first
/// mismatching byte — see the header.
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    // Without the barrier the whole loop is a candidate for being rewritten into an early-exiting
    // comparison, which is the one thing this function must not be.
    std::hint::black_box(diff) == 0
}

// -------------------------------------------------------------------------------------------
// The key
// -------------------------------------------------------------------------------------------

/// What [`Key::load_with`] should do on a platform where the file's protection cannot be read.
///
/// An explicit argument rather than a silent per-platform difference, because the difference is
/// the entire security property of the key file and the operator is the only one who can settle it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionCheck {
    /// Require evidence that only the owner can read the key. Where `std` cannot produce that
    /// evidence — Windows — loading **refuses**.
    Enforce,
    /// The operator asserts, out of band, that the file is protected, on a platform where this
    /// build cannot check. Has no effect where the check *can* run: a group-readable key on Unix is
    /// refused under either value, because there the evidence exists and says no.
    AcceptUnverifiable,
}

/// A cluster's shared signing key.
///
/// Holds the raw bytes rather than the two pre-computed HMAC states. Priming the pads costs two
/// SHA-256 compressions per message, which is nothing beside a socket write, and in exchange there
/// is exactly one long-lived copy of key-equivalent material in this process and [`Drop`] can
/// clear it. Caching the primed states would put the same secret in two places, neither of which
/// this module could wipe — `Sha256`'s fields are private to another module.
pub struct Key {
    bytes: Vec<u8>,
    /// Where it came from, for error messages. Never the key.
    source: Option<PathBuf>,
}

impl Key {
    /// Load and validate, enforcing the permission check.
    ///
    /// The call every real caller should make. [`Key::load_with`] exists for the one platform where
    /// the check cannot run and the operator has to say so.
    pub fn load(path: impl AsRef<Path>) -> Result<Key, FerroError> {
        Key::load_with(path, PermissionCheck::Enforce)
    }

    /// Load and validate.
    ///
    /// Two orderings matter here and both are deliberate.
    ///
    /// **The file is opened ONCE and judged through that handle**, never by name twice. `stat` the
    /// path, check the mode, then `read` the path is a race: between the two calls the name can be
    /// repointed at a different file, and the mode that was approved would belong to bytes nobody
    /// read. `File::metadata` is `fstat` on this descriptor, so the thing inspected and the thing
    /// read are the same file by construction rather than by timing. The window is small and needs
    /// write access to the directory to exploit — which `unix_protection` refuses anyway — so this
    /// is the second lock on a door that already has one, and it costs nothing.
    ///
    /// **The protection is judged before the contents are read**, so a key this build would refuse
    /// never reaches this process's memory.
    pub fn load_with(path: impl AsRef<Path>, check: PermissionCheck) -> Result<Key, FerroError> {
        let path = path.as_ref();
        // **The shape is checked by NAME, before the file is opened.** Not redundant with the
        // `is_file` on the descriptor below, and not a security check: opening a FIFO for reading
        // **blocks until a writer appears**, so a node configured with a FIFO as its key path hung
        // at startup for ever instead of refusing — found by an adversarial pass. `is_file()` on
        // the descriptor cannot help, because control never reaches it.
        //
        // The race between this lookup and the open below does not matter, precisely because this
        // is a shape check: the authoritative mode check is still `fstat` on the descriptor, and
        // that descriptor's own `is_file` is kept below so the name-based answer is never trusted
        // on its own. `fs::metadata` follows symlinks, so a link to a FIFO is caught here too.
        let shape = fs::metadata(path).map_err(|e| {
            FerroError::Io(format!(
                "the consensus signing key at {} could not be inspected: {e}. A node configured to \
                 sign its traffic and unable to load its key refuses to start rather than falling \
                 back to sending unsigned frames",
                path.display()
            ))
        })?;
        if !shape.is_file() {
            return Err(FerroError::Io(format!(
                "{} is not a regular file, so it cannot be a signing key. Refused here, by name, \
                 rather than after opening it: a FIFO blocks its opener until a writer appears, so \
                 a node pointed at one would hang at startup instead of telling anybody why.",
                path.display()
            )));
        }
        let mut file = fs::File::open(path).map_err(|e| {
            FerroError::Io(format!(
                "the consensus signing key at {} could not be opened: {e}. A node configured to \
                 sign its traffic and unable to load its key refuses to start rather than falling \
                 back to sending unsigned frames",
                path.display()
            ))
        })?;
        let meta = file.metadata().map_err(|e| {
            FerroError::Io(format!(
                "the consensus signing key at {} was opened but could not be inspected: {e}",
                path.display()
            ))
        })?;
        if !meta.is_file() {
            return Err(FerroError::Io(format!(
                "{} is not a regular file, so it cannot be a signing key. A directory or a device \
                 named where a key was expected is a configuration mistake, and reading it would \
                 produce a key whose bytes nobody chose",
                path.display()
            )));
        }
        check_protection(path, &meta, check)?;

        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes).map_err(|e| {
            FerroError::Io(format!("the consensus signing key at {} could not be read: {e}", path.display()))
        })?;
        // **A file of zeros is what a failed generator leaves behind, and it passes every other
        // rule here.** `truncate -s 32 cluster.key`, a sparse copy, or a script that wrote nothing
        // and exited 0 all produce a file of exactly the right length holding a key nobody chose —
        // and every node given it agrees with every other, so the cluster comes up and looks
        // healthy. Found by an adversarial pass. Refused, for the same reason a run that collected
        // nothing has not passed.
        //
        // **The limit of this check, stated rather than implied:** it is a "the generator produced
        // nothing" test, not an entropy test. A key of 32 identical `0xff` bytes, or a passphrase
        // somebody typed, is accepted. Judging randomness is not something this can do, and a check
        // that pretended to would be worse than one that says what it is.
        if !bytes.is_empty() && bytes.iter().all(|b| *b == 0) {
            let n = bytes.len();
            let mut bytes = bytes;
            wipe(&mut bytes);
            return Err(FerroError::Io(format!(
                "the consensus signing key at {} is {n} zero bytes. That is not a key — it is what \
                 `truncate`, a sparse copy, or a generator that wrote nothing and exited 0 leaves \
                 behind, and its length alone cannot tell it from a real one. Generate one with \
                 `head -c 32 /dev/urandom > {}`",
                path.display(),
                path.display()
            )));
        }
        if bytes.len() < MIN_KEY_BYTES {
            // The length is named; the bytes are not, here or anywhere else in this module.
            let n = bytes.len();
            let mut bytes = bytes;
            wipe(&mut bytes);
            return Err(FerroError::Io(format!(
                "the consensus signing key at {} is {n} byte(s); {MIN_KEY_BYTES} is the minimum. \
                 The tag it would produce is {MAC_LEN} bytes wide, so a shorter key makes that \
                 width a decoration rather than a bound. Generate one with \
                 `head -c 32 /dev/urandom > {}`",
                path.display(),
                path.display()
            )));
        }
        Ok(Key { bytes, source: Some(path.to_path_buf()) })
    }

    /// A key from bytes already in hand. **Tests only** — a real key comes from a file.
    ///
    /// `cfg(test)` rather than merely private, so that no future caller in `src/` can reach for it
    /// and reintroduce the flag-shaped key the header refuses. The length rule still applies: a
    /// test that could build a 4-byte key would be testing something the cluster cannot do.
    #[cfg(test)]
    pub(crate) fn from_bytes_for_test(bytes: Vec<u8>) -> Result<Key, FerroError> {
        if bytes.len() < MIN_KEY_BYTES {
            return Err(FerroError::Io(format!(
                "a signing key of {} byte(s) is under the {MIN_KEY_BYTES}-byte minimum",
                bytes.len()
            )));
        }
        Ok(Key { bytes, source: None })
    }

    /// How many bytes of key. Not the key.
    pub fn len(&self) -> usize {
        self.bytes.len()
    }

    /// Never true — a `Key` under [`MIN_KEY_BYTES`] cannot be constructed. Present because clippy
    /// asks for it beside `len`, and answered honestly rather than by an `allow`.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }

    /// The file this was loaded from, for an error message that has to name it.
    pub fn source(&self) -> Option<&Path> {
        self.source.as_deref()
    }

    /// The tag over one authenticated region, domain-separated.
    pub fn tag(&self, message: &[u8]) -> [u8; MAC_LEN] {
        // Streamed, not joined: joining would copy the whole frame body on every sign AND every
        // verify. See [`hmac_sha256_parts`].
        hmac_sha256_parts(&self.bytes, &[&DOMAIN, message])
    }

    /// Whether `mac` is the tag this key would produce over `message`.
    ///
    /// Constant time in the position of the first differing byte. A wrong length answers `false`
    /// immediately, which leaks only the length of the tag on the wire.
    pub fn verify(&self, message: &[u8], mac: &[u8]) -> bool {
        constant_time_eq(&self.tag(message), mac)
    }
}

/// Redacted, and deliberately not `{:?}`-derivable.
///
/// `TransportOptions` derives `Debug` and is logged and asserted on in tests; a derived `Debug`
/// here would put the cluster key in every one of those strings. The length is shown because it is
/// the field an operator debugging a refused frame actually wants.
impl std::fmt::Debug for Key {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Key")
            .field("bytes", &format_args!("<{} redacted>", self.bytes.len()))
            .field("source", &self.source)
            .finish()
    }
}

/// Clears the key on the way out.
///
/// Worth doing and worth being honest about: it removes the key from the heap when a node stops
/// signing, which narrows a core dump taken afterwards. It does nothing about the file it was read
/// from, about the page cache holding that file, or about the transient pads inside
/// [`hmac_sha256`] while a frame is being signed. It is hygiene, not a defence against an attacker
/// who can already read this process's memory.
impl Drop for Key {
    fn drop(&mut self) {
        wipe(&mut self.bytes);
    }
}

/// Whether `std` on the platform this was compiled for can show **who may read** a file.
///
/// True on Unix, where the mode bits answer it. False on Windows, where the answer lives in an ACL
/// that `std` does not expose and that this crate has no dependency to read.
pub const PROTECTION_IS_CHECKABLE: bool = cfg!(unix);

/// The file-protection rule. One copy, and one place a platform is added.
fn check_protection(
    path: &Path,
    meta: &fs::Metadata,
    check: PermissionCheck,
) -> Result<(), FerroError> {
    if !PROTECTION_IS_CHECKABLE {
        return accept_unverifiable(check, path);
    }
    #[cfg(unix)]
    {
        unix_protection(path, meta)
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        unreachable!("PROTECTION_IS_CHECKABLE is false off Unix, so this returned above")
    }
}

/// What to do on a platform whose file protection this build cannot read.
///
/// **Refuses under [`PermissionCheck::Enforce`]**, and says why. `std` exposes no way to read a
/// Windows ACL and this crate has no dependencies to borrow one from. The alternative — returning
/// `Ok(())` on the platform where nothing was inspected — is the failure shape this project keeps
/// meeting: a check that reports "protected" having verified nothing. So the guard asks, exactly as
/// a guard that cannot parse its own input must, and the operator answers.
///
/// **Split out from [`check_protection`] and taking no `cfg` of its own, so that both of its arms
/// are reachable from a test on every platform.** A rule that only the Windows runner can execute
/// is a rule whose evidence arrives once a day from somewhere else; here the decision is an
/// ordinary function and only its one-line wiring to `PROTECTION_IS_CHECKABLE` is platform-shaped.
fn accept_unverifiable(check: PermissionCheck, path: &Path) -> Result<(), FerroError> {
    match check {
        PermissionCheck::AcceptUnverifiable => Ok(()),
        PermissionCheck::Enforce => Err(FerroError::Io(format!(
            "this build cannot read the access control list on {}, so it cannot show that the \
             consensus signing key is readable only by its owner. Reading a Windows ACL needs an \
             API `std` does not expose and a crate this build does not have. Rather than report a \
             file protected having inspected nothing, loading is refused: protect the file with \
             `icacls` and pass `PermissionCheck::AcceptUnverifiable` to say that you have.",
            path.display()
        ))),
    }
}

/// The Unix rule: the file's own mode, and then the directory that holds it.
///
/// [`PermissionCheck`] is deliberately **not** a parameter. Where the evidence exists it decides,
/// and an operator cannot accept away a mode this build can see — otherwise the escape hatch for
/// the platform that cannot check would become an escape hatch for the platform that can.
#[cfg(unix)]
fn unix_protection(path: &Path, meta: &fs::Metadata) -> Result<(), FerroError> {
    use std::os::unix::fs::PermissionsExt;

    let mode = meta.permissions().mode();
    if mode & 0o077 != 0 {
        return Err(FerroError::Io(format!(
            "the consensus signing key at {} has mode {:04o}; it must not be readable by group or \
             other. Anyone who can read this file can forge any message from any node, including a \
             later term that demotes a healthy leader. Fix with `chmod 600 {}`",
            path.display(),
            mode & 0o7777,
            path.display()
        )));
    }

    // A key an attacker can REPLACE is a key they hold, so the directory matters as much as the
    // file. The sticky bit is the exception rather than a special case: on a sticky directory only
    // the owner of a file may rename or remove it, which is exactly the property being checked for.
    // This is the rule OpenSSH's StrictModes applies, for the same reason.
    //
    // **Both directories are checked: the one the NAME sits in, and the one the INODE sits in.**
    //
    // They are not the same when the path is a symlink, and checking only the first is a complete
    // bypass of this rule — found by an adversarial pass, with a working substitution. `safe/k` is
    // a link to `open/k`; `safe/` is 0700 and `open/` is 0777. The mode check is fine, because it
    // reads the open descriptor and therefore the target's mode. The directory check was not: it
    // read `safe/`, pronounced the key protected, and an attacker with write access to `open/`
    // renamed their own key over the target. `Key::load` returned `Ok` and the node then verified
    // frames the attacker had signed.
    //
    // So the rule needs both. The name's directory matters because whoever can write there can
    // repoint the link; the inode's directory matters because whoever can write THERE can replace
    // what the link points at. Either one being open is enough to lose the key.
    for dir in directories_to_check(path)? {
        check_directory(&dir)?;
    }
    Ok(())
}

/// One step of a path, owned so it can outlive the `Path` it came from.
#[cfg(unix)]
enum Step {
    Root,
    Here,
    Up,
    Name(std::ffi::OsString),
}

#[cfg(unix)]
fn steps_of(p: &Path) -> Vec<Step> {
    use std::path::Component;
    p.components()
        .map(|c| match c {
            Component::RootDir => Step::Root,
            Component::CurDir => Step::Here,
            Component::ParentDir => Step::Up,
            Component::Normal(n) => Step::Name(n.to_os_string()),
            // Unreachable under `cfg(unix)`; a drive prefix is a Windows concept.
            Component::Prefix(_) => Step::Here,
        })
        .collect()
}

/// Every directory whose contents could be swapped for the key this path names.
///
/// **A walk of every COMPONENT of the resolution, not of the final names in it.** This rule has
/// been wrong twice, in the same direction both times, and the second correction is why it is now
/// shaped like this rather than like a shortcut:
///
/// 1. It first checked the path as given and the *canonicalised* path. `canonicalize` collapses the
///    chain, so those are its two endpoints and every hop between them is invisible. An adversarial
///    pass renamed a symlink over a middle name — `safe/k -> open/k -> known`, with `open` at 0777
///    and both ends at 0700 — and pointed it at a file the node could already read.
/// 2. It then walked the chain hop by hop, which closed that. The same pass broke it again with a
///    symlink among the **directory** components: `symlink_metadata(name)` asks whether that final
///    name is a link, and the kernel has already silently resolved every directory above it. With
///    `a/k -> b/dl/real` and `b/dl -> c`, the directory `b` holds a link nobody looked at.
///
/// Both are the same mistake at different depths, and the fix that is not a third instance of it is
/// to resolve the path the way the kernel does — one component at a time, from the root — and check
/// the directory each component sits in. **The attacker never authors a key file** in any of these:
/// they repoint a name they may write at a file the node can already read, so the mode rule passes
/// on that file's own `0600`. Only the directory rule can stop redirection, and it can only stop it
/// where it actually looks.
///
/// **This checks the whole ancestry, and that is a deliberate change from an earlier stated limit.**
/// A group- or world-writable directory anywhere above the key — not merely its immediate parent —
/// is now a refusal, because an attacker who can write to `/usr/local` can replace `/usr/local/etc`
/// and redirect everything below it. It is the rule OpenSSH's `StrictModes` applies, for this
/// reason. The cost is real and accepted: a key under a group-writable prefix is refused rather than
/// warned about, and the error names the directory and the `chmod`.
///
/// **What remains by NAME, and it is an asymmetry worth stating.** The *mode* check is `fstat` on
/// the descriptor the key is read from and cannot be raced. This cannot be: `std` offers no way to
/// ask which directory holds the inode behind a descriptor, so the walk is strings. Winning that
/// race needs write access to a directory in the chain — which is exactly what this refuses.
#[cfg(unix)]
fn directories_to_check(path: &Path) -> Result<Vec<PathBuf>, FerroError> {
    use std::collections::VecDeque;

    /// Bounded so a symlink cycle is refused rather than looped on.
    const MAX_HOPS: usize = 40;

    let mut checked: Vec<PathBuf> = Vec::new();
    // The prefix resolved so far. Empty means "relative to the process's current directory".
    let mut resolved = PathBuf::new();
    let mut queue: VecDeque<Step> = steps_of(path).into();
    let mut hops = 0usize;

    while let Some(step) = queue.pop_front() {
        match step {
            Step::Root => resolved = PathBuf::from("/"),
            Step::Here => {
                if resolved.as_os_str().is_empty() {
                    resolved = PathBuf::from(".");
                }
            }
            // Kept literally rather than popped. Popping is only correct for a path with no
            // leading `..` and no `..` immediately after a symlink, and getting that wrong would
            // silently inspect a directory that has nothing to do with the chain. `..` in a string
            // is resolved by the kernel when the string is stat'd, which is the behaviour wanted;
            // the directory it denotes is one this walk has already checked on the way down.
            Step::Up => resolved.push(".."),
            Step::Name(name) => {
                let dir = if resolved.as_os_str().is_empty() {
                    PathBuf::from(".")
                } else {
                    resolved.clone()
                };
                if !checked.contains(&dir) {
                    checked.push(dir.clone());
                }
                let here = dir.join(&name);
                // Does NOT follow, which is the point: this asks what `here` itself is, so a link
                // is seen as a link rather than as the file at the end of it.
                let link_meta = fs::symlink_metadata(&here).map_err(|e| {
                    FerroError::Io(format!(
                        "{} — a component of the consensus signing key's path — could not be \
                         inspected: {e}. Refused rather than assumed safe: a path that cannot be \
                         walked is one whose directories cannot be shown to be closed.",
                        here.display()
                    ))
                })?;
                if link_meta.file_type().is_symlink() {
                    hops += 1;
                    if hops > MAX_HOPS {
                        return Err(FerroError::Io(format!(
                            "the consensus signing key at {} is behind more than {MAX_HOPS} \
                             symlinks, or behind a cycle of them. Refused rather than followed: a \
                             chain nobody can walk is a chain whose directories nobody can show \
                             are closed.",
                            path.display()
                        )));
                    }
                    let target = fs::read_link(&here).map_err(|e| {
                        FerroError::Io(format!("the symlink at {} could not be read: {e}", here.display()))
                    })?;
                    // A relative target continues from the directory the LINK sits in, never from
                    // the process's cwd — which is what leaving `resolved` alone does.
                    //
                    // An absolute target needs no special case, and a mutant proved it: an explicit
                    // `if target.is_absolute() { resolved = PathBuf::new() }` was here, deleting it
                    // changed nothing, and the reason is that an absolute path's first step IS
                    // `Step::Root`, whose arm already sets `resolved` to `/`. The line was a second
                    // statement of one rule, and the kind that goes stale. Removed rather than kept
                    // as a branch no test can reach.
                    for step in steps_of(&target).into_iter().rev() {
                        queue.push_front(step);
                    }
                } else {
                    resolved = here;
                }
            }
        }
    }
    Ok(checked)
}

/// The mode rule for one directory holding a key.
#[cfg(unix)]
fn check_directory(dir: &Path) -> Result<(), FerroError> {
    use std::os::unix::fs::PermissionsExt;
    // **Refused, not skipped, when the directory cannot be inspected.** This was `if let Ok(..)`,
    // which fell through to "allowed" whenever the `stat` failed — a guard that cannot read its own
    // input must refuse, never pass. It is reachable: an adversarial pass renamed the parent
    // directory back and forth in a second thread and got 46,895 successful loads of a key sitting
    // in a directory this rule refuses. A transient failure — an unmount, a stale handle — reaches
    // it too, and fell open the same way.
    let dmeta = fs::metadata(dir).map_err(|e| {
        FerroError::Io(format!(
            "the directory holding the consensus signing key, {}, could not be inspected: {e}. \
             Refused rather than assumed safe: this check is what stops an attacker who can write \
             to that directory from replacing the key with one they chose.",
            dir.display()
        ))
    })?;
    let dmode = dmeta.permissions().mode();
    let sticky = dmode & 0o1000 != 0;
    if dmode & 0o022 != 0 && !sticky {
        return Err(FerroError::Io(format!(
            "the directory holding the consensus signing key, {}, has mode {:04o}: it is \
             writable by group or other and is not sticky, so anyone who can write there can \
             replace the key with one they chose. The key file's own mode does not help — the \
             attacker does not need to read it. Fix with `chmod go-w {}`",
            dir.display(),
            dmode & 0o7777,
            dir.display()
        )));
    }
    Ok(())
}

// -------------------------------------------------------------------------------------------
// The frame layer
// -------------------------------------------------------------------------------------------

/// Wrap one encoded frame body — `(from, to, term, kind, fields)` — with its tag.
///
/// The tag goes **first**, so a receiver can authenticate before it parses. That ordering is the
/// point: `transport::decode` is the code that has to be safe against hostile input, and a
/// receiver that parses first and checks afterwards has already run the parser on whatever arrived.
///
/// Fallible for one reason: the tag costs [`MAC_LEN`] bytes, and a body that fitted the frame limit
/// unsigned can fail to fit signed. Refused to the caller rather than framed, exactly as
/// `transport::encode` refuses an over-large body — a leader whose batch no longer fits must send
/// fewer entries, and it can only learn that from an error.
pub fn sign_frame(key: &Key, body: &[u8]) -> Result<Vec<u8>, FerroError> {
    let total = body.len().saturating_add(MAC_LEN);
    if total > MAX_FRAME_BYTES {
        return Err(FerroError::Wal(format!(
            "a consensus message of {} bytes takes {total} once its {MAC_LEN}-byte authentication \
             tag is added, over the {MAX_FRAME_BYTES}-byte frame limit. Refused rather than framed: \
             a frame whose length header exceeds the limit is one the peer drops without saying \
             why. Send fewer entries.",
            body.len()
        )));
    }
    let mut out = Vec::with_capacity(total);
    out.extend_from_slice(&key.tag(body));
    out.extend_from_slice(body);
    Ok(out)
}

/// Authenticate one frame body and return the part `transport::decode` should see.
///
/// Borrowed rather than copied: the caller already owns the frame, and a 8 MiB memcpy per message
/// to hand back bytes it is holding is a cost with nothing on the other side.
pub fn verify_frame<'a>(key: &Key, signed: &'a [u8]) -> Result<&'a [u8], FerroError> {
    if signed.len() < MAC_LEN {
        return Err(FerroError::Wal(format!(
            "a consensus frame is {} byte(s) long and cannot hold its {MAC_LEN}-byte authentication \
             tag. This node signs its traffic, so a frame too short to carry a tag is either from a \
             peer that is not signing or is not from a peer at all",
            signed.len()
        )));
    }
    let (mac, body) = signed.split_at(MAC_LEN);
    if !key.verify(body, mac) {
        return Err(FerroError::Wal(format!(
            "a consensus frame of {} byte(s) did not authenticate against this node's signing key. \
             It was refused before it was parsed, so nothing in it reached the state machine. The \
             two ordinary causes are a peer holding a different key file and a peer not signing at \
             all; the third is what this check exists for — someone who can reach this port \
             asserting a term they have no right to.",
            body.len()
        )));
    }
    Ok(body)
}

#[cfg(test)]
#[path = "tests_signing.rs"]
mod tests_signing;
