//! SHA-256, written here because this crate has no runtime dependencies.
//!
//! `RunEntity::prompt_hash` exists so that a prompt containing customer data does not become a
//! durable copy of it: the run is identified by a digest, and the prompt itself is never written
//! anywhere. That argument only holds if the digest is a real one. Until now the field was
//! `[0u8; 32]` at every site that set it, which is not a weak hash — it is *no* hash, and it made
//! every run's "prompt identity" identical, so `same_actor` could not tell two prompts apart and
//! the field's whole stated purpose was unmet while looking met.
//!
//! # Why not a cheaper hash
//!
//! The field is a *privacy* boundary, not a hash-table key. A short or non-cryptographic digest
//! (FNV, CRC, a truncated hash) is invertible in practice for the thing that matters here: a prompt
//! drawn from a small set — "approve refund for account 4471", one per account — is recovered by
//! enumerating candidates and comparing digests. A 256-bit preimage-resistant digest is what makes
//! "the prompt is not stored" true rather than aspirational.
//!
//! # What is verified
//!
//! FIPS 180-4's own worked examples, the empty string, a one-million-character message, and the
//! block-boundary lengths where padding logic goes wrong (55, 56, 63, 64, 119, 120 bytes). Those
//! vectors are the instrument: a hash function that agrees with itself proves nothing, so the
//! expected digests below come from the published standard and from nothing in this file.

/// Round constants: the first 32 bits of the fractional parts of the cube roots of the first 64
/// primes (FIPS 180-4 §4.2.2). A single wrong constant changes every vector in this file's tests.
const K: [u32; 64] = [
    0x428a2f98, 0x71374491, 0xb5c0fbcf, 0xe9b5dba5, 0x3956c25b, 0x59f111f1, 0x923f82a4, 0xab1c5ed5,
    0xd807aa98, 0x12835b01, 0x243185be, 0x550c7dc3, 0x72be5d74, 0x80deb1fe, 0x9bdc06a7, 0xc19bf174,
    0xe49b69c1, 0xefbe4786, 0x0fc19dc6, 0x240ca1cc, 0x2de92c6f, 0x4a7484aa, 0x5cb0a9dc, 0x76f988da,
    0x983e5152, 0xa831c66d, 0xb00327c8, 0xbf597fc7, 0xc6e00bf3, 0xd5a79147, 0x06ca6351, 0x14292967,
    0x27b70a85, 0x2e1b2138, 0x4d2c6dfc, 0x53380d13, 0x650a7354, 0x766a0abb, 0x81c2c92e, 0x92722c85,
    0xa2bfe8a1, 0xa81a664b, 0xc24b8b70, 0xc76c51a3, 0xd192e819, 0xd6990624, 0xf40e3585, 0x106aa070,
    0x19a4c116, 0x1e376c08, 0x2748774c, 0x34b0bcb5, 0x391c0cb3, 0x4ed8aa4a, 0x5b9cca4f, 0x682e6ff3,
    0x748f82ee, 0x78a5636f, 0x84c87814, 0x8cc70208, 0x90befffa, 0xa4506ceb, 0xbef9a3f7, 0xc67178f2,
];

/// Initial hash value: the first 32 bits of the fractional parts of the square roots of the first
/// eight primes (FIPS 180-4 §5.3.3).
const H0: [u32; 8] = [
    0x6a09e667, 0xbb67ae85, 0x3c6ef372, 0xa54ff53a, 0x510e527f, 0x9b05688c, 0x1f83d9ab, 0x5be0cd19,
];

/// A streaming SHA-256 state.
///
/// Streaming rather than one-shot-only because a prompt is not necessarily one contiguous string,
/// and because the identity record hashes several fields in sequence. A caller that has one slice
/// uses [`sha256`].
#[derive(Clone)]
pub struct Sha256 {
    h: [u32; 8],
    /// Partial block. Never reaches 64 bytes: it is compressed the moment it fills.
    block: [u8; 64],
    used: usize,
    /// Total message length in BYTES.
    ///
    /// `u128` so that the bytes-to-bits multiplication in `finish` cannot overflow before it is
    /// encoded — in a `u64` it would wrap at 2^61 bytes, which in a debug build panics and in a
    /// release build silently produces the wrong digest.
    ///
    /// **The encoded field is still 64 bits, and that is the algorithm's own limit rather than a
    /// shortcut here:** FIPS 180-4 defines SHA-256 for messages shorter than 2^64 bits, and its
    /// length block is exactly 64 bits wide. A longer message has no defined digest at all, so
    /// there is nothing this implementation could encode instead. It does not detect that case, and
    /// this comment says so rather than implying a guard that is not there — 2^61 bytes is 2 EiB,
    /// which no caller of `prompt_digest` can construct.
    len: u128,
}

impl Default for Sha256 {
    fn default() -> Self {
        Sha256::new()
    }
}

impl Sha256 {
    pub fn new() -> Self {
        Sha256 { h: H0, block: [0u8; 64], used: 0, len: 0 }
    }

    pub fn update(&mut self, mut data: &[u8]) {
        self.len += data.len() as u128;
        // Finish any partial block first, then take whole blocks straight from the input.
        if self.used > 0 {
            let want = 64 - self.used;
            let take = want.min(data.len());
            self.block[self.used..self.used + take].copy_from_slice(&data[..take]);
            self.used += take;
            data = &data[take..];
            if self.used == 64 {
                let block = self.block;
                self.compress(&block);
                self.used = 0;
            }
        }
        while data.len() >= 64 {
            let (head, rest) = data.split_at(64);
            let mut b = [0u8; 64];
            b.copy_from_slice(head);
            self.compress(&b);
            data = rest;
        }
        if !data.is_empty() {
            self.block[..data.len()].copy_from_slice(data);
            self.used = data.len();
        }
    }

    /// The digest. Consumes the state, because SHA-256's padding is applied once and a state that
    /// has been finalised is not a state a further `update` may extend.
    pub fn finish(mut self) -> [u8; 32] {
        let bit_len = self.len * 8;
        // 0x80, then zeroes, then the 64-bit big-endian bit length in the last 8 bytes.
        self.pad_byte(0x80);
        while self.used != 56 {
            self.pad_byte(0x00);
        }
        let bits = (bit_len as u64).to_be_bytes();
        for b in bits {
            self.pad_byte(b);
        }
        debug_assert_eq!(self.used, 0, "padding must end on a block boundary");

        let mut out = [0u8; 32];
        for (i, word) in self.h.iter().enumerate() {
            out[i * 4..i * 4 + 4].copy_from_slice(&word.to_be_bytes());
        }
        out
    }

    /// Append one padding byte, compressing when the block fills. Does **not** touch `len`, which
    /// counts message bytes only — padding is not message.
    fn pad_byte(&mut self, b: u8) {
        self.block[self.used] = b;
        self.used += 1;
        if self.used == 64 {
            let block = self.block;
            self.compress(&block);
            self.used = 0;
        }
    }

    fn compress(&mut self, block: &[u8; 64]) {
        let mut w = [0u32; 64];
        for i in 0..16 {
            w[i] = u32::from_be_bytes(block[i * 4..i * 4 + 4].try_into().unwrap());
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            // Wrapping is the specification's arithmetic, not an overflow being tolerated: every
            // addition in SHA-256 is modulo 2^32.
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let [mut a, mut b, mut c, mut d, mut e, mut f, mut g, mut h] = self.h;
        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let t1 = h
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let t2 = s0.wrapping_add(maj);

            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(t1);
            d = c;
            c = b;
            b = a;
            a = t1.wrapping_add(t2);
        }
        for (dst, src) in self.h.iter_mut().zip([a, b, c, d, e, f, g, h]) {
            *dst = dst.wrapping_add(src);
        }
    }
}

/// SHA-256 of one slice.
pub fn sha256(data: &[u8]) -> [u8; 32] {
    let mut s = Sha256::new();
    s.update(data);
    s.finish()
}

/// The digest of a prompt, for [`crate::provenance::RunEntity::prompt_hash`].
///
/// A separate name from [`sha256`] on purpose: this is the one function that decides what a
/// prompt's durable identity is, so a change to it is visible as a change to *that*, not as an
/// incidental change to a general-purpose hash.
///
/// **An empty prompt hashes like any other input and is not special-cased to zero.** `[0u8; 32]`
/// is the value that means "never hashed" — the state this field was stuck in — so producing it
/// for a real (if empty) prompt would put a genuine run back into the indistinguishable state.
/// `sha256("")` is `e3b0c442...`, which is non-zero.
pub fn prompt_digest(prompt: &str) -> [u8; 32] {
    sha256(prompt.as_bytes())
}

/// Lowercase hex, for the change feed and for error messages.
pub fn to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Parse lowercase or uppercase hex back into bytes. `None` on any non-hex byte or an odd length.
pub fn from_hex(s: &str) -> Option<Vec<u8>> {
    if s.len() % 2 != 0 {
        return None;
    }
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len() / 2);
    let nib = |c: u8| -> Option<u8> {
        match c {
            b'0'..=b'9' => Some(c - b'0'),
            b'a'..=b'f' => Some(c - b'a' + 10),
            b'A'..=b'F' => Some(c - b'A' + 10),
            _ => None,
        }
    };
    for pair in b.chunks(2) {
        out.push((nib(pair[0])? << 4) | nib(pair[1])?);
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_of(data: &[u8]) -> String {
        to_hex(&sha256(data))
    }

    /// **The published vectors, and nothing derived from this file.**
    ///
    /// FIPS 180-4's own worked examples plus the standard long-message vector. An implementation
    /// checked against its own output agrees with itself about any shared mistake; these digests
    /// came from the standard.
    ///
    /// Breaking shape: any single wrong round constant, a rotate in the wrong direction, or a
    /// `>>` where the spec says `rotate_right`, changes every one of these.
    #[test]
    fn fips_180_4_vectors() {
        assert_eq!(
            hex_of(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            hex_of(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            hex_of(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq"),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
        assert_eq!(
            hex_of(
                b"abcdefghbcdefghicdefghijdefghijkefghijklfghijklmghijklmnhijklmno\
                  ijklmnopjklmnopqklmnopqrlmnopqrsmnopqrstnopqrstu"
            ),
            "cf5b16a778af8380036ce59e7b0492370b249b11e8f07a51afac45037afee9d1"
        );
    }

    /// One million 'a's — the vector that catches a length counter that is too narrow or a
    /// streaming path that mishandles many full blocks.
    #[test]
    fn the_one_million_a_vector() {
        let mut s = Sha256::new();
        // Fed in awkward chunk sizes on purpose: 1_000_000 is not a multiple of any of them, so the
        // partial-block path is exercised at every boundary rather than only at the end.
        let chunk = vec![b'a'; 1000];
        for _ in 0..1000 {
            s.update(&chunk);
        }
        assert_eq!(
            to_hex(&s.finish()),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
    }

    /// **The padding boundaries, where hand-written SHA-256 goes wrong.**
    ///
    /// 55 bytes is the largest message whose padding fits in one block; 56 is the first that needs
    /// a second. 63/64 and 119/120 are the same boundary one and two blocks along. An
    /// implementation that pads with `while used < 56` rather than `while used != 56` produces a
    /// correct digest for every length below 56 and a wrong one for every length at or above it —
    /// which is exactly the shape a test using only "abc" would certify.
    ///
    /// **Where these digests come from:** CPython's `hashlib.sha256` (which is OpenSSL's), read
    /// off with `python3 -c "import hashlib; print(hashlib.sha256(b'a'*63).hexdigest())"`. Named
    /// because an expected value with no stated instrument is indistinguishable from one this file
    /// produced, and a hash checked against itself certifies its own mistakes.
    #[test]
    fn digests_at_every_padding_boundary_match_an_independent_implementation() {
        for (len, want) in [
            (55usize, "9f4390f8d30c2dd92ec9f095b65e2b9ae9b0a925a5258e241c9f1e910f734318"),
            (56, "b35439a4ac6f0948b6d6f9e3c6af0f5f590ce20f1bde7090ef7970686ec6738a"),
            (63, "7d3e74a05d7db15bce4ad9ec0658ea98e3f06eeecf16b4c6fff2da457ddc2f34"),
            (64, "ffe054fe7ae0cb6dc65c3af9b61d5209f439851db43d0ba5997337df154668eb"),
            (119, "31eba51c313a5c08226adf18d4a359cfdfd8d2e816b13f4af952f7ea6584dcfb"),
            (120, "2f3d335432c70b580af0e8e1b3674a7c020d683aa5f73aaaedfdc55af904c21c"),
        ] {
            let msg = vec![b'a'; len];
            assert_eq!(hex_of(&msg), want, "wrong digest for {len} bytes: a padding bug");

            // Streaming in one-byte pieces must reach the same digest, at every boundary.
            let mut s = Sha256::new();
            for b in &msg {
                s.update(std::slice::from_ref(b));
            }
            assert_eq!(
                to_hex(&s.finish()),
                want,
                "streaming and one-shot disagree at {len} bytes"
            );
        }
    }

    /// A one-bit change anywhere must change the digest everywhere. Not a proof of the avalanche
    /// property, but a detector for a compressor that ignores part of its input — which is what a
    /// message-schedule loop with the wrong bounds looks like.
    #[test]
    fn one_flipped_bit_changes_the_digest() {
        let base = vec![b'x'; 200];
        let first = sha256(&base);
        for at in [0usize, 63, 64, 127, 199] {
            let mut other = base.clone();
            other[at] ^= 0x01;
            assert_ne!(sha256(&other), first, "flipping byte {at} did not change the digest");
        }
    }

    /// A prompt digest is a real digest of the prompt, and never the "never hashed" value.
    #[test]
    fn a_prompt_digest_is_non_zero_and_depends_on_the_prompt() {
        let a = prompt_digest("approve the refund for account 4471");
        let b = prompt_digest("approve the refund for account 4472");
        assert_ne!(a, [0u8; 32], "the prompt hash is the all-zero placeholder again");
        assert_ne!(a, b, "two different prompts produced one digest");
        assert_eq!(a, prompt_digest("approve the refund for account 4471"), "not deterministic");
        // Even the empty prompt is hashed rather than special-cased back to the placeholder.
        assert_ne!(prompt_digest(""), [0u8; 32]);
        assert_eq!(
            to_hex(&prompt_digest("")),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn hex_round_trips() {
        let d = sha256(b"round trip");
        assert_eq!(from_hex(&to_hex(&d)).unwrap(), d.to_vec());
        assert_eq!(from_hex("ABCD").unwrap(), vec![0xab, 0xcd]);
        assert!(from_hex("abc").is_none(), "an odd-length hex string is not bytes");
        assert!(from_hex("zz").is_none(), "non-hex must be refused, not silently zeroed");
    }
}
