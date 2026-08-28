import hmac, hashlib

DOMAIN = b"ferrodb/consensus/mac/1\x00" + bytes([ord('C'), 2])

class Xs:
    """xorshift64*, seeded per row. Reproduced byte for byte in the Rust test."""
    def __init__(self, seed):
        self.s = seed & 0xFFFFFFFFFFFFFFFF
    def next(self):
        x = self.s
        x ^= (x >> 12) & 0xFFFFFFFFFFFFFFFF
        x ^= (x << 25) & 0xFFFFFFFFFFFFFFFF
        x ^= (x >> 27) & 0xFFFFFFFFFFFFFFFF
        self.s = x & 0xFFFFFFFFFFFFFFFF
        return (self.s * 0x2545F4914F6CDD1D) & 0xFFFFFFFFFFFFFFFF
    def bytes(self, n):
        return bytes(self.next() & 0xFF for _ in range(n))

print("/// (seed, key_len, msg_len, expected) -- pseudo-random key AND message bytes, so the tables")
print("/// above are not all affine sequences. `Xorshift` below reproduces the generator exactly.")
print("const RANDOM_VECTORS: &[(u64, usize, usize, &str)] = &[")
rows = 0
for i in range(200):
    seed = 0x9E3779B97F4A7C15 ^ (i * 0x100000001B3)
    seed &= 0xFFFFFFFFFFFFFFFF
    if seed == 0: seed = 1
    r = Xs(seed)
    key_len = 1 + (r.next() % 200)
    msg_len = r.next() % 300
    k = r.bytes(key_len)
    m = r.bytes(msg_len)
    print('    (%d, %d, %d, "%s"),' % (seed, key_len, msg_len, hmac.new(k, m, hashlib.sha256).hexdigest()))
    rows += 1
print("];")
print()
print("/// The same rows, but authenticated the way a frame is: HMAC over DOMAIN || body. Only rows")
print("/// whose key reaches the 32-byte minimum, because those are the ones a `Key` can hold.")
print("const RANDOM_TAG_VECTORS: &[(u64, usize, usize, &str)] = &[")
tag_rows = 0
for i in range(200):
    seed = 0x9E3779B97F4A7C15 ^ (i * 0x100000001B3)
    seed &= 0xFFFFFFFFFFFFFFFF
    if seed == 0: seed = 1
    r = Xs(seed)
    key_len = 1 + (r.next() % 200)
    msg_len = r.next() % 300
    k = r.bytes(key_len)
    m = r.bytes(msg_len)
    if key_len < 32:
        continue
    print('    (%d, %d, %d, "%s"),' % (seed, key_len, msg_len, hmac.new(k, DOMAIN + m, hashlib.sha256).hexdigest()))
    tag_rows += 1
print("];")
import sys
print("// rows=%d tag_rows=%d" % (rows, tag_rows), file=sys.stderr)
