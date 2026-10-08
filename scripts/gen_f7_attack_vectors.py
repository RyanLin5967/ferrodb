import hmac, hashlib

DOMAIN = b"ferrodb/consensus/mac/1\x00" + bytes([ord('C'), 2])
assert len(DOMAIN) == 26, len(DOMAIN)

def key(n): return bytes((i*7 + 3) & 0xff for i in range(n))
def msg(n): return bytes((i*13 + 129) & 0xff for i in range(n))

def H(k, m): return hmac.new(k, m, hashlib.sha256).hexdigest()

KEY_LENS = [0,1,20,31,32,33,63,64,65,66,96,128,129,131,200]
MSG_LENS = [0,1,25,31,32,33,54,55,56,63,64,65,111,112,119,120,127,128,129,191,192,1000]

print("/// (key_len, msg_len, expected) -- HMAC-SHA256 over the byte patterns in `key_of`/`msg_of`,")
print("/// computed by CPython's `hmac`/`hashlib`, never by this crate. Generator command in the")
print("/// module header.")
print("const HMAC_VECTORS: &[(usize, usize, &str)] = &[")
for kl in KEY_LENS:
    for ml in MSG_LENS:
        print('    (%d, %d, "%s"),' % (kl, ml, H(key(kl), msg(ml))))
print("];")
print()

# Body lengths chosen around every block boundary the 26-byte domain prefix can straddle:
# the inner hash absorbs 64 bytes of ipad, then DOMAIN, then the body, so the boundary that
# matters is 26+n mod 64, and the length-padding boundary is 26+n == 56 (mod 64).
BODY_LENS = [0,1,25,26,27,28,29,30,31,32,33,36,37,38,39,40,53,54,55,63,64,65,
             90,93,94,95,101,102,103,127,128,129,166,167,1000,4096]
TAG_KEY_LENS = [32,33,63,64,65,131]
print("/// (key_len, body_len, expected) -- HMAC-SHA256 over DOMAIN || body_of(body_len), i.e. what")
print("/// `Key::tag` must produce. Same external generator.")
print("const TAG_VECTORS: &[(usize, usize, &str)] = &[")
for kl in TAG_KEY_LENS:
    for bl in BODY_LENS:
        print('    (%d, %d, "%s"),' % (kl, bl, H(key(kl), DOMAIN + msg(bl))))
print("];")
