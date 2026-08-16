#[derive(Debug, Clone, PartialEq)]

pub struct Column {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool
}

// add support for more later
#[derive(Debug, Clone, PartialEq)]
pub enum DataType {
    Integer,
    Float,
    Varchar(u16),
    Boolean,
    /// 64-bit signed integer. Distinct from `Integer` because `Integer` is `i32` and a great many
    /// real keys (snowflake ids, epoch-nanosecond stamps, account numbers) do not fit in one.
    BigInt,
    /// Exact decimal, held as its digit text rather than as a binary float. See [`Value::Decimal`]
    /// for why the text and not a scaled integer.
    Decimal,
    /// Milliseconds since the Unix epoch, signed so pre-1970 instants are representable.
    Timestamp,
}

#[derive(Debug, Clone)]
pub enum Value {
    Integer(i32),
    Float(f64),
    Varchar(String),
    Boolean(bool),
    /// `i64`. Its extremes are ±9.2e18, which is **past** the 2^53 an IEEE double represents
    /// exactly — which is the entire reason the change feed ships this type as a JSON string.
    BigInt(i64),
    /// An exact decimal, stored as the digit text the user wrote.
    ///
    /// **Why text and not a scaled integer.** A scaled integer (`units: i128, scale: u8`) is the
    /// other obvious choice and it was rejected for two reasons. First, it needs a declared
    /// precision and scale on the column — `DECIMAL(18,4)` — and therefore a rounding rule for
    /// every literal that does not fit it. Rounding is precisely the silent digit loss this type
    /// exists to prevent, so introducing it here would defeat the point. Second, `i128` caps the
    /// significant digits at 38; text has no cap, so a 60-digit ledger amount survives.
    ///
    /// The cost is real and stated: comparison parses the text (see `decimal_cmp`), and there is
    /// no decimal arithmetic — this engine stores and ships decimals, it does not add them.
    ///
    /// The stored text is exactly what was written, scale included: `1.50` stays `1.50` and does
    /// not become `1.5`, because trailing zeros are significant to a consumer reading a price.
    /// Comparison is numeric, so `1.50` and `1.5` are still *equal*; only the bytes differ.
    Decimal(String),
    /// Milliseconds since the Unix epoch. `i64` for the same reason as [`Value::BigInt`]: epoch
    /// millis for the year 2100 is 4.1e12, comfortably inside a double, but epoch *micros* or
    /// *nanos* are not, and a consumer that parses the column into a double has already lost the
    /// ability to notice. It ships as a string for the same reason.
    Timestamp(i64),
    Null
}


impl Column {
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Column {name, data_type, nullable}
    }
}

/// Validate and normalise decimal literal text.
///
/// Accepts an optional sign, digits, and at most one decimal point with at least one digit on some
/// side of it. Returns the text with a leading `+` removed and a bare `.5` / `5.` filled out to
/// `0.5` / `5.0`, so every stored decimal has a digit on both sides of the point. Nothing else is
/// changed — in particular trailing zeros are kept, because their presence is information.
pub fn parse_decimal(text: &str) -> Result<String, String> {
    let t = text.trim();
    if t.is_empty() {
        return Err("empty decimal".into());
    }
    let (neg, rest) = match t.as_bytes()[0] {
        b'-' => (true, &t[1..]),
        b'+' => (false, &t[1..]),
        _ => (false, t),
    };
    let mut parts = rest.splitn(2, '.');
    let int_part = parts.next().unwrap_or("");
    let frac_part = parts.next().unwrap_or("");
    if rest.matches('.').count() > 1 {
        return Err(format!("more than one decimal point: {text}"));
    }
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(format!("no digits in decimal: {text}"));
    }
    if !int_part.bytes().all(|b| b.is_ascii_digit()) || !frac_part.bytes().all(|b| b.is_ascii_digit())
    {
        return Err(format!("not a decimal: {text}"));
    }
    let int_part = if int_part.is_empty() { "0" } else { int_part };
    let frac_part = if frac_part.is_empty() { "0" } else { frac_part };
    Ok(format!("{}{}.{}", if neg { "-" } else { "" }, int_part, frac_part))
}

/// Split validated decimal text into (negative, integer digits without leading zeros, fraction
/// digits without trailing zeros). Both digit strings may be empty, which means zero.
fn decimal_parts(s: &str) -> (bool, &str, &str) {
    let (neg, rest) = match s.as_bytes().first() {
        Some(b'-') => (true, &s[1..]),
        Some(b'+') => (false, &s[1..]),
        _ => (false, s),
    };
    let (int_part, frac_part) = match rest.split_once('.') {
        Some((i, f)) => (i, f),
        None => (rest, ""),
    };
    let int_part = int_part.trim_start_matches('0');
    let frac_part = frac_part.trim_end_matches('0');
    (neg, int_part, frac_part)
}

/// Numeric ordering of two decimals held as text.
///
/// Lexicographic ordering of the raw text is wrong in every direction that matters (`"9" > "10"`,
/// `"1.50" != "1.5"`, `"-1" > "-2"`), so the digits are compared positionally: sign first, then
/// integer-part length, then digits left to right, then fraction digits padded with zeros.
pub fn decimal_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    let (a_neg, a_int, a_frac) = decimal_parts(a);
    let (b_neg, b_int, b_frac) = decimal_parts(b);

    let a_zero = a_int.is_empty() && a_frac.is_empty();
    let b_zero = b_int.is_empty() && b_frac.is_empty();
    // -0 and 0 are the same number, so sign must not be consulted when both are zero.
    if a_zero && b_zero {
        return Ordering::Equal;
    }
    if a_zero != b_zero || a_neg != b_neg {
        // Exactly one is zero, or the signs differ. Either way the more-negative one is smaller.
        let a_sign = if a_zero { 0i8 } else if a_neg { -1 } else { 1 };
        let b_sign = if b_zero { 0i8 } else if b_neg { -1 } else { 1 };
        if a_sign != b_sign {
            return a_sign.cmp(&b_sign);
        }
    }

    let magnitude = a_int
        .len()
        .cmp(&b_int.len())
        .then_with(|| a_int.cmp(b_int))
        .then_with(|| {
            let n = a_frac.len().max(b_frac.len());
            let pad = |s: &str, i: usize| s.as_bytes().get(i).copied().unwrap_or(b'0');
            (0..n)
                .map(|i| pad(a_frac, i).cmp(&pad(b_frac, i)))
                .find(|o| *o != Ordering::Equal)
                .unwrap_or(Ordering::Equal)
        });
    if a_neg { magnitude.reverse() } else { magnitude }
}

/// The exact decimal expansion of a finite `f64`.
///
/// Every finite binary float is a terminating decimal; the longest one (the smallest subnormal,
/// 2^-1074) needs 1074 fractional digits. Rust's `{:.N}` is correctly rounded, so asking for 1074
/// digits prints the value exactly rather than an approximation of it. This is what lets
/// `Decimal` and `Float` compare without either side being widened into the other's error.
fn f64_exact_decimal(f: f64) -> String {
    debug_assert!(f.is_finite());
    format!("{f:.1074}")
}

/// True when validated decimal text denotes a value strictly below zero.
fn decimal_is_negative(s: &str) -> bool {
    let (neg, int_part, frac_part) = decimal_parts(s);
    neg && !(int_part.is_empty() && frac_part.is_empty())
}

/// Compare a finite-or-not `f64` against decimal text, exactly.
///
/// Three cases do not go through the digits, and each is a fixed answer independent of the decimal
/// — which is exactly what keeps the ordering transitive:
///
/// * NaN. `total_cmp` places +NaN above and -NaN below every real number, and this agrees.
/// * ±Infinity, likewise.
/// * `-0.0`. `total_cmp` treats it as strictly below `0.0`, so a `Float` carrying it is a distinct
///   value from a `Float` carrying `+0.0`. Decimal has no signed zero (`-0.0` and `0` are the same
///   number, as in SQL `NUMERIC`), so `-0.0f64` has to sit strictly below *every* decimal zero or
///   the equalities cycle: `Decimal("0") == Float(-0.0) == ...` while `Float(-0.0) < Float(0.0)`.
fn cmp_f64_decimal(f: f64, d: &str) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    if f.is_nan() {
        return if f.is_sign_negative() { Ordering::Less } else { Ordering::Greater };
    }
    if f.is_infinite() {
        return if f > 0.0 { Ordering::Greater } else { Ordering::Less };
    }
    if f == 0.0 && f.is_sign_negative() {
        return if decimal_is_negative(d) { Ordering::Greater } else { Ordering::Less };
    }
    decimal_cmp(&f64_exact_decimal(f), d)
}

/// Compare an `i64` against an `f64` without widening the integer.
///
/// `i as f64` is lossy past 2^53, which would make `BigInt(i64::MAX)` and
/// `BigInt(i64::MAX - 1)` both compare equal to the same float — the exact class of silent error
/// this whole feature exists to remove. NaN keeps `total_cmp`'s placement so mixed comparison
/// agrees with float-to-float comparison about where NaN sits.
fn cmp_i64_f64(a: i64, b: f64) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    if b.is_nan() {
        return (a as f64).total_cmp(&b);
    }
    if b == f64::INFINITY {
        return Ordering::Less;
    }
    if b == f64::NEG_INFINITY {
        return Ordering::Greater;
    }
    // `-0.0` sits strictly below `0.0` under `total_cmp`, and `Integer` already agrees with that
    // via `(a as f64).total_cmp(b)`. `BigInt` has to agree too, or `Integer(0) > Float(-0.0)` and
    // `BigInt(0) == Float(-0.0)` would contradict `Integer(0) == BigInt(0)`.
    if b == 0.0 && b.is_sign_negative() {
        return a.cmp(&0).then(Ordering::Greater);
    }
    // 2^63 is the first float above every i64; -2^63 is exactly i64::MIN.
    if b >= 9223372036854775808.0 {
        return Ordering::Less;
    }
    if b < -9223372036854775808.0 {
        return Ordering::Greater;
    }
    let truncated = b.trunc();
    // `truncated` is in [-2^63, 2^63) and integral, so the cast is exact.
    let as_int = truncated as i64;
    match a.cmp(&as_int) {
        Ordering::Equal => {
            // Equal integer parts: the fractional part decides.
            if b > truncated {
                Ordering::Less
            } else if b < truncated {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        }
        other => other,
    }
}

// Cross-type ordering falls back to `type_rank`, which is fine for genuinely incomparable
// variants (a string is not less than a bool in any meaningful sense, we just need SOME total
// order). It is NOT fine for Integer vs Float: rank alone made `Float(2.0) > Integer(5)` true
// unconditionally, because Float outranks Integer. That silently defeated every numeric guard —
// `Delta::apply` promotes an Integer cell to Float on a float delta, after which a floor check
// like `qty >= 0` compared Float against Integer and passed on RANK, never on value.
// So the numeric variants compare numerically; everything else keeps the rank fallback.
//
// i32 -> f64 is lossless (f64 carries a 53-bit mantissa, i32 needs 32), so widening is exact.
// i64 -> f64 is NOT, so BigInt never widens: `cmp_i64_f64` compares exactly. Decimal never widens
// either: `cmp_f64_decimal` expands the float to its exact decimal digits instead.
//
// Transitivity survives because the four numeric variants — Integer, BigInt, Float, Decimal —
// occupy one contiguous rank band (ranks 2..5), every pair inside it compares by value, and every
// comparison is exact, so no two of them can disagree about equality. Timestamp deliberately does
// NOT join that band: an epoch-millis stamp is not the same kind of quantity as a price, and
// letting it compare numerically against only *some* of the band would create a rank/value cycle.
// It compares with Timestamp and otherwise by rank, uniformly, which is cycle-free.
impl Ord for Value {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use std::cmp::Ordering;
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.total_cmp(b),
            (Value::Integer(a), Value::Float(b)) => (*a as f64).total_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.total_cmp(&(*b as f64)),
            (Value::Boolean(a), Value::Boolean(b)) => a.cmp(b),
            (Value::Varchar(a), Value::Varchar(b)) => a.cmp(b),
            (Value::Null, Value::Null) => Ordering::Equal,

            (Value::BigInt(a), Value::BigInt(b)) => a.cmp(b),
            (Value::BigInt(a), Value::Integer(b)) => a.cmp(&(*b as i64)),
            (Value::Integer(a), Value::BigInt(b)) => (*a as i64).cmp(b),
            (Value::BigInt(a), Value::Float(b)) => cmp_i64_f64(*a, *b),
            (Value::Float(a), Value::BigInt(b)) => cmp_i64_f64(*b, *a).reverse(),

            (Value::Decimal(a), Value::Decimal(b)) => decimal_cmp(a, b),
            (Value::Decimal(a), Value::Integer(b)) => decimal_cmp(a, &b.to_string()),
            (Value::Integer(a), Value::Decimal(b)) => decimal_cmp(&a.to_string(), b),
            (Value::Decimal(a), Value::BigInt(b)) => decimal_cmp(a, &b.to_string()),
            (Value::BigInt(a), Value::Decimal(b)) => decimal_cmp(&a.to_string(), b),
            (Value::Decimal(a), Value::Float(b)) => cmp_f64_decimal(*b, a).reverse(),
            (Value::Float(a), Value::Decimal(b)) => cmp_f64_decimal(*a, b),

            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),

            (a, b) => type_rank(a).cmp(&type_rank(b))
        }
    }
}

fn type_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Boolean(_) => 1,
        // Ranks 2..5 are the numeric band. Keep them contiguous: see the comment on `Ord`.
        Value::Integer(_) => 2,
        Value::BigInt(_) => 3,
        Value::Float(_) => 4,
        Value::Decimal(_) => 5,
        Value::Timestamp(_) => 6,
        Value::Varchar(_) => 7,
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == std::cmp::Ordering::Equal
    }
}

impl Eq for Value {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;

    // Each of these three FAILS on the pre-fix implementation, where cross-type comparison
    // fell through to type_rank and Float(rank 3) beat Integer(rank 2) regardless of value.

    #[test]
    fn float_does_not_outrank_a_larger_integer() {
        assert_eq!(Value::Float(2.0).cmp(&Value::Integer(5)), Ordering::Less);
        assert_eq!(Value::Integer(5).cmp(&Value::Float(2.0)), Ordering::Greater);
        assert!(Value::Integer(5) > Value::Float(2.0));
        assert!(!(Value::Float(2.0) > Value::Integer(5)));
    }

    #[test]
    fn a_floor_guard_rejects_a_negative_float() {
        // The concrete failure this bug caused: `qty >= 0` where a float delta promoted the
        // cell to Float. Pre-fix this passed on rank alone and a negative quantity merged.
        let qty = Value::Float(-1.0);
        let floor = Value::Integer(0);
        assert!(!(qty >= floor), "a negative float must not satisfy `qty >= 0`");
    }

    #[test]
    fn numerically_equal_int_and_float_compare_equal() {
        assert_eq!(Value::Integer(5).cmp(&Value::Float(5.0)), Ordering::Equal);
        assert_eq!(Value::Integer(0).cmp(&Value::Float(0.0)), Ordering::Equal);
        assert!(Value::Float(0.0) >= Value::Integer(0));
    }

    #[test]
    fn rank_fallback_still_orders_incomparable_variants() {
        assert!(Value::Null < Value::Boolean(false));
        assert!(Value::Boolean(true) < Value::Integer(-9999));
        assert!(Value::Integer(9999) < Value::Varchar("a".into()));
        assert!(Value::Float(1e30) < Value::Varchar("a".into()));
    }

    #[test]
    fn ordering_is_transitive_across_the_numeric_band() {
        // Both numeric variants sit in one rank band (above Boolean, below Varchar), which is
        // what keeps the mixed numeric/rank comparison a valid total order.
        let b = Value::Boolean(true);
        let i = Value::Integer(3);
        let f = Value::Float(7.5);
        let s = Value::Varchar("z".into());
        assert!(b < i && i < f && f < s);
        assert!(b < f && b < s && i < s);
    }

    #[test]
    fn integer_to_float_widening_is_exact_at_i32_bounds() {
        assert_eq!(Value::Integer(i32::MAX).cmp(&Value::Float(i32::MAX as f64)), Ordering::Equal);
        assert_eq!(Value::Integer(i32::MIN).cmp(&Value::Float(i32::MIN as f64)), Ordering::Equal);
        assert!(Value::Integer(i32::MAX) < Value::Float(i32::MAX as f64 + 1.0));
    }

    // ---- wide numeric and temporal types ----

    #[test]
    fn bigint_orders_by_value_at_the_i64_extremes() {
        assert!(Value::BigInt(i64::MIN) < Value::BigInt(0));
        assert!(Value::BigInt(0) < Value::BigInt(i64::MAX));
        assert!(Value::BigInt(i64::MAX - 1) < Value::BigInt(i64::MAX));
        assert_eq!(Value::BigInt(7).cmp(&Value::Integer(7)), Ordering::Equal);
        assert!(Value::Integer(i32::MAX) < Value::BigInt(i32::MAX as i64 + 1));
    }

    /// **The load-bearing one.** `i as f64` collapses these two distinct integers onto the same
    /// double. If `cmp_i64_f64` widened instead of comparing exactly, the second assertion here
    /// would report Equal and two different rows would sort as one.
    #[test]
    fn bigint_versus_float_does_not_widen_past_2_53() {
        let a = 9007199254740993i64; // 2^53 + 1, not representable as f64
        let f = 9007199254740992.0f64; // 2^53
        assert_eq!(a as f64, f, "premise: the naive widening really does collapse these");
        assert_eq!(Value::BigInt(a).cmp(&Value::Float(f)), Ordering::Greater);
        assert_eq!(Value::Float(f).cmp(&Value::BigInt(a)), Ordering::Less);

        // i64::MAX widens to 2^63, which is strictly greater than i64::MAX.
        assert_eq!(Value::BigInt(i64::MAX).cmp(&Value::Float(i64::MAX as f64)), Ordering::Less);
        assert_eq!(Value::BigInt(i64::MIN).cmp(&Value::Float(i64::MIN as f64)), Ordering::Equal);
    }

    #[test]
    fn bigint_versus_non_finite_floats_matches_total_cmp_placement() {
        assert!(Value::BigInt(i64::MAX) < Value::Float(f64::INFINITY));
        assert!(Value::BigInt(i64::MIN) > Value::Float(f64::NEG_INFINITY));
        // total_cmp puts +NaN above +inf and -NaN below -inf; mixed comparison must agree.
        assert!(Value::BigInt(0) < Value::Float(f64::NAN));
        assert!(Value::BigInt(0) > Value::Float(-f64::NAN));
    }

    #[test]
    fn decimal_orders_numerically_not_lexicographically() {
        // Every one of these is the opposite of what a string compare would say.
        assert!(Value::Decimal("9".into()) < Value::Decimal("10".into()));
        assert!(Value::Decimal("-2".into()) < Value::Decimal("-1".into()));
        assert!(Value::Decimal("2".into()) < Value::Decimal("10.0".into()));
        assert_eq!(
            Value::Decimal("1.50".into()).cmp(&Value::Decimal("1.5".into())),
            Ordering::Equal,
            "trailing zeros change the bytes, not the number"
        );
        assert_eq!(
            Value::Decimal("-0.0".into()).cmp(&Value::Decimal("0".into())),
            Ordering::Equal
        );
        assert!(Value::Decimal("0.0000001".into()) > Value::Decimal("0".into()));
        assert!(Value::Decimal("-0.0000001".into()) < Value::Decimal("0".into()));
    }

    /// A decimal guard must reject a negative value. This is the `qty >= 0` bug re-checked for
    /// the new type: on a rank-only fallback it would pass because Decimal outranks Integer.
    #[test]
    fn a_floor_guard_rejects_a_negative_decimal() {
        assert!(!(Value::Decimal("-1.00".into()) >= Value::Integer(0)));
        assert!(Value::Decimal("0.00".into()) >= Value::Integer(0));
        assert!(Value::Decimal("0.01".into()) > Value::Integer(0));
    }

    #[test]
    fn decimal_beyond_i128_still_compares_exactly() {
        // 60 significant digits: past anything a scaled i128 could have held.
        let a = "123456789012345678901234567890123456789012345678901234567890";
        let b = "123456789012345678901234567890123456789012345678901234567891";
        assert!(Value::Decimal(a.into()) < Value::Decimal(b.into()));
        assert_eq!(Value::Decimal(a.into()).cmp(&Value::Decimal(a.into())), Ordering::Equal);
    }

    /// Decimal versus Float compares against the float's *exact* decimal expansion, not against a
    /// shortest-round-trip rendering of it. `0.1f64` is really 0.1000000000000000055511151231...,
    /// so it is strictly greater than the decimal `0.1`.
    #[test]
    fn decimal_versus_float_uses_the_floats_exact_value() {
        assert_eq!(
            Value::Decimal("0.1".into()).cmp(&Value::Float(0.1)),
            Ordering::Less,
            "0.1f64 is slightly above the decimal 0.1"
        );
        assert_eq!(Value::Decimal("0.5".into()).cmp(&Value::Float(0.5)), Ordering::Equal);
        assert_eq!(Value::Decimal("2".into()).cmp(&Value::Float(2.0)), Ordering::Equal);
        assert_eq!(Value::Decimal("-0.5".into()).cmp(&Value::Float(-0.5)), Ordering::Equal);
    }

    #[test]
    fn decimal_versus_non_finite_floats_is_fixed_and_cycle_free() {
        for d in ["0", "-999999999999999999999", "999999999999999999999"] {
            assert!(Value::Decimal(d.into()) < Value::Float(f64::INFINITY));
            assert!(Value::Decimal(d.into()) > Value::Float(f64::NEG_INFINITY));
            assert!(Value::Decimal(d.into()) < Value::Float(f64::NAN));
            assert!(Value::Decimal(d.into()) > Value::Float(-f64::NAN));
        }
    }

    /// The whole numeric band must be one total order. This walks every pair of a mixed sample and
    /// checks antisymmetry and transitivity directly rather than assuming the rank layout is right.
    #[test]
    fn the_numeric_band_is_a_total_order() {
        let vals = vec![
            Value::Integer(-5),
            Value::Integer(0),
            Value::Integer(7),
            Value::BigInt(-9007199254740993),
            Value::BigInt(0),
            Value::BigInt(9007199254740993),
            Value::BigInt(i64::MAX),
            Value::Float(-1.5),
            Value::Float(-0.0),
            Value::Float(0.0),
            Value::Float(7.0),
            Value::Float(1e300),
            Value::Decimal("-5.000".into()),
            Value::Decimal("-0.0".into()),
            Value::Decimal("0".into()),
            Value::Decimal("7".into()),
            Value::Decimal("9007199254740993".into()),
        ];
        for a in &vals {
            for b in &vals {
                assert_eq!(
                    a.cmp(b),
                    b.cmp(a).reverse(),
                    "antisymmetry broken for {a:?} vs {b:?}"
                );
                for c in &vals {
                    if a <= b && b <= c {
                        assert!(a <= c, "transitivity broken: {a:?} <= {b:?} <= {c:?} but not {a:?} <= {c:?}");
                    }
                }
            }
        }
    }

    /// Timestamp deliberately does not join the numeric band. Whatever the answer is, it must be
    /// the *same* answer against every member of that band, or the order has a cycle in it.
    #[test]
    fn timestamp_compares_by_value_and_sits_outside_the_numeric_band() {
        assert!(Value::Timestamp(i64::MIN) < Value::Timestamp(0));
        assert!(Value::Timestamp(0) < Value::Timestamp(i64::MAX));
        assert_eq!(Value::Timestamp(5).cmp(&Value::Timestamp(5)), Ordering::Equal);

        let band = [
            Value::Integer(i32::MAX),
            Value::BigInt(i64::MAX),
            Value::Float(f64::INFINITY),
            Value::Decimal("999999999999999999999999".into()),
        ];
        for v in &band {
            assert_eq!(
                Value::Timestamp(i64::MIN).cmp(v),
                Ordering::Greater,
                "Timestamp must outrank every numeric variant uniformly, or the order cycles"
            );
        }
    }

    #[test]
    fn the_whole_value_domain_is_a_total_order() {
        let vals = vec![
            Value::Null,
            Value::Boolean(false),
            Value::Boolean(true),
            Value::Integer(-1),
            Value::Integer(3),
            Value::BigInt(i64::MIN),
            Value::BigInt(9007199254740993),
            Value::Float(f64::NEG_INFINITY),
            Value::Float(-0.0),
            Value::Float(0.0),
            Value::Float(0.25),
            Value::Float(f64::NAN),
            Value::Float(-f64::NAN),
            Value::Decimal("-0.0".into()),
            Value::Decimal("0".into()),
            Value::Decimal("12345678901234567890.123".into()),
            Value::Timestamp(-1),
            Value::Timestamp(1_700_000_000_000),
            Value::Varchar("".into()),
            Value::Varchar("zz".into()),
        ];
        for a in &vals {
            for b in &vals {
                assert_eq!(a.cmp(b), b.cmp(a).reverse(), "antisymmetry: {a:?} vs {b:?}");
                for c in &vals {
                    if a <= b && b <= c {
                        assert!(a <= c, "transitivity: {a:?} <= {b:?} <= {c:?}");
                    }
                }
            }
        }
    }

    #[test]
    fn parse_decimal_accepts_what_sql_writes_and_refuses_what_it_does_not() {
        assert_eq!(parse_decimal("123.4500").unwrap(), "123.4500");
        assert_eq!(parse_decimal("-0.5").unwrap(), "-0.5");
        assert_eq!(parse_decimal("+7").unwrap(), "7.0");
        assert_eq!(parse_decimal(".5").unwrap(), "0.5");
        assert_eq!(parse_decimal("5.").unwrap(), "5.0");
        assert_eq!(parse_decimal("0").unwrap(), "0.0");
        for bad in ["", ".", "-", "1.2.3", "1e5", "abc", "1 2", "--1"] {
            assert!(parse_decimal(bad).is_err(), "`{bad}` was accepted as a decimal");
        }
    }

    #[test]
    fn f64_exact_decimal_is_exact_not_shortest() {
        let s = f64_exact_decimal(0.1);
        assert!(
            s.starts_with("0.1000000000000000055511151231257827"),
            "expected the exact binary value of 0.1, got {}",
            &s[..40.min(s.len())]
        );
        assert!(f64_exact_decimal(0.5).starts_with("0.5000000000"));
    }
}
