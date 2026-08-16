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
}

#[derive(Debug, Clone)]
pub enum Value {
    Integer(i32),
    Float(f64),
    Varchar(String),
    Boolean(bool),
    Null
}


impl Column {
    pub fn new(name: String, data_type: DataType, nullable: bool) -> Self {
        Column {name, data_type, nullable}
    }
}

// Cross-type ordering falls back to `type_rank`, which is fine for genuinely incomparable
// variants (a string is not less than a bool in any meaningful sense, we just need SOME total
// order). It is NOT fine for Integer vs Float: rank alone made `Float(2.0) > Integer(5)` true
// unconditionally, because Float outranks Integer. That silently defeated every numeric guard —
// `Delta::apply` promotes an Integer cell to Float on a float delta, after which a floor check
// like `qty >= 0` compared Float against Integer and passed on RANK, never on value.
// So the two numeric variants compare numerically; everything else keeps the rank fallback.
//
// i32 -> f64 is lossless (f64 carries a 53-bit mantissa, i32 needs 32), so widening is exact.
// Transitivity survives because every Integer and every Float sits in the same rank band
// relative to the other variants: both are above Null/Boolean and below Varchar.
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
            (a, b) => type_rank(a).cmp(&type_rank(b))
        }
    }
}

fn type_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Boolean(_) => 1,
        Value::Integer(_) => 2,
        Value::Float(_) => 3,
        Value::Varchar(_) => 4,
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
}