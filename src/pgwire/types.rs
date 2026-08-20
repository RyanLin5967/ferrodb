//! Type OIDs, and the two wire encodings every value can travel in.
//!
//! ## Why binary is not optional
//!
//! The simple query protocol only ever sends text, so this server started with text alone. The
//! extended protocol lets the *client* choose, and a real driver does: asyncpg registers binary
//! codecs for `bool`, `int2`, `int4`, `int8`, `float4`, `float8`, `text`/`varchar` and `numeric`,
//! and `Bind` asks for every one of those columns in binary. Answering a binary request with text
//! is not a graceful degradation — the client hands the bytes straight to a binary decoder, so
//! `SELECT 1` comes back as a four-byte integer read out of the ASCII `"1"` and no error is
//! raised anywhere.
//!
//! So both directions are implemented here, for exactly these types:
//!
//! | ferrodb `Value` | OID           | binary encoding                                    |
//! |-----------------|---------------|----------------------------------------------------|
//! | `Boolean`       | 16 `bool`     | one byte, 0 or 1                                    |
//! | `BigInt`        | 20 `int8`     | 8 bytes, big-endian two's complement                |
//! | `Integer`       | 23 `int4`     | 4 bytes, big-endian two's complement                |
//! | `Varchar`       | 25 `text`     | the UTF-8 bytes, which is also its text format      |
//! | `Float`         | 701 `float8`  | 8 bytes, IEEE-754 big-endian                        |
//! | `Decimal`       | 1700 `numeric`| sign/weight/dscale header + base-10000 digits       |
//! | `Timestamp`     | 20 `int8`     | epoch milliseconds — see [`oid_of`] for why not 1114 |
//!
//! Parameters arriving from a client are decoded from the same set, plus the narrower `int2` and
//! `float4` and the other string-ish OIDs a driver may pick. Anything else is **refused by name**
//! rather than guessed at: a wrong guess about a parameter's encoding is a wrong row written to a
//! table, with no error anywhere.

use crate::catalog::column::{DataType, Value};
use crate::parser::scanner::TokenType;

/// Postgres type OIDs. These are fixed constants of the protocol, not choices.
pub mod oid {
    pub const BOOL: i32 = 16;
    pub const NAME: i32 = 19;
    pub const INT8: i32 = 20;
    pub const INT2: i32 = 21;
    pub const INT4: i32 = 23;
    pub const TEXT: i32 = 25;
    pub const FLOAT4: i32 = 700;
    pub const FLOAT8: i32 = 701;
    pub const UNKNOWN: i32 = 705;
    pub const BPCHAR: i32 = 1042;
    pub const VARCHAR: i32 = 1043;
    pub const NUMERIC: i32 = 1700;
}

/// The OID this server announces for a value of this kind.
pub fn oid_of(v: &Value) -> i32 {
    match v {
        Value::Integer(_) => oid::INT4,
        Value::Float(_) => oid::FLOAT8,
        Value::Boolean(_) => oid::BOOL,
        Value::BigInt(_) => oid::INT8,
        // `numeric`'s text format is exactly the digit string this type already holds, so a
        // conforming client reads it back without loss.
        Value::Decimal(_) => oid::NUMERIC,
        // Deliberately NOT `timestamp` (1114). This engine stores epoch milliseconds and has no
        // calendar formatter, so it would send `1700000000000` where a conforming client expects
        // `2023-11-14 22:13:20`. Announcing `int8` and sending an integer is true; announcing
        // `timestamp` and sending an integer is a parse error at every real driver.
        Value::Timestamp(_) => oid::INT8,
        Value::Varchar(_) | Value::Null => oid::TEXT,
    }
}

/// The OID for a *declared* column type, which is what `Describe` has to answer with — it runs
/// before any row exists, so [`oid_of`] has nothing to look at.
pub fn oid_of_type(t: &DataType) -> i32 {
    match t {
        DataType::Integer => oid::INT4,
        DataType::Float => oid::FLOAT8,
        DataType::Varchar(_) => oid::TEXT,
        DataType::Boolean => oid::BOOL,
        DataType::BigInt => oid::INT8,
        DataType::Decimal => oid::NUMERIC,
        DataType::Timestamp => oid::INT8,
    }
}

/// Text-format rendering. `None` is SQL NULL, which the protocol encodes as length -1 rather than
/// as the string "NULL" — a client cannot tell those apart otherwise.
pub fn render(v: &Value) -> Option<String> {
    match v {
        Value::Null => None,
        Value::Integer(i) => Some(i.to_string()),
        Value::Float(f) => Some(f.to_string()),
        Value::Boolean(b) => Some(if *b { "t".into() } else { "f".into() }),
        Value::BigInt(i) => Some(i.to_string()),
        Value::Decimal(d) => Some(d.clone()),
        Value::Timestamp(ms) => Some(ms.to_string()),
        Value::Varchar(s) => Some(s.clone()),
    }
}

/// Encode one value for the wire, in the format the client asked for and under the OID this
/// server already announced for that column.
///
/// **The OID is an obligation, not a hint.** `RowDescription` promised a type before the row
/// existed; if the row then holds something else, encoding it anyway is how a client ends up
/// decoding four bytes of text as an integer. Every mismatch is refused here, naming both sides,
/// because the alternative is silent corruption at the client.
pub fn encode_value(v: &Value, type_oid: i32, format: i16) -> Result<Option<Vec<u8>>, String> {
    if matches!(v, Value::Null) {
        return Ok(None);
    }
    if format == 0 {
        // Text format is the same rendering for every type, which is the whole point of it.
        return Ok(render(v).map(|s| s.into_bytes()));
    }
    let bytes = match type_oid {
        oid::BOOL => match v {
            Value::Boolean(b) => vec![if *b { 1u8 } else { 0u8 }],
            other => return Err(mismatch("bool", other)),
        },
        oid::INT4 => match v {
            Value::Integer(i) => i.to_be_bytes().to_vec(),
            other => return Err(mismatch("int4", other)),
        },
        oid::INT8 => match v {
            Value::BigInt(i) => i.to_be_bytes().to_vec(),
            Value::Timestamp(ms) => ms.to_be_bytes().to_vec(),
            // A column declared BIGINT can hold a literal that bound as Integer, so widening here
            // is the ordinary case and not a coercion of convenience: i32 -> i64 is exact.
            Value::Integer(i) => (*i as i64).to_be_bytes().to_vec(),
            other => return Err(mismatch("int8", other)),
        },
        oid::FLOAT8 => match v {
            Value::Float(f) => f.to_be_bytes().to_vec(),
            Value::Integer(i) => (*i as f64).to_be_bytes().to_vec(),
            other => return Err(mismatch("float8", other)),
        },
        oid::NUMERIC => match v {
            Value::Decimal(d) => numeric_to_binary(d)?,
            Value::Integer(i) => numeric_to_binary(&i.to_string())?,
            Value::BigInt(i) => numeric_to_binary(&i.to_string())?,
            other => return Err(mismatch("numeric", other)),
        },
        // Every string-ish type's binary format IS its text format: the raw bytes. That is why a
        // described-as-text column can carry any value kind at all, which is what makes it the
        // honest answer for a computed column whose type this server cannot infer.
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::UNKNOWN => {
            render(v).unwrap_or_default().into_bytes()
        }
        other => {
            return Err(format!(
                "this server has no binary encoding for type oid {other}; it announces only \
                 bool, int4, int8, float8, numeric and text"
            ))
        }
    };
    Ok(Some(bytes))
}

fn mismatch(declared: &str, got: &Value) -> String {
    format!(
        "column was described as `{declared}` but the row holds {got:?}; the description and the \
         row must agree or the client decodes the wrong bytes"
    )
}

/// A decoded parameter, in the shape the SQL layer already understands: the literal token kind and
/// its text. Substituting *this* into the statement — rather than splicing text into the SQL — is
/// what keeps a parameter a value. See `params::substitute`.
#[derive(Clone, Debug, PartialEq)]
pub struct ParamLiteral {
    pub token: TokenType,
    pub text: String,
}

impl ParamLiteral {
    fn number(text: impl Into<String>) -> Self {
        ParamLiteral { token: TokenType::Number, text: text.into() }
    }
}

/// Decode one bound parameter into a literal.
///
/// `format` is 0 for text and 1 for binary, chosen per-parameter by the client. `type_oid` is what
/// this server said it would accept in `ParameterDescription` — a client is entitled to hold the
/// server to it, so anything that does not decode under that OID is refused rather than
/// reinterpreted.
pub fn decode_param(type_oid: i32, format: i16, bytes: &[u8]) -> Result<ParamLiteral, String> {
    if format == 0 {
        let text = std::str::from_utf8(bytes)
            .map_err(|e| format!("text-format parameter is not UTF-8: {e}"))?;
        return decode_text_param(type_oid, text);
    }
    if format != 1 {
        return Err(format!("unknown parameter format code {format}; only 0 (text) and 1 (binary) exist"));
    }
    match type_oid {
        oid::BOOL => {
            let b = only(bytes, 1, "bool")?;
            match b[0] {
                0 => Ok(ParamLiteral { token: TokenType::False, text: "FALSE".into() }),
                1 => Ok(ParamLiteral { token: TokenType::True, text: "TRUE".into() }),
                other => Err(format!("binary bool must be 0 or 1, got {other}")),
            }
        }
        oid::INT2 => {
            let b = only(bytes, 2, "int2")?;
            Ok(ParamLiteral::number(i16::from_be_bytes([b[0], b[1]]).to_string()))
        }
        oid::INT4 => {
            let b = only(bytes, 4, "int4")?;
            Ok(ParamLiteral::number(i32::from_be_bytes([b[0], b[1], b[2], b[3]]).to_string()))
        }
        oid::INT8 => {
            let b = only(bytes, 8, "int8")?;
            let v = i64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]);
            Ok(ParamLiteral::number(v.to_string()))
        }
        oid::FLOAT4 => {
            let b = only(bytes, 4, "float4")?;
            float_literal(f32::from_be_bytes([b[0], b[1], b[2], b[3]]) as f64)
        }
        oid::FLOAT8 => {
            let b = only(bytes, 8, "float8")?;
            float_literal(f64::from_be_bytes([b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7]]))
        }
        oid::NUMERIC => Ok(ParamLiteral::number(numeric_from_binary(bytes)?)),
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::UNKNOWN => {
            let s = std::str::from_utf8(bytes)
                .map_err(|e| format!("binary text parameter is not UTF-8: {e}"))?;
            Ok(ParamLiteral { token: TokenType::String, text: s.to_string() })
        }
        other => Err(format!(
            "this server cannot decode a binary parameter of type oid {other}; it accepts bool, \
             int2, int4, int8, float4, float8, numeric and the string types"
        )),
    }
}

fn decode_text_param(type_oid: i32, text: &str) -> Result<ParamLiteral, String> {
    match type_oid {
        oid::BOOL => match text.trim().to_ascii_lowercase().as_str() {
            "t" | "true" | "y" | "yes" | "on" | "1" => {
                Ok(ParamLiteral { token: TokenType::True, text: "TRUE".into() })
            }
            "f" | "false" | "n" | "no" | "off" | "0" => {
                Ok(ParamLiteral { token: TokenType::False, text: "FALSE".into() })
            }
            other => Err(format!("`{other}` is not a boolean")),
        },
        oid::INT2 | oid::INT4 | oid::INT8 | oid::FLOAT4 | oid::FLOAT8 | oid::NUMERIC => {
            // Validated, not trusted: the text goes on to be read as a number by the binder, and
            // an unchecked string here would arrive there as a bind error with no mention of which
            // parameter caused it.
            let t = text.trim();
            let body = t.strip_prefix('-').or_else(|| t.strip_prefix('+')).unwrap_or(t);
            let numeric = !body.is_empty()
                && body.chars().all(|c| c.is_ascii_digit() || c == '.')
                && body.chars().filter(|c| *c == '.').count() <= 1
                && body.chars().any(|c| c.is_ascii_digit());
            if !numeric {
                return Err(format!("`{text}` is not a number"));
            }
            // Keep the sign attached to the text. `Binder::signed_numeric_text` reads a literal's
            // own text, so `-5` binds the same way whether it arrived as a token pair or like this.
            Ok(ParamLiteral::number(t.strip_prefix('+').unwrap_or(t)))
        }
        oid::TEXT | oid::VARCHAR | oid::BPCHAR | oid::NAME | oid::UNKNOWN => {
            Ok(ParamLiteral { token: TokenType::String, text: text.to_string() })
        }
        other => Err(format!("this server cannot decode a parameter of type oid {other}")),
    }
}

fn only<'a>(bytes: &'a [u8], n: usize, what: &str) -> Result<&'a [u8], String> {
    if bytes.len() != n {
        return Err(format!("binary {what} must be exactly {n} bytes, got {}", bytes.len()));
    }
    Ok(bytes)
}

/// A float parameter, rendered so it stays a float.
///
/// `f64::to_string` prints `5` for `5.0`, and a bare `5` binds as an *integer* literal — which
/// then fails to be written into a `FLOAT` column with a width error, or compares under integer
/// rules. The trailing `.0` is what keeps the type it arrived as.
fn float_literal(v: f64) -> Result<ParamLiteral, String> {
    if !v.is_finite() {
        // `Value::Float` can hold these, but no literal text parses back into them here, so a
        // client sending one would get an unrelated bind error much later. Say it now.
        return Err(format!("`{v}` has no literal form in this SQL dialect"));
    }
    let s = v.to_string();
    let s = if s.contains('.') || s.contains('e') || s.contains('E') {
        s
    } else {
        format!("{s}.0")
    };
    Ok(ParamLiteral::number(s))
}

// ---- numeric, the one type whose binary format is not just its bytes -------------------------
//
// `numeric` travels as: int16 ndigits, int16 weight, uint16 sign, uint16 dscale, then `ndigits`
// int16 digits in base 10000, most significant first. The value is
// `sum(digits[i] * 10000^(weight - i))`, and `dscale` is how many decimal places to *display* —
// it carries the trailing zeros that make `1.50` different bytes from `1.5` while still comparing
// equal, which is exactly the distinction `Value::Decimal` exists to keep.

const NUMERIC_POS: u16 = 0x0000;
const NUMERIC_NEG: u16 = 0x4000;
const NUMERIC_NAN: u16 = 0xC000;

/// Encode decimal digit text as a `numeric`.
pub fn numeric_to_binary(text: &str) -> Result<Vec<u8>, String> {
    let t = text.trim();
    let (neg, body) = match t.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, t.strip_prefix('+').unwrap_or(t)),
    };
    let (int_part, frac_part) = match body.split_once('.') {
        Some((i, f)) => (i, f),
        None => (body, ""),
    };
    if int_part.is_empty() && frac_part.is_empty() {
        return Err(format!("`{text}` is not a decimal number"));
    }
    if !int_part.chars().all(|c| c.is_ascii_digit()) || !frac_part.chars().all(|c| c.is_ascii_digit())
    {
        return Err(format!("`{text}` is not a decimal number"));
    }
    let dscale = frac_part.len();

    // Pad so both halves split into whole base-10000 digits: the integer half from the right, the
    // fractional half from the left, because the decimal point is where the grouping starts.
    let ip = format!("{}{}", "0".repeat((4 - int_part.len() % 4) % 4), int_part);
    let fp = format!("{}{}", frac_part, "0".repeat((4 - frac_part.len() % 4) % 4));

    let mut digits: Vec<i16> = Vec::with_capacity((ip.len() + fp.len()) / 4);
    for chunk in ip.as_bytes().chunks(4) {
        digits.push(std::str::from_utf8(chunk).unwrap().parse::<i16>().unwrap());
    }
    let int_groups = digits.len() as i32;
    for chunk in fp.as_bytes().chunks(4) {
        digits.push(std::str::from_utf8(chunk).unwrap().parse::<i16>().unwrap());
    }
    let mut weight = int_groups - 1;

    // Leading zero groups are not information; dropping one moves the weight down with it.
    let lead = digits.iter().take_while(|d| **d == 0).count();
    digits.drain(..lead);
    weight -= lead as i32;
    // Trailing zero groups are not information either, and `dscale` still remembers the display
    // width, so `1.50` survives as digits [1, 5000] with dscale 2 rather than losing its zero.
    while digits.last() == Some(&0) {
        digits.pop();
    }
    if digits.is_empty() {
        weight = 0;
    }

    let mut out = Vec::with_capacity(8 + digits.len() * 2);
    out.extend_from_slice(&(digits.len() as i16).to_be_bytes());
    out.extend_from_slice(&(weight as i16).to_be_bytes());
    // Zero is neither positive nor negative: `-0.00` must not come back as `-0`.
    let sign = if neg && !digits.is_empty() { NUMERIC_NEG } else { NUMERIC_POS };
    out.extend_from_slice(&sign.to_be_bytes());
    out.extend_from_slice(&(dscale as u16).to_be_bytes());
    for d in digits {
        out.extend_from_slice(&d.to_be_bytes());
    }
    Ok(out)
}

/// Decode a `numeric` back into decimal digit text.
pub fn numeric_from_binary(b: &[u8]) -> Result<String, String> {
    if b.len() < 8 {
        return Err(format!("a numeric header is 8 bytes, got {}", b.len()));
    }
    let ndigits = i16::from_be_bytes([b[0], b[1]]);
    let weight = i16::from_be_bytes([b[2], b[3]]) as i32;
    let sign = u16::from_be_bytes([b[4], b[5]]);
    let dscale = u16::from_be_bytes([b[6], b[7]]) as usize;
    if ndigits < 0 {
        return Err(format!("numeric claims {ndigits} digits"));
    }
    let ndigits = ndigits as usize;
    if b.len() != 8 + ndigits * 2 {
        return Err(format!(
            "numeric claims {ndigits} digits, which needs {} bytes, but carries {}",
            8 + ndigits * 2,
            b.len()
        ));
    }
    if sign == NUMERIC_NAN {
        // Refused rather than rendered: `Value::Decimal` holds digit text, and there is no digit
        // text for NaN. Storing the string "NaN" would compare as a number nowhere.
        return Err("this server has no NaN decimal".into());
    }
    if sign != NUMERIC_POS && sign != NUMERIC_NEG {
        return Err(format!("unknown numeric sign 0x{sign:04x} (±Infinity is not supported)"));
    }
    let digit = |i: i32| -> i16 {
        if i < 0 || i as usize >= ndigits {
            0
        } else {
            let o = 8 + (i as usize) * 2;
            i16::from_be_bytes([b[o], b[o + 1]])
        }
    };

    let mut int_text = String::new();
    if weight < 0 {
        int_text.push('0');
    } else {
        for i in 0..=weight {
            let d = digit(i);
            if i == 0 {
                int_text.push_str(&d.to_string());
            } else {
                int_text.push_str(&format!("{d:04}"));
            }
        }
    }

    let mut frac_text = String::new();
    let mut i = weight + 1;
    while frac_text.len() < dscale {
        frac_text.push_str(&format!("{:04}", digit(i)));
        i += 1;
    }
    frac_text.truncate(dscale);

    let sign_text = if sign == NUMERIC_NEG { "-" } else { "" };
    if dscale == 0 {
        Ok(format!("{sign_text}{int_text}"))
    } else {
        Ok(format!("{sign_text}{int_text}.{frac_text}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn values_map_to_the_postgres_type_oids_a_client_expects() {
        assert_eq!(oid_of(&Value::Integer(1)), 23);
        assert_eq!(oid_of(&Value::Float(1.0)), 701);
        assert_eq!(oid_of(&Value::Boolean(true)), 16);
        assert_eq!(oid_of(&Value::Varchar("x".into())), 25);
        assert_eq!(oid_of(&Value::Decimal("1.50".into())), 1700);
        assert_eq!(oid_of(&Value::BigInt(1)), 20);
        assert_eq!(oid_of(&Value::Timestamp(1)), 20);
    }

    #[test]
    fn a_declared_column_type_maps_to_the_same_oid_as_a_value_of_it() {
        // Describe answers from the declared type and rows answer from the value. If those two
        // disagreed, every prepared statement would announce one type and deliver another.
        assert_eq!(oid_of_type(&DataType::Integer), oid_of(&Value::Integer(0)));
        assert_eq!(oid_of_type(&DataType::Float), oid_of(&Value::Float(0.0)));
        assert_eq!(oid_of_type(&DataType::Boolean), oid_of(&Value::Boolean(true)));
        assert_eq!(oid_of_type(&DataType::BigInt), oid_of(&Value::BigInt(0)));
        assert_eq!(oid_of_type(&DataType::Decimal), oid_of(&Value::Decimal("0".into())));
        assert_eq!(oid_of_type(&DataType::Timestamp), oid_of(&Value::Timestamp(0)));
        assert_eq!(oid_of_type(&DataType::Varchar(8)), oid_of(&Value::Varchar(String::new())));
    }

    #[test]
    fn booleans_render_as_t_and_f_the_way_postgres_text_format_does() {
        assert_eq!(render(&Value::Boolean(true)).as_deref(), Some("t"));
        assert_eq!(render(&Value::Boolean(false)).as_deref(), Some("f"));
        assert_eq!(render(&Value::Null), None);
    }

    /// Breaking shape: `SELECT 1` under a binary-format request. Text `"1"` is one byte; a binary
    /// `int4` is four. A client that asked for binary and got the text byte reads whatever three
    /// bytes follow it in the buffer.
    #[test]
    fn binary_integers_are_fixed_width_big_endian() {
        assert_eq!(encode_value(&Value::Integer(1), oid::INT4, 1).unwrap(), Some(vec![0, 0, 0, 1]));
        assert_eq!(encode_value(&Value::Integer(1), oid::INT4, 0).unwrap(), Some(b"1".to_vec()));
        assert_eq!(
            encode_value(&Value::BigInt(-2), oid::INT8, 1).unwrap(),
            Some(vec![0xff; 7].into_iter().chain([0xfe]).collect::<Vec<u8>>())
        );
        assert_eq!(encode_value(&Value::Boolean(true), oid::BOOL, 1).unwrap(), Some(vec![1]));
        assert_eq!(
            encode_value(&Value::Float(1.5), oid::FLOAT8, 1).unwrap(),
            Some(1.5f64.to_be_bytes().to_vec())
        );
    }

    #[test]
    fn null_is_null_in_both_formats_and_never_the_word() {
        assert_eq!(encode_value(&Value::Null, oid::INT4, 1).unwrap(), None);
        assert_eq!(encode_value(&Value::Null, oid::TEXT, 0).unwrap(), None);
    }

    /// Breaking shape: a column described `int4` whose row holds a string. Encoding it as text
    /// bytes under a binary int4 promise is a client-side misread with no error anywhere, so this
    /// must be refused at the server.
    #[test]
    fn a_row_that_disagrees_with_its_description_is_refused_not_encoded() {
        let e = encode_value(&Value::Varchar("nope".into()), oid::INT4, 1).unwrap_err();
        assert!(e.contains("int4"), "{e}");
        assert!(encode_value(&Value::Boolean(true), oid::INT8, 1).is_err());
        // ...but a text-described column carries anything, which is what makes it the safe answer
        // for a computed column of unknown type.
        assert_eq!(
            encode_value(&Value::Integer(7), oid::TEXT, 1).unwrap(),
            Some(b"7".to_vec())
        );
    }

    #[test]
    fn an_unencodable_oid_is_named_rather_than_guessed() {
        let e = encode_value(&Value::Integer(1), 1114, 1).unwrap_err();
        assert!(e.contains("1114"), "{e}");
    }

    #[test]
    fn binary_parameters_decode_to_the_literal_the_binder_already_understands() {
        assert_eq!(decode_param(oid::INT4, 1, &7i32.to_be_bytes()).unwrap(), ParamLiteral::number("7"));
        assert_eq!(
            decode_param(oid::INT8, 1, &(-9007199254740993i64).to_be_bytes()).unwrap(),
            ParamLiteral::number("-9007199254740993"),
            "a bigint past 2^53 must survive as digits, not as a float"
        );
        assert_eq!(
            decode_param(oid::TEXT, 1, "hello".as_bytes()).unwrap(),
            ParamLiteral { token: TokenType::String, text: "hello".into() }
        );
        assert_eq!(decode_param(oid::BOOL, 1, &[1]).unwrap().token, TokenType::True);
        assert_eq!(decode_param(oid::BOOL, 1, &[0]).unwrap().token, TokenType::False);
    }

    /// Breaking shape: `5.0` sent as a float8 parameter. `f64::to_string` prints `5`, which binds
    /// as an integer literal — and an integer written into a FLOAT column is a hard width error,
    /// because `serialize` lays down four bytes where `deserialize` reads eight.
    #[test]
    fn a_whole_float_parameter_stays_a_float() {
        let p = decode_param(oid::FLOAT8, 1, &5.0f64.to_be_bytes()).unwrap();
        assert_eq!(p.text, "5.0");
        assert!(p.text.contains('.'), "without the point this binds as an integer");
    }

    #[test]
    fn a_short_binary_parameter_is_refused_rather_than_padded() {
        assert!(decode_param(oid::INT4, 1, &[0, 0, 1]).is_err());
        assert!(decode_param(oid::INT8, 1, &[]).is_err());
        assert!(decode_param(oid::BOOL, 1, &[2]).is_err(), "a bool is 0 or 1 and nothing else");
        assert!(decode_param(9999, 1, &[0]).is_err(), "an unknown oid must be refused by name");
        assert!(decode_param(oid::FLOAT8, 1, &f64::NAN.to_be_bytes()).is_err());
    }

    #[test]
    fn text_parameters_decode_under_their_declared_oid() {
        assert_eq!(decode_param(oid::INT4, 0, b"42").unwrap(), ParamLiteral::number("42"));
        assert_eq!(decode_param(oid::INT4, 0, b"-42").unwrap(), ParamLiteral::number("-42"));
        assert_eq!(decode_param(oid::BOOL, 0, b"true").unwrap().token, TokenType::True);
        assert_eq!(
            decode_param(oid::TEXT, 0, b"o'brien").unwrap(),
            ParamLiteral { token: TokenType::String, text: "o'brien".into() },
            "a quote in a text parameter is data, and never becomes SQL"
        );
        assert!(decode_param(oid::INT4, 0, b"1; DROP TABLE t").is_err());
        assert!(decode_param(oid::INT4, 0, b"").is_err());
        assert!(decode_param(oid::INT4, 0, b"..").is_err());
    }

    #[test]
    fn numeric_encodes_to_the_documented_base_10000_shape() {
        // 1.50 = digits [1, 5000], weight 0, sign +, dscale 2. Worked out by hand from the
        // protocol description rather than from this implementation.
        let b = numeric_to_binary("1.50").unwrap();
        assert_eq!(i16::from_be_bytes([b[0], b[1]]), 2, "ndigits");
        assert_eq!(i16::from_be_bytes([b[2], b[3]]), 0, "weight");
        assert_eq!(u16::from_be_bytes([b[4], b[5]]), 0, "sign +");
        assert_eq!(u16::from_be_bytes([b[6], b[7]]), 2, "dscale");
        assert_eq!(i16::from_be_bytes([b[8], b[9]]), 1);
        assert_eq!(i16::from_be_bytes([b[10], b[11]]), 5000);
    }

    #[test]
    fn numeric_round_trips_including_the_trailing_zeros_that_are_information() {
        // `1.50` and `1.5` are equal numbers and different bytes; the dscale is what keeps them
        // apart, and losing it is exactly the digit loss `Value::Decimal` exists to prevent.
        for text in [
            "0", "1", "-1", "1.50", "1.5", "0.5", "-0.5", "12345", "99999999", "0.0001",
            "123456789012345678901234567890.123456789", "-99999999999999999999.99", "0.00",
            "10000", "1000000000000", "0.000000000001",
        ] {
            let round = numeric_from_binary(&numeric_to_binary(text).unwrap()).unwrap();
            assert_eq!(round, text, "numeric {text} did not survive the wire");
        }
    }

    #[test]
    fn numeric_zero_has_no_sign_and_no_digits() {
        let b = numeric_to_binary("-0.00").unwrap();
        assert_eq!(i16::from_be_bytes([b[0], b[1]]), 0, "zero has no significant digits");
        assert_eq!(u16::from_be_bytes([b[4], b[5]]), 0, "zero is not negative");
        assert_eq!(numeric_from_binary(&b).unwrap(), "0.00");
    }

    #[test]
    fn a_malformed_numeric_is_refused_rather_than_read_past_its_end() {
        assert!(numeric_from_binary(&[0, 1, 0, 0]).is_err(), "a short header must be refused");
        // Claims one digit, carries none.
        assert!(numeric_from_binary(&[0, 1, 0, 0, 0, 0, 0, 0]).is_err());
        // NaN and ±Infinity have no representation in `Value::Decimal`.
        assert!(numeric_from_binary(&[0, 0, 0, 0, 0xC0, 0x00, 0, 0]).is_err());
        assert!(numeric_from_binary(&[0, 0, 0, 0, 0xD0, 0x00, 0, 0]).is_err());
        assert!(numeric_to_binary("not a number").is_err());
        assert!(numeric_to_binary("").is_err());
        assert!(numeric_to_binary("1.2.3").is_err());
    }
}
