//! E10 — the change feed as newline-delimited JSON.
//!
//! [`super::logical`] produces `ChangeEvent`s in memory, which only a Rust caller inside this
//! process can use. A CDC source is defined by what a **consumer** can read, so the feed needs a
//! representation that leaves the process.
//!
//! JSON Lines: one self-contained JSON object per line. That shape is chosen for two properties
//! rather than for familiarity. Each line is independently parseable, so a consumer that dies
//! halfway through a file resumes by reading forward to the next newline instead of re-parsing from
//! the start; and the output is inspectable with `cat`, which matters more than it sounds when the
//! question is "did the database really emit that".
//!
//! ```text
//! {"table":"inventory","op":"INSERT","txn":2,"lsn":216,"commit_lsn":422,"commit_end_lsn":455,
//!  "before":null,"after":{"id":1,"qty":10}}
//! ```
//!
//! `before`/`after` are objects keyed by column name, not arrays. A positional array would require
//! the consumer to hold this database's catalog to know what column three is, which defeats the
//! purpose of having a wire format at all.
//!
//! # The two things that are actually hard
//!
//! **String escaping.** A `VARCHAR` can hold quotes, backslashes, newlines, tabs and control
//! characters. Emitting any of them raw produces a document that breaks the consumer's parser —
//! and it breaks it at *some later line*, so the damage is attributed to the wrong record. This is
//! the reason the tests validate output with **Python's `json` module** rather than with anything
//! written here: an encoder checked against its own decoder agrees with itself about a shared
//! misreading, which is the same argument the pgwire tests make for using an independently written
//! client.
//!
//! **Values JSON cannot represent.** `NaN`, `Infinity` and `-Infinity` are not JSON numbers.
//! Rust's `{}` prints them as bare `NaN`/`inf`, which is invalid JSON — one such value poisons the
//! whole document for a strict parser. There is no good option here, only a least-bad one, so it is
//! chosen explicitly and stated:
//!
//! - Emitting them bare produces a document nothing can parse. **Worst.**
//! - Emitting `null` keeps the document valid and silently destroys the distinction between "no
//!   value" and "not a number".
//! - Emitting the strings `"NaN"`, `"Infinity"`, `"-Infinity"` keeps the document valid and
//!   preserves the information, at the cost of that column changing JSON type for those rows.
//!
//! The third is used. A consumer that sees a string where it expected a number has been told
//! something true and can act on it; one that sees `null` has been told something false.
//!
//! # Why BIGINT, DECIMAL and TIMESTAMP ship as JSON strings
//!
//! JSON has one number type and no stated precision. In practice the overwhelmingly common
//! consumer behaviour is to parse every JSON number into an **IEEE 754 double**: that is what
//! JavaScript's `JSON.parse` does, what Python's `json` does for anything with a decimal point,
//! what Go's `encoding/json` does into `interface{}`, and what almost every dynamically typed
//! pipeline does by default. A double carries a 53-bit significand, so:
//!
//! * `9223372036854775807` (`i64::MAX`) comes back as `9223372036854775808` — off by one, and
//!   larger than the type it came from.
//! * `9007199254740993` (2^53 + 1) comes back as `9007199254740992`.
//! * `0.1` comes back as `0.1000000000000000055511151231257827…`, and a decimal with more than 17
//!   significant digits comes back rounded.
//!
//! None of that raises an error. The parse succeeds, the number is wrong, and the corruption is
//! discovered — if ever — downstream of the system that could have prevented it. A payment ledger
//! that reconciles to the cent and a job queue keyed on a snowflake id both fail this way silently.
//!
//! So `BIGINT`, `DECIMAL` and `TIMESTAMP` are emitted as **JSON strings**. A string is not
//! coerced by any JSON parser: the digits arrive at the consumer byte-for-byte as they left, and
//! the consumer decides what to widen them into with full knowledge of what it is doing. The cost
//! is that the consumer must call `strconv.ParseInt`/`BigInt(...)`/its own decimal type rather
//! than reading a number field — an explicit step that can fail loudly, replacing an implicit one
//! that fails quietly.
//!
//! `INTEGER` is deliberately **not** included: it is `i32`, whose extremes are ±2.1e9, three
//! orders of magnitude inside what a double represents exactly. There is no precision to lose, and
//! turning it into a string would break every consumer reading that column today for no gain.
//! `FLOAT` is not included either — it *is* a double, so a double round-trips it exactly, and the
//! shortest-round-trip printing below is what makes that true.

use std::io::Write;

use crate::catalog::column::Value;
use crate::error::FerroError;

use super::logical::{ChangeEvent, ChangeOp, SchemaChange};

/// Append `s` to `out` as a quoted, escaped JSON string.
///
/// Handles the two mandatory escapes (`"` and `\`), the short forms JSON defines for common control
/// characters, and `\u00XX` for everything else below 0x20. Characters at or above 0x20 other than
/// those two are passed through, which is correct for UTF-8: JSON strings are Unicode and Rust
/// `str` is already valid UTF-8, so no transcoding is needed or wanted.
pub fn escape_json_into(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            // Everything else below 0x20 must be escaped; JSON has no short form for these.
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// Append a value as JSON.
///
/// See the module docs for why non-finite floats become strings rather than bare tokens or nulls,
/// and why `BIGINT`/`DECIMAL`/`TIMESTAMP` become strings while `INTEGER` and `FLOAT` do not.
pub fn value_into(v: &Value, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Integer(i) => out.push_str(&i.to_string()),
        // The three string-encoded types. `escape_json_into` is used rather than a hand-built
        // `"..."` so that the quoting is the same code path every other string goes through; the
        // digits themselves need no escaping, but nothing here depends on that staying true.
        Value::BigInt(i) => escape_json_into(&i.to_string(), out),
        Value::Decimal(d) => escape_json_into(d, out),
        Value::Timestamp(ms) => escape_json_into(&ms.to_string(), out),
        Value::Float(f) if f.is_nan() => out.push_str("\"NaN\""),
        Value::Float(f) if f.is_infinite() => {
            out.push_str(if *f > 0.0 { "\"Infinity\"" } else { "\"-Infinity\"" })
        }
        Value::Float(f) => {
            // `{:?}` round-trips an f64 through Rust's shortest-representation printer and always
            // includes a decimal point, so a whole-numbered float stays visibly a float. `{}` would
            // print 1.0 as "1", which a consumer would read back as an integer.
            out.push_str(&format!("{f:?}"));
        }
        Value::Varchar(s) => escape_json_into(s, out),
    }
}

fn row_into(columns: &[String], values: &[Value], out: &mut String) {
    // A row whose value count disagrees with its column count cannot be keyed honestly. Rather than
    // emit a half-labelled object, the extra or missing positions are made visible: surplus values
    // get explicit synthetic keys, so nothing is dropped without a reader noticing.
    out.push('{');
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        match columns.get(i) {
            Some(name) => escape_json_into(name, out),
            None => escape_json_into(&format!("__unnamed_column_{i}"), out),
        }
        out.push(':');
        value_into(v, out);
    }
    out.push('}');
}

/// One change event as a single line of JSON, **without** the trailing newline.
pub fn to_json_line(e: &ChangeEvent) -> String {
    let mut out = String::with_capacity(128);
    out.push_str("{\"table\":");
    escape_json_into(&e.table, &mut out);

    out.push_str(",\"op\":");
    escape_json_into(e.op.name(), &mut out);

    out.push_str(&format!(
        ",\"txn\":{},\"lsn\":{},\"commit_lsn\":{},\"commit_end_lsn\":{}",
        e.txn_id, e.lsn, e.commit_lsn, e.commit_end_lsn
    ));

    // `before` and `after` are always present, null where they do not apply. A consumer branching
    // on which keys exist is a consumer that breaks the first time a key is added; one branching on
    // `op` is not.
    out.push_str(",\"before\":");
    match &e.op {
        // A snapshot row and a schema change have no prior state to report, the same as an insert.
        ChangeOp::Read { .. } | ChangeOp::Insert { .. } | ChangeOp::Schema { .. } => {
            out.push_str("null")
        }
        ChangeOp::Update { old, .. } | ChangeOp::Delete { old } => {
            row_into(&e.columns, old, &mut out)
        }
    }

    out.push_str(",\"after\":");
    match &e.op {
        // **A DROP carries no shape.** E69: this matched every `Schema` event and emitted
        // `{"columns":[]}` for a drop, which the Go consumer refuses outright - `line 3: DROP_TABLE
        // carries an after image`. Producer and validator had never been reconciled because until
        // `DROP TABLE` reached the SQL surface no feed could contain one, so the first real drop
        // failed the whole run.
        //
        // The consumer's rule is the right one: the table is gone, there is no shape to report, and
        // its DROP branch never reads `after`. An empty column list is not "no columns", it is a
        // table with zero columns, and those are different claims.
        ChangeOp::Schema { change: SchemaChange::DropTable, .. } => out.push_str("null"),
        // A schema event's payload is the table's shape, not a row. Keyed under `columns` so a
        // consumer never confuses it with data.
        ChangeOp::Schema { columns, .. } => {
            out.push_str("{\"columns\":[");
            for (i, c) in columns.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                out.push_str("{\"name\":");
                escape_json_into(&c.name, &mut out);
                out.push_str(",\"type\":");
                escape_json_into(&c.sql_type, &mut out);
                out.push_str(",\"nullable\":");
                out.push_str(if c.nullable { "true" } else { "false" });
                out.push('}');
            }
            out.push_str("]}");
        }
        ChangeOp::Read { row } => row_into(&e.columns, row, &mut out),
        ChangeOp::Insert { new } | ChangeOp::Update { new, .. } => {
            row_into(&e.columns, new, &mut out)
        }
        ChangeOp::Delete { .. } => out.push_str("null"),
    }

    out.push('}');
    out
}

/// Write a feed of events as JSON Lines.
///
/// Returns how many lines were written, so a caller can tell "wrote nothing" from "wrote
/// something" without re-reading its own output.
/// Render a table's live rows as one JSON array of objects — the **source** side of a diff.
///
/// # Why this lives here and not in the example that calls it
///
/// The Go consumer re-materializes a table from the change events and can dump what it built. Until
/// now nothing produced the other half of that comparison, so "diff the re-materialized table against
/// the source" had to be done by a test harness holding a hardcoded expectation. This is the source
/// half, and it is in this module on purpose: it reuses [`value_into`], the **same** renderer the feed
/// itself uses, so the two sides agree by construction rather than by two functions happening to make
/// the same choices.
///
/// That matters most for the types a JSON number cannot hold. A `BigInt` past 2^53, a `Decimal` and a
/// `Timestamp` all ship as strings in the feed; a second renderer written for this function would
/// have had to rediscover that, and the first divergence would have looked like a data mismatch in
/// the pipeline rather than a formatting difference in the tooling.
///
/// Row order is by the rows as given and is **not** part of the contract: the consumer indexes both
/// sides by primary key before comparing, so ordering cannot produce a false difference.
pub fn write_table_json<W: Write>(
    columns: &[String],
    rows: &[Vec<Value>],
    w: &mut W,
) -> Result<usize, FerroError> {
    let mut out = String::from("[");
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        row_into(columns, row, &mut out);
    }
    out.push(']');
    writeln!(w, "{out}").map_err(|e| FerroError::Io(format!("write table json: {e}")))?;
    Ok(rows.len())
}

pub fn write_feed<W: Write>(events: &[ChangeEvent], w: &mut W) -> Result<usize, FerroError> {
    let mut n = 0;
    for e in events {
        let line = to_json_line(e);
        writeln!(w, "{line}").map_err(|err| FerroError::Io(format!("write feed: {err}")))?;
        n += 1;
    }
    w.flush().map_err(|err| FerroError::Io(format!("flush feed: {err}")))?;
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    fn event(op: ChangeOp) -> ChangeEvent {
        ChangeEvent {
            txn_id: 2,
            lsn: 216,
            commit_lsn: 422,
            commit_end_lsn: 455,
            table: "inventory".into(),
            columns: Arc::new(vec!["id".into(), "qty".into()]),
            op,
        }
    }

    #[test]
    fn an_insert_line_carries_after_and_a_null_before() {
        let line = to_json_line(&event(ChangeOp::Insert {
            new: vec![Value::Integer(1), Value::Integer(10)],
        }));
        assert!(line.contains("\"op\":\"INSERT\""), "{line}");
        assert!(line.contains("\"before\":null"), "{line}");
        assert!(line.contains("\"after\":{\"id\":1,\"qty\":10}"), "{line}");
        assert!(!line.contains('\n'), "a JSON *line* must not contain a newline: {line}");
    }

    #[test]
    fn a_delete_line_carries_before_and_a_null_after() {
        let line = to_json_line(&event(ChangeOp::Delete {
            old: vec![Value::Integer(2), Value::Integer(20)],
        }));
        assert!(line.contains("\"op\":\"DELETE\""), "{line}");
        assert!(line.contains("\"before\":{\"id\":2,\"qty\":20}"), "{line}");
        assert!(line.contains("\"after\":null"), "{line}");
    }

    #[test]
    fn an_update_line_carries_both_images() {
        let line = to_json_line(&event(ChangeOp::Update {
            old: vec![Value::Integer(1), Value::Integer(10)],
            new: vec![Value::Integer(1), Value::Integer(999)],
        }));
        assert!(line.contains("\"before\":{\"id\":1,\"qty\":10}"), "{line}");
        assert!(line.contains("\"after\":{\"id\":1,\"qty\":999}"), "{line}");
    }

    /// Every escape JSON requires, in one string. If any of these leak through raw, the document
    /// breaks at the consumer — and it breaks on a *later* line, so the blame lands on the wrong
    /// record.
    #[test]
    fn every_character_json_requires_escaping_is_escaped() {
        let mut out = String::new();
        escape_json_into("a\"b\\c\nd\te\rf\u{08}g\u{0C}h\u{01}i", &mut out);
        assert_eq!(out, "\"a\\\"b\\\\c\\nd\\te\\rf\\bg\\fh\\u0001i\"");
    }

    /// Non-ASCII must pass through as UTF-8, not be mangled into escapes or replaced.
    #[test]
    fn non_ascii_passes_through_as_utf8() {
        let mut out = String::new();
        escape_json_into("café — 日本語 🎉", &mut out);
        assert_eq!(out, "\"café — 日本語 🎉\"");
    }

    /// **The values JSON has no numbers for.** Emitting them bare would produce a document that a
    /// strict parser rejects entirely.
    #[test]
    fn non_finite_floats_become_strings_rather_than_invalid_json() {
        for (v, expected) in [
            (f64::NAN, "\"NaN\""),
            (f64::INFINITY, "\"Infinity\""),
            (f64::NEG_INFINITY, "\"-Infinity\""),
        ] {
            let mut out = String::new();
            value_into(&Value::Float(v), &mut out);
            assert_eq!(out, expected, "a non-finite float was emitted as {out}");
        }
    }

    /// A whole-numbered float must stay a float on the wire. `1.0` printed as `1` would be read
    /// back as an integer and silently change the column's type downstream.
    #[test]
    fn a_whole_numbered_float_keeps_its_decimal_point() {
        let mut out = String::new();
        value_into(&Value::Float(1.0), &mut out);
        assert_eq!(out, "1.0");
    }

    /// A column name is itself a string from the catalog and gets the same escaping as a value.
    /// A table created with a quote in a column name would otherwise break the feed.
    #[test]
    fn column_names_are_escaped_too() {
        let e = ChangeEvent {
            txn_id: 1,
            lsn: 1,
            commit_lsn: 2,
            commit_end_lsn: 3,
            table: "odd\"table".into(),
            columns: Arc::new(vec!["we\"ird".into()]),
            op: ChangeOp::Insert { new: vec![Value::Integer(1)] },
        };
        let line = to_json_line(&e);
        assert!(line.contains("\"we\\\"ird\":1"), "column name was not escaped: {line}");
        assert!(line.contains("\"odd\\\"table\""), "table name was not escaped: {line}");
    }

    /// More values than column names must not silently drop the surplus.
    #[test]
    fn surplus_values_get_visible_synthetic_names_rather_than_vanishing() {
        let e = ChangeEvent {
            txn_id: 1,
            lsn: 1,
            commit_lsn: 2,
            commit_end_lsn: 3,
            table: "t".into(),
            columns: Arc::new(vec!["a".into()]),
            op: ChangeOp::Insert { new: vec![Value::Integer(1), Value::Integer(2)] },
        };
        let line = to_json_line(&e);
        assert!(line.contains("\"a\":1"), "{line}");
        assert!(
            line.contains("__unnamed_column_1\":2"),
            "the surplus value vanished from the feed: {line}"
        );
    }

    /// **Integer fidelity.** JSON numbers are commonly parsed into f64, which loses integers past
    /// 2^53 — the classic silent-corruption bug in CDC pipelines carrying BIGINT.
    ///
    /// `INTEGER` is `i32`, whose extremes are ±2.1e9, three orders of magnitude inside f64's
    /// exactly-representable range, so it stays a **bare JSON number**: there is nothing to lose,
    /// and stringifying it would break every consumer reading that column today.
    ///
    /// This test used to carry a note saying that if a wider integer type were ever added it would
    /// start failing. That was wrong — it only ever looked at `Value::Integer`, so it would have
    /// stayed green while `BIGINT` shipped broken next to it. `BIGINT` was added; the note is
    /// corrected here, and the tests that actually cover the wide types are below.
    #[test]
    fn integer_extremes_survive_exactly() {
        for v in [i32::MIN, i32::MAX, 0, -1] {
            let mut out = String::new();
            value_into(&Value::Integer(v), &mut out);
            assert_eq!(out, v.to_string(), "integer {v} did not round-trip");
            // Exactly representable as f64, so a consumer parsing into a double is still safe.
            assert_eq!(out.parse::<f64>().unwrap() as i64, v as i64, "{v} lost precision as f64");
        }
    }

    /// **The three wide types are JSON strings, quotes included.**
    ///
    /// Asserting on the exact bytes rather than on "contains the digits" is deliberate: a bare
    /// number also contains the digits, so a `contains` check would pass on the encoding this test
    /// exists to forbid.
    #[test]
    fn the_wide_types_are_emitted_as_quoted_strings() {
        let cases: Vec<(Value, &str)> = vec![
            (Value::BigInt(i64::MAX), "\"9223372036854775807\""),
            (Value::BigInt(i64::MIN), "\"-9223372036854775808\""),
            (Value::BigInt(9007199254740993), "\"9007199254740993\""),
            (Value::BigInt(0), "\"0\""),
            (Value::Decimal("123456789012345678901234567890.123456789".into()),
             "\"123456789012345678901234567890.123456789\""),
            (Value::Decimal("-0.00000000000000000001".into()), "\"-0.00000000000000000001\""),
            (Value::Decimal("1.50".into()), "\"1.50\""),
            (Value::Timestamp(1_700_000_000_123), "\"1700000000123\""),
            (Value::Timestamp(i64::MIN), "\"-9223372036854775808\""),
        ];
        for (v, expected) in cases {
            let mut out = String::new();
            value_into(&v, &mut out);
            assert_eq!(out, expected, "{v:?} was not emitted as the exact JSON string {expected}");
            assert!(out.starts_with('"') && out.ends_with('"'), "{v:?} left the quotes off: {out}");
        }
    }

    /// **The negative control.** This is the test that would go red if the three types were ever
    /// switched back to bare JSON numbers, and it says *why* in the assertion rather than just
    /// failing: it strips the quotes the encoder added — which is exactly what a bare-number
    /// encoder would have produced — and shows the value not surviving a double.
    ///
    /// Without this, "we emit strings" is a stylistic preference. With it, the preference has a
    /// number attached.
    #[test]
    fn a_bare_json_number_would_lose_these_values_which_is_why_they_are_strings() {
        // BIGINT past 2^53.
        for v in [i64::MAX, i64::MIN + 1, 9007199254740993, -9007199254740993] {
            let mut out = String::new();
            value_into(&Value::BigInt(v), &mut out);
            let unquoted = out.trim_matches('"');
            assert_eq!(unquoted, v.to_string(), "the digits themselves must be intact");
            // The loss is measured on the DIGITS, not through `as_double as i64`.
            //
            // A float-to-int `as` cast in Rust saturates. `i64::MAX` parses to the double 2^63
            // (9223372036854775808), and casting that back to `i64` clamps it to `i64::MAX` —
            // landing on the original value and making the round trip look lossless when it was
            // not. The saturating cast reverses exactly the error being measured, so it reported
            // "survived" for `i64::MAX` while the four other values here reported "lost". A
            // consumer does not have that clamp: it holds the double and prints 2^63.
            //
            // Comparing the rendered integral digits has no such blind spot. Every double at this
            // magnitude is an exact integer, so `{:.0}` prints its true value.
            let as_double: f64 = unquoted.parse().unwrap();
            assert_ne!(
                format!("{as_double:.0}"),
                v.to_string(),
                "premise broken: {v} survived an f64 round trip, so this test proves nothing. \
                 Pick a value that does not."
            );
            // And the string form does survive, which is the whole point.
            assert_eq!(unquoted.parse::<i64>().unwrap(), v);
        }

        // DECIMAL with more significant digits than a double carries.
        let d = "123456789012345678901234567890.123456789";
        let mut out = String::new();
        value_into(&Value::Decimal(d.into()), &mut out);
        let unquoted = out.trim_matches('"');
        assert_eq!(unquoted, d);
        let as_double: f64 = unquoted.parse().unwrap();
        assert_ne!(
            format!("{as_double}"),
            d,
            "premise broken: this decimal survived an f64 round trip"
        );

        // TIMESTAMP: epoch millis fits a double, but the type is i64 and its extremes do not.
        let ts = i64::MAX - 7;
        let mut out = String::new();
        value_into(&Value::Timestamp(ts), &mut out);
        let unquoted = out.trim_matches('"');
        assert_eq!(unquoted, ts.to_string());
        // Same digit comparison as above, and for the same reason: the saturating cast would
        // measure this one through the very clamp that hides the error.
        let as_double: f64 = unquoted.parse().unwrap();
        assert_ne!(format!("{as_double:.0}"), ts.to_string());
    }

    /// A row mixing all of them: the narrow types stay numbers, the wide ones become strings.
    /// A consumer reading this line can tell which is which by JSON type alone.
    #[test]
    fn a_mixed_row_keeps_narrow_types_as_numbers_and_wide_types_as_strings() {
        let e = ChangeEvent {
            txn_id: 1,
            lsn: 1,
            commit_lsn: 2,
            commit_end_lsn: 3,
            table: "t".into(),
            columns: Arc::new(vec![
                "i".into(), "f".into(), "b".into(), "big".into(), "dec".into(), "ts".into(),
            ]),
            op: ChangeOp::Insert {
                new: vec![
                    Value::Integer(42),
                    Value::Float(1.5),
                    Value::Boolean(true),
                    Value::BigInt(i64::MAX),
                    Value::Decimal("0.10".into()),
                    Value::Timestamp(1_700_000_000_123),
                ],
            },
        };
        let line = to_json_line(&e);
        assert!(line.contains("\"i\":42"), "INTEGER must stay a bare number: {line}");
        assert!(line.contains("\"f\":1.5"), "FLOAT must stay a bare number: {line}");
        assert!(line.contains("\"b\":true"), "{line}");
        assert!(line.contains("\"big\":\"9223372036854775807\""), "{line}");
        assert!(line.contains("\"dec\":\"0.10\""), "{line}");
        assert!(line.contains("\"ts\":\"1700000000123\""), "{line}");
    }

    /// A NULL in a wide column is JSON `null`, not the string `"null"`. A consumer that cannot tell
    /// a missing amount from the four characters n-u-l-l has a worse problem than precision.
    #[test]
    fn a_null_wide_column_is_json_null_not_a_quoted_null() {
        for v in [Value::Null] {
            let mut out = String::new();
            value_into(&v, &mut out);
            assert_eq!(out, "null");
        }
    }

    /// Float fidelity: the printed form must parse back to the identical bit pattern. `{}` would
    /// print 0.30000000000000004 as "0.3", which is a different number.
    #[test]
    fn floats_round_trip_bit_for_bit() {
        for v in [0.1 + 0.2, 1e300, -1e-300, f64::MIN_POSITIVE, -0.0, 12345.6789] {
            let mut out = String::new();
            value_into(&Value::Float(v), &mut out);
            let back: f64 = out.parse().unwrap_or_else(|e| panic!("{out} did not parse: {e}"));
            assert_eq!(
                back.to_bits(),
                v.to_bits(),
                "float {v} printed as {out} and came back as {back}"
            );
        }
    }

    /// NULL and a missing key are different things and must stay different. A consumer that cannot
    /// tell "this column is null" from "this column was not sent" cannot apply an update correctly.
    #[test]
    fn a_null_column_is_present_and_null_not_absent() {
        let e = ChangeEvent {
            txn_id: 1,
            lsn: 1,
            commit_lsn: 2,
            commit_end_lsn: 3,
            table: "t".into(),
            columns: Arc::new(vec!["a".into(), "b".into()]),
            op: ChangeOp::Insert { new: vec![Value::Integer(1), Value::Null] },
        };
        let line = to_json_line(&e);
        assert!(line.contains("\"b\":null"), "the null column was omitted entirely: {line}");
    }

    #[test]
    fn write_feed_reports_how_many_lines_it_wrote() {
        let events = vec![
            event(ChangeOp::Insert { new: vec![Value::Integer(1), Value::Null] }),
            event(ChangeOp::Delete { old: vec![Value::Integer(1), Value::Null] }),
        ];
        let mut buf: Vec<u8> = Vec::new();
        let n = write_feed(&events, &mut buf).unwrap();
        assert_eq!(n, 2);
        let text = String::from_utf8(buf).unwrap();
        assert_eq!(text.lines().count(), 2, "one object per line: {text}");
        assert!(text.ends_with('\n'), "the last line must be terminated too");
        assert!(text.contains("\"qty\":null"), "a NULL did not survive: {text}");
    }
}
