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

use std::io::Write;

use crate::catalog::column::Value;
use crate::error::FerroError;

use super::logical::{ChangeEvent, ChangeOp};

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
/// See the module docs for why non-finite floats become strings rather than bare tokens or nulls.
pub fn value_into(v: &Value, out: &mut String) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Boolean(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Integer(i) => out.push_str(&i.to_string()),
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
        ChangeOp::Insert { .. } => out.push_str("null"),
        ChangeOp::Update { old, .. } | ChangeOp::Delete { old } => {
            row_into(&e.columns, old, &mut out)
        }
    }

    out.push_str(",\"after\":");
    match &e.op {
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
