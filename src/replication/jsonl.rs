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
//!  "writer":{"prov_id":1,"agent":"restock-agent","run":"run-42","model":"claude-opus",
//!            "model_version":"2026-05","prompt_sha256":"9f86d0...","started_at":"1700000000000",
//!            "branch":"b4@g0"},
//!  "before":null,"after":{"id":1,"qty":10}}
//! ```
//!
//! # `writer`, and why it is always present
//!
//! Attribution used to stop at this boundary. The database could say which agent run wrote a
//! version and the feed could not, so a consumer holding a million rows from a model that has since
//! been found unsound had no way to ask which of them came from it.
//!
//! `writer` is `null` for a change no agent run produced, and for the snapshot `READ` rows and
//! schema declarations that have no writer to name. It is **always present**, null included, for
//! the same reason `before`/`after` are: a consumer that branches on which keys exist breaks the
//! first time a key is added, and one that branches on the value does not.
//!
//! The prompt travels as `prompt_sha256`, a hex digest, and never as text. That is the field's
//! whole purpose — a prompt containing customer data must not become a durable copy of it in every
//! consumer's destination table.
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

//! # What may leave at all — B7
//!
//! Every rule above is about representing a value faithfully. A [`Publication`] is about whether the
//! value may be represented here in the first place, and it is enforced in this module because this
//! is where a row becomes bytes: [`row_into`] holds the allowlist, so the WAL stream, the initial
//! snapshot and the table dump are all covered by one decision rather than by three copies of it.
//!
//! What that looks like on the wire:
//!
//! * A withheld column's key is **absent** from `before`/`after`, and nothing anywhere names it.
//!   Not a redaction marker, not a `withheld` list: a guard whose job is that a column cannot leave
//!   must not ship the name of the column it is protecting, and `salary_band` is information even
//!   with the number removed.
//! * The **declared shape is projected by the same mask**, which is what makes the absence honest
//!   rather than silent. This module's standing rule is that a consumer unable to tell "null" from
//!   "not sent" cannot apply an update — that rule assumes the consumer knows the column exists, and
//!   under a publication it does not. The `CREATE_TABLE` it is given and the rows it receives agree
//!   exactly, so there is no absence for it to misread. Skip the projection of the shape and the rule
//!   bites immediately: the consumer creates a column, never receives a value for it, and reports it
//!   as permanently null.
//! * A feed with no publication — or one whose every column is published — is byte-for-byte what it
//!   was before publications existed.
//!
//! A refusal (an undecided table, a row with nothing publishable in it) is not a line at all. It
//! comes back as a [`Refusal`], and what the streaming caller must then do with its cursor is the
//! hard part — see [`super::stream::FeedStreamer::pump`].

use std::io::Write;

use crate::catalog::column::Value;
use crate::error::FerroError;
use crate::provenance::sha256::to_hex;
use crate::provenance::RunEntity;

use super::logical::{ChangeEvent, ChangeOp, SchemaChange};
use super::publication::{Mask, Publication, Refusal};

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

/// Render one row as a JSON object, **through the publication's mask**.
///
/// The one function that turns a **row** into bytes, which is why the allowlist is enforced here
/// rather than at any of the callers. A column the mask does not publish is not written: not nulled,
/// not renamed, not emptied — the key is absent, and **nothing anywhere names it**. An earlier version
/// of this comment said the name was declared in a `withheld` list; that list was removed for the
/// reason given where it used to be emitted (a column name is information in its own right), and the
/// declared shape being projected by the same mask is what keeps the absence honest.
///
/// A row is not the only thing this module renders. The `CREATE_TABLE` shape is a list of column
/// *names* rather than values and has its own masked loop in [`line_with_mask`] — so the column rule
/// has two enforcement sites, one per kind of payload, and each is fire-checked separately (M2 and M5
/// in `tests/integration_cdc_publication.rs`). Anything that adds a third payload carrying column
/// names must mask it too; nothing forces that but a reader of this paragraph.
///
/// A row whose value count disagrees with its column count cannot be keyed honestly. Rather than
/// emit a half-labelled object, the extra or missing positions are made visible: with no policy in
/// force, surplus values get explicit synthetic keys, so nothing is dropped without a reader
/// noticing. Under an allowlist a nameless value is instead **refused** — a policy cannot decide
/// about a column it cannot name, and a guard that falls through to allow when it cannot read its
/// own input is not a guard.
fn row_into(
    mask: &Mask<'_>,
    columns: &[String],
    values: &[Value],
    out: &mut String,
) -> Result<(), Refusal> {
    out.push('{');
    // Counted rather than using the loop index: a withheld FIRST column with `if i > 0` would leave
    // a leading comma and emit `{,"qty":10}`, which is not JSON at all. The separator has to follow
    // what was written, not what was considered.
    let mut written = 0usize;
    for (i, v) in values.iter().enumerate() {
        match columns.get(i) {
            Some(name) => {
                if !mask.publishes(name) {
                    continue;
                }
                if written > 0 {
                    out.push(',');
                }
                escape_json_into(name, out);
            }
            None if mask.may_name_a_surplus_value() => {
                if written > 0 {
                    out.push(',');
                }
                escape_json_into(&format!("__unnamed_column_{i}"), out);
            }
            None => return Err(mask.refuse_unnamed(i)),
        }
        out.push(':');
        value_into(v, out);
        written += 1;
    }
    // **`{}` says the row has no columns.** The schema arm below refuses exactly this claim for a
    // declared shape, and a row can reach it too: an image shorter than the column list whose present
    // values are all withheld. Neither upstream check sees that shape - `mask_for` refuses only when
    // EVERY column of the table is withheld, and `check` inspects only images that are too long - so
    // it is refused here, where the bytes are. Found by an adversarial review; reachable from any
    // caller that supplies its own rows, which is the dump and the snapshot's read closure, not the
    // WAL path (a decoded tuple always matches its schema's width).
    //
    // An image with no values at all is a different thing and stays `{}`: nothing was withheld, there
    // was nothing to withhold.
    if written == 0 && !values.is_empty() {
        return Err(mask.refuse_nothing_publishable());
    }
    out.push('}');
    Ok(())
}

/// Append a run's identity as a JSON object.
///
/// `started_at` ships as a **string**, following the same rule this module applies to `TIMESTAMP`
/// columns: epoch milliseconds are an integer whose consumer decides the width, and a JSON number
/// is a double in most of them. It is well inside 2^53 today, which is exactly the reasoning that
/// made every other epoch field wrong eventually.
///
/// The prompt is a hex `prompt_sha256` and never text; see the module header.
pub fn writer_into(w: &RunEntity, out: &mut String) {
    out.push_str("{\"prov_id\":");
    out.push_str(&w.prov_id.0.to_string());
    out.push_str(",\"agent\":");
    escape_json_into(&w.agent_id, out);
    out.push_str(",\"run\":");
    escape_json_into(&w.run_id, out);
    out.push_str(",\"model\":");
    escape_json_into(&w.model, out);
    out.push_str(",\"model_version\":");
    escape_json_into(&w.model_version, out);
    out.push_str(",\"prompt_sha256\":");
    escape_json_into(&to_hex(&w.prompt_hash), out);
    out.push_str(",\"started_at\":");
    escape_json_into(&w.started_at.to_string(), out);
    out.push_str(",\"branch\":");
    escape_json_into(&w.parent_branch.to_string(), out);
    out.push('}');
}

/// One change event as a single line of JSON, **without** the trailing newline.
///
/// Refuses rather than returns bytes when the publication has not decided about the event's table,
/// or when nothing in the row is publishable. A caller advancing a cursor must read
/// [`super::stream::FeedStreamer::pump`] before deciding what to do with the refusal: the event is
/// not lost, but only if the cursor stays behind the commit that produced it.
pub fn to_json_line(e: &ChangeEvent, publication: &Publication) -> Result<String, Refusal> {
    let mask = publication.check(e)?;
    line_with_mask(e, &mask)
}

/// The renderer both entry points share, so [`to_json_line`] and [`write_feed`] cannot disagree
/// about what a masked event looks like on the wire.
fn line_with_mask(e: &ChangeEvent, mask: &Mask<'_>) -> Result<String, Refusal> {
    let mut out = String::with_capacity(128);
    out.push_str("{\"table\":");
    escape_json_into(&e.table, &mut out);

    out.push_str(",\"op\":");
    escape_json_into(e.op.name(), &mut out);

    out.push_str(&format!(
        ",\"txn\":{},\"lsn\":{},\"commit_lsn\":{},\"commit_end_lsn\":{}",
        e.txn_id, e.lsn, e.commit_lsn, e.commit_end_lsn
    ));

    out.push_str(",\"writer\":");
    match &e.writer {
        Some(w) => writer_into(w, &mut out),
        None => out.push_str("null"),
    }

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
            row_into(mask, &e.columns, old, &mut out)?
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
            // **The declared shape goes through the same mask as a row.** A withheld column's name
            // and type are themselves information that must not leave — and worse, a consumer told
            // about a column it will never receive a value for creates it and then reports the
            // column as permanently null, which reads as data loss rather than as policy.
            out.push_str("{\"columns\":[");
            let mut written = 0usize;
            for c in columns.iter() {
                if !mask.publishes(&c.name) {
                    continue;
                }
                if written > 0 {
                    out.push(',');
                }
                out.push_str("{\"name\":");
                escape_json_into(&c.name, &mut out);
                out.push_str(",\"type\":");
                escape_json_into(&c.sql_type, &mut out);
                out.push_str(",\"nullable\":");
                out.push_str(if c.nullable { "true" } else { "false" });
                out.push('}');
                written += 1;
            }
            out.push_str("]}");
            // Unreachable while a `CREATE_TABLE`'s spec list and the event's column list come from
            // the same DDL record, because the publication refuses an event whose every column is
            // withheld before this runs. Kept because the byte it would otherwise emit is
            // `{"columns":[]}`, which claims the table has no columns at all — a different and
            // false statement, and one the Go consumer refuses by name.
            if written == 0 && !columns.is_empty() {
                return Err(mask.refuse_nothing_publishable());
            }
        }
        ChangeOp::Read { row } => row_into(mask, &e.columns, row, &mut out)?,
        ChangeOp::Insert { new } | ChangeOp::Update { new, .. } => {
            row_into(mask, &e.columns, new, &mut out)?
        }
        ChangeOp::Delete { .. } => out.push_str("null"),
    }

    // **Nothing names the withheld columns on the wire, and that is a decision rather than an
    // omission.** An earlier version of this emitted `"withheld":["ssn"]`, on the reasoning that a
    // consumer which cannot tell "null" from "not sent" cannot apply an update — the rule the rest of
    // this module is built on. It is the wrong rule here, because it assumes the consumer knows the
    // column exists. Under a publication it does not: the `CREATE_TABLE` shape is projected by the
    // same mask, so the shape the consumer is told about and the rows it receives agree exactly, and
    // there is no absence for it to misread.
    //
    // What the list did cost was real: a column NAME is itself information — `hiv_status`,
    // `salary_band` — and a guard whose job is that a column cannot leave the database must not ship
    // the name of the column it is protecting. So a denied column appears in the feed neither as a
    // key, nor as a value, nor as a name in a metadata field.
    out.push('}');
    Ok(out)
}

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
///
/// The publication applies here too, and for a reason worth stating: a dump is egress. The rows go
/// to a file that a consumer reads, so a column withheld from the feed and printed by the dump has
/// left the database just as thoroughly — and the diff would then compare a projected feed against
/// an unprojected source and report the withheld column as a data mismatch, which is the wrong
/// answer to the wrong question.
pub fn write_table_json<W: Write>(
    table: &str,
    columns: &[String],
    rows: &[Vec<Value>],
    publication: &Publication,
    w: &mut W,
) -> Result<usize, FerroError> {
    let mask = publication.mask_for(table, columns)?;
    let mut out = String::from("[");
    for (i, row) in rows.iter().enumerate() {
        if i > 0 {
            out.push(',');
        }
        row_into(&mask, columns, row, &mut out)?;
    }
    out.push(']');
    writeln!(w, "{out}").map_err(|e| FerroError::Io(format!("write table json: {e}")))?;
    Ok(rows.len())
}

/// Write a feed of events as JSON Lines, **or write none of them**.
///
/// Returns how many lines were written, so a caller can tell "wrote nothing" from "wrote something"
/// without re-reading its own output.
///
/// # Every event is decided before any byte is written
///
/// The publication is asked about the whole batch first, and a single refusal means nothing is
/// written at all. A half-written batch is the worst of both outcomes: the caller gets an error, so
/// it must assume the write failed, while the consumer already holds the prefix — and for the
/// snapshot path, that prefix is a partial table that looks like a complete one.
///
/// It is not a check-then-act shape, and the difference is the whole of it: every line is **rendered**
/// before any line is written, so a refusal from anywhere in the batch — including one the renderer
/// mints that the up-front decision cannot see — returns `Err` with nothing on the wire. The earlier
/// form checked up front and wrote as it rendered, which was atomic only while one function's
/// refusals were a superset of the other's. They were not.
///
/// The cost is the batch held as strings for the length of the call, which `max_bytes` already bounds
/// for the stream and which the snapshot path already pays for its rows.
///
/// **Atomicity stops at this call.** A caller writing two batches to one sink — the multi-table
/// snapshot recipe in [`super::snapshot::SnapshotBoundaryBuilder`] does exactly that — can still leave
/// the first batch on the wire when the second refuses. Check the publication covers every table
/// first: `publication.columns_of(t).is_some()` for each, before the first `deliver`.
pub fn write_feed<W: Write>(
    events: &[ChangeEvent],
    publication: &Publication,
    w: &mut W,
) -> Result<usize, FerroError> {
    // Rendered in full before the first byte is written, so the promise above is structural rather
    // than a property of the two passes agreeing. It used to check every event up front and then
    // render-and-write in a loop, which held only while `check` refused a superset of what
    // `line_with_mask` refuses — and it does not: the schema arm mints a refusal from the declared
    // spec list, which `check` never looks at. An adversarial review built that event and watched the
    // first line reach the writer under an `Err` return.
    let lines: Vec<String> = events
        .iter()
        .map(|e| {
            let mask = publication.check(e)?;
            line_with_mask(e, &mask)
        })
        .collect::<Result<_, Refusal>>()?;
    let n = lines.len();
    for line in lines {
        writeln!(w, "{line}").map_err(|err| FerroError::Io(format!("write feed: {err}")))?;
    }
    w.flush().map_err(|err| FerroError::Io(format!("flush feed: {err}")))?;
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// Render with no policy in force. Every test below that is not about publications uses this, so
    /// that what it asserts is the encoding rather than the encoding-plus-a-policy — and so that the
    /// bytes it pins are the ones a feed with no publication still emits today.
    fn line(e: &ChangeEvent) -> String {
        to_json_line(e, &Publication::unrestricted()).expect("an unrestricted feed refused an event")
    }

    fn event(op: ChangeOp) -> ChangeEvent {
        ChangeEvent {
            txn_id: 2,
            lsn: 216,
            commit_lsn: 422,
            commit_end_lsn: 455,
            table: "inventory".into(),
            columns: Arc::new(vec!["id".into(), "qty".into()]),
            op,
            writer: None,
        }
    }

    #[test]
    fn an_insert_line_carries_after_and_a_null_before() {
        let line = line(&event(ChangeOp::Insert {
            new: vec![Value::Integer(1), Value::Integer(10)],
        }));
        assert!(line.contains("\"op\":\"INSERT\""), "{line}");
        assert!(line.contains("\"before\":null"), "{line}");
        assert!(line.contains("\"after\":{\"id\":1,\"qty\":10}"), "{line}");
        assert!(!line.contains('\n'), "a JSON *line* must not contain a newline: {line}");
    }

    #[test]
    fn a_delete_line_carries_before_and_a_null_after() {
        let line = line(&event(ChangeOp::Delete {
            old: vec![Value::Integer(2), Value::Integer(20)],
        }));
        assert!(line.contains("\"op\":\"DELETE\""), "{line}");
        assert!(line.contains("\"before\":{\"id\":2,\"qty\":20}"), "{line}");
        assert!(line.contains("\"after\":null"), "{line}");
    }

    #[test]
    fn an_update_line_carries_both_images() {
        let line = line(&event(ChangeOp::Update {
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
            writer: None,
        };
        let line = line(&e);
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
            writer: None,
        };
        let line = line(&e);
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
            writer: None,
        };
        let line = line(&e);
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
            writer: None,
        };
        let line = line(&e);
        assert!(line.contains("\"b\":null"), "the null column was omitted entirely: {line}");
    }

    // ---- B7: the publication, at the boundary where a row becomes bytes ------------------------

    /// A three-column table whose third column must never leave.
    fn customer(op: ChangeOp) -> ChangeEvent {
        ChangeEvent {
            txn_id: 2,
            lsn: 216,
            commit_lsn: 422,
            commit_end_lsn: 455,
            table: "customers".into(),
            columns: Arc::new(vec!["id".into(), "name".into(), "ssn".into()]),
            op,
            // B5 added attribution to the event; this fixture predates it and is about
            // egress policy, not who wrote the row.
            writer: None,
        }
    }

    fn analytics() -> Publication {
        Publication::parse("publication analytics\ncustomers: id, name\n").unwrap()
    }

    fn row3() -> Vec<Value> {
        vec![Value::Integer(1), Value::Varchar("ada".into()), Value::Varchar("000-11-2222".into())]
    }

    /// **The property this whole lane exists for, at the byte level.**
    ///
    /// Breaking shape: any row of a table with a denied column — here `customers(id, name, ssn)`
    /// under a publication naming `id, name`. The digits of the ssn must not appear in the bytes at
    /// all, and its siblings must still ship, which is what makes the test about the allowlist rather
    /// than about emitting nothing.
    #[test]
    fn a_denied_column_is_named_nowhere_in_the_line() {
        let line =
            to_json_line(&customer(ChangeOp::Insert { new: row3() }), &analytics()).expect("refused");
        assert!(!line.contains("000-11-2222"), "the denied value left the database: {line}");
        // The NAME, anywhere in the line, not just as a key. A redaction marker or a `withheld`
        // list would satisfy "no key" and still ship the name of the column being protected.
        assert!(!line.contains("ssn"), "the denied column is named in the line: {line}");
        assert!(line.contains("\"after\":{\"id\":1,\"name\":\"ada\"}"), "{line}");
    }

    /// **A withheld FIRST column must not leave a leading comma**, which would make the line
    /// unparseable — and it would break at the consumer's parser rather than here.
    ///
    /// Breaking shape: the separator written from the loop index rather than from what has actually
    /// been written. `{,"name":"ada"}` is not JSON, and the first version of this loop did exactly
    /// that.
    #[test]
    fn a_withheld_first_column_leaves_valid_json() {
        let p = Publication::parse("publication p\ncustomers: name, ssn\n").unwrap();
        let line = to_json_line(&customer(ChangeOp::Insert { new: row3() }), &p).expect("refused");
        assert!(!line.contains("{,"), "a leading comma made the row unparseable: {line}");
        assert!(line.contains("\"after\":{\"name\":\"ada\",\"ssn\":\"000-11-2222\"}"), "{line}");
        assert!(!line.contains("\"id\""), "the withheld column is named in the line: {line}");
    }

    /// **Both images of an UPDATE go through the mask.**
    ///
    /// Breaking shape: an update that changes the ssn. A projection applied only to `after` — the
    /// obvious half, and the one a reader of `to_json_line` sees first — ships the OLD ssn in
    /// `before` and passes any test that only looks at `after`.
    #[test]
    fn both_images_of_an_update_are_projected() {
        let e = customer(ChangeOp::Update {
            old: vec![
                Value::Integer(1),
                Value::Varchar("ada".into()),
                Value::Varchar("000-11-2222".into()),
            ],
            new: vec![
                Value::Integer(1),
                Value::Varchar("ada".into()),
                Value::Varchar("999-88-7777".into()),
            ],
        });
        let line = to_json_line(&e, &analytics()).expect("refused");
        assert!(!line.contains("000-11-2222"), "the OLD denied value left in `before`: {line}");
        assert!(!line.contains("999-88-7777"), "the new denied value left in `after`: {line}");
        assert!(line.contains("\"before\":{\"id\":1,\"name\":\"ada\"}"), "{line}");
        assert!(line.contains("\"after\":{\"id\":1,\"name\":\"ada\"}"), "{line}");
    }

    /// A DELETE carries **only** a before image, so a fix applied to the after path alone misses it
    /// entirely — and a delete's before image is a whole row of denied values.
    #[test]
    fn a_delete_projects_its_only_image() {
        let line = to_json_line(&customer(ChangeOp::Delete { old: row3() }), &analytics())
            .expect("refused");
        assert!(!line.contains("000-11-2222"), "a DELETE shipped the denied column: {line}");
        assert!(line.contains("\"before\":{\"id\":1,\"name\":\"ada\"}"), "{line}");
        assert!(line.contains("\"after\":null"), "{line}");
    }

    /// A snapshot `READ` is the third image-carrying op and the one E75 proves is a separate path.
    #[test]
    fn a_snapshot_read_projects_its_row() {
        let line =
            to_json_line(&customer(ChangeOp::Read { row: row3() }), &analytics()).expect("refused");
        assert!(!line.contains("000-11-2222"), "a snapshot row shipped the denied column: {line}");
        assert!(line.contains("\"op\":\"READ\""), "{line}");
        assert!(line.contains("\"after\":{\"id\":1,\"name\":\"ada\"}"), "{line}");
    }

    /// **The declared shape is projected too.** A consumer told about a column it will never receive
    /// creates it and then reports it as permanently null, which reads as data loss; and the name and
    /// type of a denied column are themselves information that must not leave.
    #[test]
    fn a_create_table_shape_omits_the_denied_column() {
        use crate::replication::logical::ColumnSpec;
        let e = customer(ChangeOp::Schema {
            change: SchemaChange::CreateTable,
            columns: vec![
                ColumnSpec { name: "id".into(), sql_type: "INTEGER".into(), nullable: false },
                ColumnSpec { name: "name".into(), sql_type: "VARCHAR(32)".into(), nullable: true },
                ColumnSpec { name: "ssn".into(), sql_type: "VARCHAR(16)".into(), nullable: true },
            ],
        });
        let line = to_json_line(&e, &analytics()).expect("refused");
        assert!(!line.contains("ssn"), "the denied column was declared to the consumer: {line}");
        assert!(line.contains("\"name\":\"id\""), "{line}");
        assert!(line.contains("\"name\":\"name\""), "{line}");
    }

    /// A table the publication has never heard of is refused rather than emptied, and the anti-vacuity
    /// half shows the same renderer emitting the table it does name.
    #[test]
    fn an_undecided_table_is_refused_by_the_renderer() {
        let e = ChangeEvent {
            txn_id: 1,
            lsn: 1,
            commit_lsn: 2,
            commit_end_lsn: 3,
            table: "audit_log".into(),
            columns: Arc::new(vec!["id".into()]),
            op: ChangeOp::Insert { new: vec![Value::Integer(1)] },
            // B5 added attribution to the event; this fixture predates it and is about
            // egress policy, not who wrote the row.
            writer: None,
        };
        let r = to_json_line(&e, &analytics()).expect_err("an undecided table was rendered");
        assert_eq!(r.table, "audit_log");
        to_json_line(&customer(ChangeOp::Insert { new: row3() }), &analytics())
            .expect("the published table was refused too, so the refusal proves nothing");
    }

    /// **A refused event must not leave a prefix on the wire.**
    ///
    /// Breaking shape: a batch whose *second* event is refused. Written line by line, the consumer
    /// holds event one while the caller sees an error and must assume the write failed — and for the
    /// snapshot path that prefix is a partial table that looks exactly like a complete one.
    #[test]
    fn write_feed_writes_nothing_at_all_when_any_event_is_refused() {
        let good = customer(ChangeOp::Insert { new: row3() });
        let bad = ChangeEvent {
            table: "audit_log".into(),
            columns: Arc::new(vec!["id".into()]),
            op: ChangeOp::Insert { new: vec![Value::Integer(7)] },
            ..customer(ChangeOp::Insert { new: row3() })
        };
        let events = vec![good.clone(), bad, good.clone()];
        let mut buf: Vec<u8> = Vec::new();
        let err = write_feed(&events, &analytics(), &mut buf)
            .expect_err("a batch containing a refused event was written");
        assert!(format!("{err}").contains("audit_log"), "{err}");
        assert!(
            buf.is_empty(),
            "the events before the refusal were already on the wire: {}",
            String::from_utf8_lossy(&buf)
        );

        // Anti-vacuity: the same batch without the refused event writes every line.
        let mut buf2: Vec<u8> = Vec::new();
        assert_eq!(write_feed(&[good.clone(), good], &analytics(), &mut buf2).unwrap(), 2);
        assert_eq!(String::from_utf8(buf2).unwrap().lines().count(), 2);
    }

    /// **A row whose present values are all withheld is refused, not emitted as `{}`.**
    ///
    /// Breaking shape: a row image SHORTER than the table's column list whose present values are all
    /// withheld — `write_table_json` with rows a caller supplied, or a snapshot's read closure.
    /// Neither upstream check sees it: `mask_for` refuses only when every column of the table is
    /// withheld (here `ssn` is published, so it does not), and `check` looks only at images that are
    /// too long. `{}` is not a redacted row, it is a claim that the row has no columns — the same
    /// claim the schema arm refuses for a declared shape.
    #[test]
    fn a_row_whose_present_values_are_all_withheld_is_refused() {
        let p = Publication::parse("publication p\ncustomers: ssn\n").unwrap();
        let columns = vec!["id".to_string(), "name".to_string(), "ssn".to_string()];
        let short = vec![vec![Value::Integer(1), Value::Varchar("ada".into())]];
        let mut buf: Vec<u8> = Vec::new();
        let err = write_table_json("customers", &columns, &short, &p, &mut buf)
            .expect_err("a row rendered as {} instead of being refused");
        assert!(format!("{err}").contains("empty object"), "wrong reason: {err}");
        assert!(!String::from_utf8_lossy(&buf).contains("{}"), "an empty row reached the wire");

        // Anti-vacuity: a full row under the same policy ships its published column, and a row with
        // no values at all is still `{}` — nothing was withheld, there was nothing to withhold.
        let full = vec![vec![
            Value::Integer(1),
            Value::Varchar("ada".into()),
            Value::Varchar("000-11-2222".into()),
        ]];
        let mut ok: Vec<u8> = Vec::new();
        write_table_json("customers", &columns, &full, &p, &mut ok).expect("a full row was refused");
        assert!(String::from_utf8_lossy(&ok).contains("000-11-2222"));
        let mut empty: Vec<u8> = Vec::new();
        write_table_json("customers", &columns, &[vec![]], &p, &mut empty)
            .expect("a row with no values was refused");
        assert_eq!(String::from_utf8_lossy(&empty).trim(), "[{}]");
    }

    /// **The refusal the up-front decision cannot see must still leave nothing on the wire.**
    ///
    /// Breaking shape: a batch whose second event is a `CREATE_TABLE` where the event's column list is
    /// published but the declared spec list is not — `check` decides from the column list and passes,
    /// and the refusal is minted from the spec list inside the renderer. An adversarial review built
    /// exactly this and watched the first line reach the writer under an `Err` return, which is the
    /// harm `write_feed`'s doc promises not to do.
    #[test]
    fn a_refusal_minted_inside_the_renderer_still_writes_nothing() {
        use crate::replication::logical::ColumnSpec;
        let p = Publication::parse("publication analytics\ncustomers: id\n").unwrap();
        let good = ChangeEvent {
            txn_id: 1,
            lsn: 10,
            commit_lsn: 10,
            commit_end_lsn: 20,
            table: "customers".into(),
            columns: Arc::new(vec!["id".into()]),
            op: ChangeOp::Insert { new: vec![Value::Integer(1)] },
            // B5 added attribution to the event; this fixture predates it and is about
            // egress policy, not who wrote the row.
            writer: None,
        };
        // Column list published; declared shape entirely denied.
        let mismatched = ChangeEvent {
            op: ChangeOp::Schema {
                change: SchemaChange::CreateTable,
                columns: vec![ColumnSpec {
                    name: "ssn".into(),
                    sql_type: "VARCHAR(16)".into(),
                    nullable: true,
                }],
            },
            ..good.clone()
        };
        p.check(&mismatched).expect("the premise is gone: check now refuses this by itself");

        let mut buf: Vec<u8> = Vec::new();
        let err = write_feed(&[good, mismatched], &p, &mut buf)
            .expect_err("a batch with an unrenderable event was written");
        assert!(format!("{err}").contains("empty object"), "wrong reason: {err}");
        assert!(
            buf.is_empty(),
            "the first line was written before the second refused: {}",
            String::from_utf8_lossy(&buf)
        );
    }

    /// **A feed with nothing withheld is byte-for-byte what it was before publications existed.**
    ///
    /// Two ways to be in that state and both are asserted: no policy at all, and a policy that
    /// publishes every column the event carries. Neither may grow a `withheld` key, or every existing
    /// consumer sees a field it was not built for.
    #[test]
    fn a_feed_with_nothing_withheld_is_unchanged() {
        let e = customer(ChangeOp::Insert { new: row3() });
        let none = to_json_line(&e, &Publication::unrestricted()).unwrap();
        let all = to_json_line(
            &e,
            &Publication::parse("publication p\ncustomers: id, name, ssn\n").unwrap(),
        )
        .unwrap();
        assert_eq!(none, all, "a fully published table is not rendered like an unpublished feed");
        assert!(none.contains("000-11-2222"), "the unrestricted feed dropped a column: {none}");
        // No marker of any kind is added to a line that withheld nothing, so every consumer built
        // against the pre-B7 envelope keeps working unchanged.
        assert!(!none.contains("withheld"), "an unwithheld line grew a policy field: {none}");
    }

    /// The table dump goes through the same mask, because a dump is egress too — and because a
    /// projected feed diffed against an unprojected dump reports the withheld column as a data
    /// mismatch, which is a true statement about the wrong thing.
    #[test]
    fn a_table_dump_is_projected_and_refused_by_the_same_policy() {
        let columns = vec!["id".to_string(), "name".to_string(), "ssn".to_string()];
        let rows = vec![row3()];
        let mut buf: Vec<u8> = Vec::new();
        let n = write_table_json("customers", &columns, &rows, &analytics(), &mut buf).unwrap();
        assert_eq!(n, 1);
        let text = String::from_utf8(buf).unwrap();
        assert!(!text.contains("000-11-2222"), "the dump shipped the denied column: {text}");
        assert!(text.contains("\"name\":\"ada\""), "the dump withheld a published column: {text}");

        // And a table the publication does not name cannot be dumped at all.
        let mut buf2: Vec<u8> = Vec::new();
        write_table_json("audit_log", &columns, &rows, &analytics(), &mut buf2)
            .expect_err("an undecided table was dumped");
        assert!(buf2.is_empty(), "bytes were written before the refusal: {buf2:?}");
    }

    #[test]
    fn write_feed_reports_how_many_lines_it_wrote() {
        let events = vec![
            event(ChangeOp::Insert { new: vec![Value::Integer(1), Value::Null] }),
            event(ChangeOp::Delete { old: vec![Value::Integer(1), Value::Null] }),
        ];
        let mut buf: Vec<u8> = Vec::new();
        let n = write_feed(&events, &Publication::unrestricted(), &mut buf).unwrap();
        assert_eq!(n, 2);
        let text = String::from_utf8(buf).unwrap();
        assert_eq!(text.lines().count(), 2, "one object per line: {text}");
        assert!(text.ends_with('\n'), "the last line must be terminated too");
        assert!(text.contains("\"qty\":null"), "a NULL did not survive: {text}");
    }
}
