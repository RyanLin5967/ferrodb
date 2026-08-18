//! B7 — the publication allowlist: what a change feed is **allowed** to carry out of the database.
//!
//! Every other guard in this pipeline is about not *losing* a row: the cursor rule in
//! [`super::stream`], the boundary in [`super::snapshot`], the composite idempotence key in the Go
//! sink. This one is the mirror image — it is about not *shipping* one — and the two failure modes
//! need opposite defaults. A lost row is recovered by re-reading the log. A column that has already
//! left the database is not recoverable by anything: it is in a warehouse, a webhook payload and
//! somebody's log aggregator before anyone notices. So this module is an **allowlist**, and
//! everything it has not been told about is refused rather than shipped.
//!
//! # Where it is enforced
//!
//! At [`super::jsonl::to_json_line`], through the mask it hands to `row_into` — the one function
//! that turns a row into bytes. Not at the SQL layer, not in the decoder, and not in the sinks:
//! those are three places that would each need their own copy of the rule, and a fourth write path
//! added later would arrive with none of it. Three write paths already exist and none of them is
//! the "main" one:
//!
//! * [`super::stream::FeedStreamer::pump`] decodes the WAL and streams it,
//! * [`super::snapshot::snapshot_table`] scans the live table for an initial backfill,
//! * `examples/table_dump` renders the same rows as the expected side of `cdc-consumer diff`.
//!
//! All three reach the wire through the renderers in [`super::jsonl`], so that is where the
//! allowlist sits. A future path cannot forget it by omission: the renderers take a `&Publication`
//! they cannot synthesise, so a caller with no policy has to say so in its own source.
//!
//! # Two outcomes, and they are deliberately not the same outcome
//!
//! * A **column** the publication does not name is **withheld**. Its key is left out of the emitted
//!   row and the event carries `"withheld":["ssn"]`, so the consumer is *told* that a column exists
//!   which it is not being sent. Dropping it in silence would break the rule this feed's format is
//!   built on — that a consumer which cannot tell "this column is null" from "this column was not
//!   sent" cannot apply an update correctly (see [`super::jsonl`]).
//! * A **table** the publication does not name is **refused**. No event for it is emitted at all,
//!   and the feed stops there rather than stepping over it. An absent table is a question the
//!   policy has not answered, and the only safe answer to an unanswered question about egress is
//!   no. This is the "could not be evaluated → hard reject" rule the verification gate uses, in the
//!   one place where the cost of guessing is unrecoverable.
//!
//! The asymmetry is the point. Withholding a column still delivers a row a consumer can apply.
//! There is no such thing as delivering a row of a table nobody has decided about.
//!
//! # A refusal is not free, and it must not be silent
//!
//! Refusing an event stalls the feed at that event, on purpose. [`super::stream::FeedStreamer::pump`]
//! carries the hard half of that: the cursor may not advance past a refused **commit**, and "not
//! past" has to mean the commit boundary rather than the offending event, or a sibling row of the
//! same commit takes the cursor over it. A stalled feed is visible in
//! [`super::stream::Pumped::refusal`] and makes [`super::stream::Pumped::is_clean`] false; a feed
//! that quietly skipped the event would be indistinguishable from a feed that never had it.
//!
//! # The declaration
//!
//! One line per table, `table: col, col`. Deliberately **not** JSON, and the reason is not taste:
//! this file is read by two independent programs — this module and `cdc-consumer/publication.go` —
//! and the whole value of reading it twice is that the two readers were written separately. A format
//! whose parser is thirty lines in either language keeps the second implementation honest work
//! rather than a transliteration of the first.
//!
//! ```text
//! publication analytics
//! # everything from a # to end of line is a comment
//! inventory: id, qty
//! orders: id, total
//! ```
//!
//! **The parser can only ever produce a smaller allowlist than its author intended, never a larger
//! one**, and that property is what makes a line-oriented format safe for a security guard.
//! Splitting on `,` cannot invent a name that contains a comma, so a column whose name holds a
//! comma or a colon — this database permits one, and [`super::jsonl`] has a test with a quote in a
//! column name — simply fails to match anything in the set and is withheld. Every way of
//! mis-parsing this file lands on "does not ship", which is the direction that cannot hurt anyone.
//!
//! Refused at parse time, rather than at the first event: a file with no `publication` header, a
//! file that names no table, a table named twice, a table with an empty column list, a duplicated
//! column, or a line that is neither a comment nor `table: cols`. A publication that is wrong is a
//! configuration error, and the moment to say so is when it is loaded.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use crate::catalog::column::Value;
use crate::error::FerroError;

use super::logical::{ChangeEvent, ChangeOp, SchemaChange};

/// What a feed may publish.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Publication {
    /// No policy declared: every table and every column ships, and the bytes are byte-for-byte what
    /// this feed emitted before publications existed.
    ///
    /// **This is the blind spot of the whole mechanism, and it is stated here rather than in a
    /// design note somewhere.** It exists because the renderers require a `&Publication` and the
    /// call sites that predate this module have no policy to give them. Making it the only way to
    /// spell "no policy" means every such site names it in its own source, so
    /// `grep -rn 'unrestricted'` enumerates exactly the paths this guard does not cover.
    ///
    /// What it must never become is a **default**. Nothing in this module hands it to a caller who
    /// forgot to choose, because a guard that defaults to open is a guard that is on in the tests
    /// and off in production.
    Unrestricted,
    /// An allowlist. A table that is not a key of `tables` is refused, never published; a column
    /// that is not in its table's set is withheld.
    Allowlist {
        /// For the refusal messages, so an operator learns *which* publication refused.
        name: String,
        tables: BTreeMap<String, BTreeSet<String>>,
    },
}

/// The decision for one event or one table dump: which columns may become bytes.
///
/// Carries the publication and table names so the byte renderer can mint a refusal without being
/// handed the policy a second time, and the `withheld` list so the emitted event can declare what it
/// is not carrying.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Mask<'a> {
    publication: &'a str,
    table: &'a str,
    /// `None` means every column ships — [`Publication::Unrestricted`].
    published: Option<&'a BTreeSet<String>>,
    withheld: Vec<String>,
}

impl<'a> Mask<'a> {
    /// Whether a column of this name may leave the database.
    pub fn publishes(&self, column: &str) -> bool {
        match self.published {
            None => true,
            Some(set) => set.contains(column),
        }
    }

    /// Whether a value at a position with **no column name** may be emitted under a synthetic one.
    ///
    /// True only under [`Publication::Unrestricted`], where surplus values have always been given
    /// `__unnamed_column_N` so that nothing vanishes unnoticed. Under an allowlist a nameless value
    /// is a value this guard cannot decide about, and the answer to that is a refusal rather than a
    /// guess — see [`RefusalReason::UnnamedValue`].
    pub fn may_name_a_surplus_value(&self) -> bool {
        self.published.is_none()
    }

    /// Columns of this table that exist and are **not** being sent, in the table's column order.
    pub fn withheld(&self) -> &[String] {
        &self.withheld
    }

    /// Refuse a value position that has no column name.
    pub fn refuse_unnamed(&self, index: usize) -> Refusal {
        Refusal {
            publication: self.publication.to_string(),
            table: self.table.to_string(),
            reason: RefusalReason::UnnamedValue { index },
        }
    }
}

/// One event the publication will not let out, and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Refusal {
    pub publication: String,
    pub table: String,
    pub reason: RefusalReason,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RefusalReason {
    /// The publication says nothing about this table. Not "publish none of it" — *nothing*, which is
    /// a different claim and the reason this is a refusal rather than an empty row.
    TableNotPublished,
    /// The table is published but not one of the columns this event carries is, so the honest
    /// rendering of the row is `{}` — an object claiming the row has no columns at all. That is a
    /// false statement about the data rather than a redacted true one.
    NothingPublishable { withheld: Vec<String> },
    /// A value arrived at a position with no column name (more values than names). A guard cannot
    /// decide about a column it cannot name, and a guard that cannot evaluate its own input must
    /// refuse rather than fall through to allow.
    UnnamedValue { index: usize },
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.reason {
            RefusalReason::TableNotPublished => write!(
                f,
                "publication {} does not publish table {}, so its changes are refused rather than \
                 emitted. This is an allowlist: a table it has not been told about is undecided, \
                 not permitted. The feed will not advance past this commit until either the table \
                 is added to the publication (with the columns that may leave) or the feed is run \
                 with a publication that covers it — nothing is lost meanwhile, the commit is \
                 replayed on the next pump.",
                self.publication, self.table
            ),
            RefusalReason::NothingPublishable { withheld } => write!(
                f,
                "publication {} publishes table {} but none of the columns this event carries \
                 ({}), so the row would be emitted as an empty object — which says the row has no \
                 columns rather than that its columns were withheld. Refused instead. Either \
                 publish a column of {} or leave the table out of the publication altogether.",
                self.publication,
                self.table,
                withheld.join(", "),
                self.table
            ),
            RefusalReason::UnnamedValue { index } => write!(
                f,
                "publication {} cannot decide about value {} of table {}: the event carries more \
                 values than column names, so that value has no name to check against the \
                 allowlist. Refused rather than emitted under a synthetic name — an allowlist that \
                 falls through to allow when it cannot read its own input is not an allowlist.",
                self.publication, index, self.table
            ),
        }
    }
}

impl From<Refusal> for FerroError {
    fn from(r: Refusal) -> Self {
        FerroError::Publication(r.to_string())
    }
}

impl Publication {
    /// No policy. See [`Publication::Unrestricted`] for why this has to be written out by name.
    pub fn unrestricted() -> Self {
        Publication::Unrestricted
    }

    /// An empty allowlist, which publishes **nothing** until told about a table.
    ///
    /// Only useful with [`Publication::publishing`]; [`Publication::parse`] refuses an empty one
    /// coming from a file, because a publication that reached production naming no table would
    /// refuse every event in the feed.
    pub fn named(name: &str) -> Self {
        Publication::Allowlist { name: name.to_string(), tables: BTreeMap::new() }
    }

    /// Publish `columns` of `table`. Chainable onto [`Publication::named`].
    ///
    /// Calling it on [`Publication::Unrestricted`] would be a caller trying to narrow something that
    /// is by definition not narrowed, which is a confusion worth refusing rather than silently
    /// resolving one way or the other.
    pub fn publishing<I, S>(mut self, table: &str, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        match &mut self {
            Publication::Unrestricted => panic!(
                "Publication::unrestricted() cannot be narrowed with publishing({table}, ..): an \
                 unrestricted publication already publishes every column of every table. Start \
                 from Publication::named(..) to build an allowlist."
            ),
            Publication::Allowlist { tables, .. } => {
                let set: BTreeSet<String> =
                    columns.into_iter().map(|c| c.as_ref().to_string()).collect();
                tables.insert(table.to_string(), set);
            }
        }
        self
    }

    /// The publication's name, for messages. `(unrestricted)` when there is no policy, spelled with
    /// parentheses so it cannot be mistaken for the name of a real publication in a log line.
    pub fn name(&self) -> &str {
        match self {
            Publication::Unrestricted => "(unrestricted)",
            Publication::Allowlist { name, .. } => name,
        }
    }

    /// Tables this publication names, in sorted order. Empty for [`Publication::Unrestricted`],
    /// which names none because it covers all.
    pub fn tables(&self) -> Vec<&str> {
        match self {
            Publication::Unrestricted => Vec::new(),
            Publication::Allowlist { tables, .. } => tables.keys().map(String::as_str).collect(),
        }
    }

    /// Columns published for `table`, or `None` when the table is not in the publication at all.
    pub fn columns_of(&self, table: &str) -> Option<Vec<&str>> {
        match self {
            Publication::Unrestricted => None,
            Publication::Allowlist { tables, .. } => {
                tables.get(table).map(|c| c.iter().map(String::as_str).collect())
            }
        }
    }

    /// Read a publication from the declaration described in the module docs.
    ///
    /// Every refusal here is a configuration error being reported at load time rather than as a
    /// mysterious stalled feed later. See the module docs for the list and for why mis-parsing this
    /// format can only ever *narrow* the allowlist.
    pub fn parse(text: &str) -> Result<Self, FerroError> {
        let mut name: Option<String> = None;
        let mut tables: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();

        for (i, raw) in text.lines().enumerate() {
            let lineno = i + 1;
            let line = match raw.find('#') {
                Some(at) => &raw[..at],
                None => raw,
            }
            .trim();
            if line.is_empty() {
                continue;
            }
            if let Some(rest) = line.strip_prefix("publication ") {
                let declared = rest.trim();
                if declared.is_empty() {
                    return Err(bad_publication(lineno, "the `publication` header has no name"));
                }
                if name.is_some() {
                    return Err(bad_publication(
                        lineno,
                        "a second `publication` header: one file declares one publication, and \
                         which of the two names the tables belong to would be a guess",
                    ));
                }
                name = Some(declared.to_string());
                continue;
            }
            let Some((table, cols)) = line.split_once(':') else {
                return Err(bad_publication(
                    lineno,
                    &format!(
                        "`{line}` is neither a comment nor `table: col, col`. A line this parser \
                         does not understand is refused rather than skipped: a skipped line is a \
                         table an operator believes is published and is not"
                    ),
                ));
            };
            let table = table.trim();
            if table.is_empty() {
                return Err(bad_publication(lineno, "a column list with no table name"));
            }
            if name.is_none() {
                return Err(bad_publication(
                    lineno,
                    &format!(
                        "table `{table}` appears before any `publication <name>` header, so there \
                         is no publication for it to belong to"
                    ),
                ));
            }
            // Checked before the split, so `t:` is reported as the empty column list it is rather
            // than as "an empty column between commas" — a message naming punctuation the line does
            // not contain, which sends the reader looking for a typo that is not there.
            if cols.trim().is_empty() {
                return Err(bad_publication(lineno, &empty_column_list(table)));
            }
            let mut set: BTreeSet<String> = BTreeSet::new();
            for col in cols.split(',') {
                let col = col.trim();
                if col.is_empty() {
                    return Err(bad_publication(
                        lineno,
                        &format!("table `{table}` has an empty column between commas"),
                    ));
                }
                if !set.insert(col.to_string()) {
                    return Err(bad_publication(
                        lineno,
                        &format!("table `{table}` lists column `{col}` twice"),
                    ));
                }
            }
            if set.is_empty() {
                // Unreachable given the blank-list check above; kept because the invariant this
                // upholds is "no empty set reaches the map", and an unreachable arm that returns the
                // same refusal is cheaper than an arm that would let one through.
                return Err(bad_publication(lineno, &empty_column_list(table)));
            }
            if tables.insert(table.to_string(), set).is_some() {
                return Err(bad_publication(
                    lineno,
                    &format!(
                        "table `{table}` is declared twice; whichever line won would be an \
                         accident of parse order"
                    ),
                ));
            }
        }

        let Some(name) = name else {
            return Err(bad_publication(
                0,
                "no `publication <name>` header. An empty or unrelated file must not be read as a \
                 publication: it would parse as an allowlist that publishes nothing and stall the \
                 whole feed with no clue as to why",
            ));
        };
        if tables.is_empty() {
            return Err(bad_publication(
                0,
                &format!(
                    "publication `{name}` names no table, so it would refuse every event in the \
                     feed. If that is the intent, do not attach a publication at all; if it is \
                     not, the table lines are missing"
                ),
            ));
        }
        Ok(Publication::Allowlist { name, tables })
    }

    /// Load a publication from a file.
    pub fn load(path: &std::path::Path) -> Result<Self, FerroError> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| FerroError::Io(format!("reading publication {}: {e}", path.display())))?;
        Self::parse(&text)
    }

    /// The decision for a bare table + column list — the table-dump path, which has rows but no
    /// events.
    pub fn mask_for<'a>(
        &'a self,
        table: &'a str,
        columns: &[String],
    ) -> Result<Mask<'a>, Refusal> {
        let Some(published) = self.published_columns(table)? else {
            return Ok(Mask {
                publication: self.name(),
                table,
                published: None,
                withheld: Vec::new(),
            });
        };
        let withheld: Vec<String> =
            columns.iter().filter(|c| !published.contains(c.as_str())).cloned().collect();
        if !columns.is_empty() && withheld.len() == columns.len() {
            return Err(Refusal {
                publication: self.name().to_string(),
                table: table.to_string(),
                reason: RefusalReason::NothingPublishable { withheld },
            });
        }
        Ok(Mask { publication: self.name(), table, published: Some(published), withheld })
    }

    /// The decision for one change event.
    ///
    /// Separate from [`Publication::mask_for`] because an event carries values as well as names, and
    /// two of the three refusals can only be seen from here: a `DROP TABLE` has no row to withhold
    /// anything from, and a value with no column name is only visible once the images are in hand.
    pub fn check<'a>(&'a self, e: &'a ChangeEvent) -> Result<Mask<'a>, Refusal> {
        let Some(published) = self.published_columns(&e.table)? else {
            return Ok(Mask {
                publication: self.name(),
                table: &e.table,
                published: None,
                withheld: Vec::new(),
            });
        };

        // **A DROP carries no row and no shape**, so the table decision is the whole decision. Its
        // `before` and `after` are both null (E69), so there is nothing for a column rule to leak
        // and nothing for it to withhold — and running the column rule here would refuse the drop
        // of a table whose published columns had all been dropped first, which is a legitimate
        // event a consumer needs in order to drop its own copy.
        if matches!(&e.op, ChangeOp::Schema { change: SchemaChange::DropTable, .. }) {
            return Ok(Mask {
                publication: self.name(),
                table: &e.table,
                published: Some(published),
                withheld: Vec::new(),
            });
        }

        let mask = self.mask_for(&e.table, &e.columns)?;
        for image in images(&e.op) {
            if image.len() > e.columns.len() {
                return Err(mask.refuse_unnamed(e.columns.len()));
            }
        }
        Ok(mask)
    }

    /// `Ok(None)` for no policy, `Ok(Some(set))` for a published table, `Err` for one the
    /// publication has never heard of.
    fn published_columns(&self, table: &str) -> Result<Option<&BTreeSet<String>>, Refusal> {
        match self {
            Publication::Unrestricted => Ok(None),
            Publication::Allowlist { name, tables } => match tables.get(table) {
                // An empty set can only arrive through `publishing(t, [])`, since `parse` refuses
                // it. Treated as the refusal it is rather than as a table with nothing to send: the
                // two are the same policy and one message for both is one place to fix.
                Some(cols) if !cols.is_empty() => Ok(Some(cols)),
                _ => Err(Refusal {
                    publication: name.clone(),
                    table: table.to_string(),
                    reason: RefusalReason::TableNotPublished,
                }),
            },
        }
    }
}

/// The row images an op carries. A schema event carries none: its payload is the table's shape,
/// which [`super::jsonl`] projects through the same mask.
fn images(op: &ChangeOp) -> Vec<&[Value]> {
    match op {
        ChangeOp::Read { row } => vec![row.as_slice()],
        ChangeOp::Insert { new } => vec![new.as_slice()],
        ChangeOp::Delete { old } => vec![old.as_slice()],
        ChangeOp::Update { old, new } => vec![old.as_slice(), new.as_slice()],
        ChangeOp::Schema { .. } => Vec::new(),
    }
}

/// The one wording for "this table publishes nothing", used by both the blank-list check and the
/// unreachable set-is-empty arm, so the two cannot drift into two explanations of one policy error.
fn empty_column_list(table: &str) -> String {
    format!(
        "table `{table}` lists no columns. A table that publishes nothing must be left out of the \
         publication rather than named with an empty list — as written, every event for it would be \
         refused, which reads as a bug in the feed rather than as the policy it is"
    )
}

fn bad_publication(lineno: usize, why: &str) -> FerroError {
    if lineno == 0 {
        FerroError::Publication(format!("publication declaration: {why}"))
    } else {
        FerroError::Publication(format!("publication declaration, line {lineno}: {why}"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::replication::logical::ColumnSpec;

    fn event(table: &str, columns: &[&str], op: ChangeOp) -> ChangeEvent {
        ChangeEvent {
            txn_id: 2,
            lsn: 216,
            commit_lsn: 422,
            commit_end_lsn: 455,
            table: table.into(),
            columns: Arc::new(columns.iter().map(|c| c.to_string()).collect()),
            op,
        }
    }

    fn insert(values: Vec<Value>) -> ChangeOp {
        ChangeOp::Insert { new: values }
    }

    /// The happy path, with its anti-vacuity half attached: the publication narrows to exactly what
    /// it names, and the columns it does name still ship.
    ///
    /// Breaking shape: a parser that returned an empty allowlist on a well-formed file would refuse
    /// everything, and a test that only asserted "the secret is withheld" would pass against it.
    #[test]
    fn parse_narrows_to_exactly_what_it_names() {
        let p = Publication::parse(
            "publication analytics\n\
             # the customer table's ssn must never leave\n\
             customers: id, name\n\
             \n\
             orders: id, total\n",
        )
        .expect("a well-formed declaration was refused");
        assert_eq!(p.name(), "analytics");
        assert_eq!(p.tables(), vec!["customers", "orders"]);
        assert_eq!(p.columns_of("customers"), Some(vec!["id", "name"]));
        assert_eq!(p.columns_of("nowhere"), None);

        let e = event("customers", &["id", "name", "ssn"], insert(vec![
            Value::Integer(1),
            Value::Varchar("ada".into()),
            Value::Varchar("000-11-2222".into()),
        ]));
        let mask = p.check(&e).expect("a published table was refused");
        assert!(mask.publishes("id") && mask.publishes("name"), "a published column was withheld");
        assert!(!mask.publishes("ssn"), "the denied column was published");
        assert_eq!(mask.withheld(), ["ssn"], "the withholding was not declared");
    }

    /// **A file that is not a publication must not read as one that publishes nothing.**
    ///
    /// Breaking shape: pointing the flag at the wrong file, or at a file whose header was lost in an
    /// edit. Parsed permissively it becomes an empty allowlist, which refuses every event in the
    /// feed — a total stall whose cause is invisible at the point it is felt.
    #[test]
    fn a_declaration_with_no_header_is_refused() {
        let err = Publication::parse("customers: id, name\n").expect_err("headerless file accepted");
        assert!(format!("{err}").contains("publication <name>"), "wrong reason: {err}");

        // Anti-vacuity: the same file with a header parses.
        Publication::parse("publication p\ncustomers: id, name\n").expect("header form refused");
    }

    /// A header with no tables refuses every event, so it is refused at load instead.
    #[test]
    fn a_publication_that_names_no_table_is_refused_at_parse() {
        let err = Publication::parse("publication p\n# nothing yet\n").expect_err("accepted");
        assert!(format!("{err}").contains("names no table"), "wrong reason: {err}");
    }

    /// An empty column list is a denial written as a permission, and the two must not look alike.
    #[test]
    fn a_table_with_no_columns_is_refused_at_parse() {
        let err = Publication::parse("publication p\ncustomers:\n").expect_err("accepted");
        assert!(format!("{err}").contains("lists no columns"), "wrong reason: {err}");

        // Anti-vacuity: one column is enough.
        Publication::parse("publication p\ncustomers: id\n").expect("one column refused");
    }

    /// Two lines for one table: whichever won would be an accident of parse order, and the loser is
    /// silently narrower or wider than the operator believes.
    #[test]
    fn a_table_declared_twice_is_refused() {
        let err = Publication::parse("publication p\nt: id\nt: id, ssn\n").expect_err("accepted");
        assert!(format!("{err}").contains("declared twice"), "wrong reason: {err}");
    }

    #[test]
    fn a_duplicate_column_is_refused() {
        let err = Publication::parse("publication p\nt: id, id\n").expect_err("accepted");
        assert!(format!("{err}").contains("twice"), "wrong reason: {err}");
    }

    /// **A line the parser does not understand is refused, not skipped.**
    ///
    /// Breaking shape: `customers id, name` — a missing colon, which is exactly what a hand-edited
    /// file gets wrong. Skipped, the operator reads the file and sees `customers` published while
    /// the feed refuses every one of its events.
    #[test]
    fn a_line_the_parser_cannot_read_is_refused_rather_than_skipped() {
        let err =
            Publication::parse("publication p\nt: id\ncustomers id, name\n").expect_err("accepted");
        let msg = format!("{err}");
        assert!(msg.contains("neither a comment nor"), "wrong reason: {msg}");
        assert!(msg.contains("line 3"), "the message does not locate the line: {msg}");
    }

    /// **The fail-safe direction of a line-oriented format, asserted rather than asserted about.**
    ///
    /// This database allows a column name containing a comma — `jsonl` has a test with a quote in
    /// one. The declaration cannot express such a name, so it can never end up in the allowlist, so
    /// the column is withheld. Every way of mis-splitting this file narrows the allowlist; none of
    /// them widens it, which is the only property a security guard needs from its own parser.
    ///
    /// Breaking shape: a format where a mis-parse could *add* a name — a glob, a prefix rule, or a
    /// regex — where a typo silently publishes a neighbouring column.
    #[test]
    fn a_column_name_the_format_cannot_express_is_withheld_never_published() {
        let p = Publication::parse("publication p\nt: id, qty, secret\n").unwrap();
        assert_eq!(p.columns_of("t"), Some(vec!["id", "qty", "secret"]));
        // A real column literally named `qty, secret`, which no line in that file can name.
        let e = event("t", &["id", "qty, secret"], insert(vec![
            Value::Integer(1),
            Value::Integer(9),
        ]));
        let mask = p.check(&e).unwrap();
        assert!(mask.publishes("id"));
        assert!(
            !mask.publishes("qty, secret"),
            "splitting on the separator invented a name that ships; the parser can widen the \
             allowlist and is therefore not safe as one"
        );
        assert_eq!(mask.withheld(), ["qty, secret"]);
    }

    /// **A table the publication has never heard of is refused**, and this is the case an operator
    /// hits by creating a table after writing the publication.
    ///
    /// Breaking shape: a denylist. `CREATE TABLE audit_log (...)` after the policy was written is
    /// published in full by a denylist and refused by an allowlist, and the operator finds out from
    /// the stalled feed rather than from the warehouse.
    #[test]
    fn a_table_that_is_not_in_the_publication_is_refused() {
        let p = Publication::parse("publication p\nt: id\n").unwrap();
        let e = event("audit_log", &["id", "actor"], insert(vec![
            Value::Integer(1),
            Value::Varchar("root".into()),
        ]));
        let r = p.check(&e).expect_err("an undecided table was published");
        assert_eq!(r.reason, RefusalReason::TableNotPublished);
        assert!(format!("{r}").contains("allowlist"), "the message does not say why: {r}");

        // Anti-vacuity: the table it does name is not refused.
        p.check(&event("t", &["id"], insert(vec![Value::Integer(1)]))).expect("published refused");
    }

    /// A table named with an empty set through the builder is the same policy as an absent one, and
    /// gets the same refusal rather than an empty row.
    #[test]
    fn a_table_published_with_no_columns_is_refused_like_an_absent_one() {
        let p = Publication::named("p").publishing("t", Vec::<String>::new());
        let r = p
            .check(&event("t", &["id"], insert(vec![Value::Integer(1)])))
            .expect_err("a table with an empty column set published a row");
        assert_eq!(r.reason, RefusalReason::TableNotPublished);
    }

    /// **Withholding every column would emit `{}`, which says the row has no columns.** That is a
    /// false statement rather than a redacted true one, so it is refused.
    ///
    /// Breaking shape: `secrets(token)` under a publication naming `secrets: id` — a table whose
    /// only column is denied. Emitted as `{}` a consumer materialises a row it cannot key and calls
    /// the feed healthy.
    #[test]
    fn an_event_whose_every_column_is_withheld_is_refused() {
        let p = Publication::parse("publication p\nsecrets: id\n").unwrap();
        let r = p
            .check(&event("secrets", &["token"], insert(vec![Value::Varchar("t".into())])))
            .expect_err("a fully withheld row was emitted");
        assert!(matches!(r.reason, RefusalReason::NothingPublishable { .. }), "{r:?}");

        // Anti-vacuity: one publishable column and the same table ships.
        p.check(&event("secrets", &["id", "token"], insert(vec![
            Value::Integer(1),
            Value::Varchar("t".into()),
        ])))
        .expect("a row with one published column was refused");
    }

    /// **A value with no column name cannot be decided about, so it is refused.**
    ///
    /// Breaking shape: an event carrying more values than the catalog has names for — which `jsonl`
    /// already handles for an unrestricted feed by emitting `__unnamed_column_1`, precisely so
    /// nothing vanishes unnoticed. Under an allowlist that synthetic name is a column no policy has
    /// decided about, and emitting it would be the guard falling through to allow when it cannot
    /// read its own input.
    #[test]
    fn a_value_with_no_column_name_is_refused_under_an_allowlist() {
        let p = Publication::parse("publication p\nt: a\n").unwrap();
        let e = event("t", &["a"], insert(vec![Value::Integer(1), Value::Integer(2)]));
        let r = p.check(&e).expect_err("a nameless value was allowed out");
        assert_eq!(r.reason, RefusalReason::UnnamedValue { index: 1 });

        // Anti-vacuity, and the documented behaviour it must not change: with no policy the same
        // event is fine, because `jsonl` names the surplus visibly.
        let none = Publication::unrestricted();
        let mask = none.check(&e).expect("unrestricted refused a surplus");
        assert!(mask.may_name_a_surplus_value());
    }

    /// Both images of an UPDATE are checked. The `before` image is the one a naive projection
    /// forgets, and it is the one that carries the old value of the denied column.
    #[test]
    fn both_images_of_an_update_are_covered_by_the_value_check() {
        let p = Publication::parse("publication p\nt: a\n").unwrap();
        // Surplus in the BEFORE image only.
        let e = event("t", &["a"], ChangeOp::Update {
            old: vec![Value::Integer(1), Value::Integer(99)],
            new: vec![Value::Integer(1)],
        });
        assert_eq!(
            p.check(&e).expect_err("a nameless value in the before image was allowed out").reason,
            RefusalReason::UnnamedValue { index: 1 }
        );
    }

    /// **A DROP is decided by its table alone.** It carries no row and no shape — both images are
    /// null — so there is nothing to withhold, and running the column rule over the dropped table's
    /// former shape would refuse a consumer the one event that tells it to drop its own copy.
    #[test]
    fn a_drop_table_is_decided_by_the_table_and_not_by_its_columns() {
        let p = Publication::parse("publication p\nt: id\n").unwrap();
        let drop = event("t", &["gone_secret"], ChangeOp::Schema {
            change: SchemaChange::DropTable,
            columns: Vec::new(),
        });
        let mask = p.check(&drop).expect("a drop of a published table was refused");
        assert!(mask.withheld().is_empty());

        // And a drop of a table the publication does not name is still refused.
        let other = event("audit_log", &[], ChangeOp::Schema {
            change: SchemaChange::DropTable,
            columns: Vec::new(),
        });
        assert_eq!(
            p.check(&other).expect_err("a drop of an undecided table was emitted").reason,
            RefusalReason::TableNotPublished
        );
    }

    /// A CREATE_TABLE's shape is a row of names rather than of values, and the mask still applies to
    /// it — asserted here at the decision level, and at the byte level in `jsonl`.
    #[test]
    fn a_create_table_shape_is_masked_like_a_row() {
        let p = Publication::parse("publication p\nt: id\n").unwrap();
        let e = event("t", &["id", "ssn"], ChangeOp::Schema {
            change: SchemaChange::CreateTable,
            columns: vec![
                ColumnSpec { name: "id".into(), sql_type: "INTEGER".into(), nullable: false },
                ColumnSpec { name: "ssn".into(), sql_type: "VARCHAR(16)".into(), nullable: true },
            ],
        });
        let mask = p.check(&e).expect("a create of a published table was refused");
        assert!(mask.publishes("id") && !mask.publishes("ssn"));
        assert_eq!(mask.withheld(), ["ssn"]);
    }

    /// With no policy nothing is withheld and nothing is refused — the state every call site that
    /// predates this module is in, asserted so a change to the mask cannot quietly alter it.
    #[test]
    fn unrestricted_publishes_everything() {
        let p = Publication::unrestricted();
        let e = event("anything", &["a", "b"], insert(vec![Value::Integer(1), Value::Integer(2)]));
        let mask = p.check(&e).expect("unrestricted refused an event");
        assert!(mask.publishes("a") && mask.publishes("b") && mask.publishes("never_declared"));
        assert!(mask.withheld().is_empty());
        assert_eq!(p.name(), "(unrestricted)");
        assert!(p.tables().is_empty());
    }

    /// The comment and blank-line handling, including a `#` at end of line.
    #[test]
    fn comments_and_blank_lines_are_ignored() {
        let p = Publication::parse(
            "# leading comment\n\npublication p # named here\nt: id, qty # only these two\n",
        )
        .unwrap();
        assert_eq!(p.name(), "p");
        assert_eq!(p.columns_of("t"), Some(vec!["id", "qty"]));
    }

    /// `mask_for` is the table-dump path: same allowlist, no event.
    #[test]
    fn mask_for_decides_a_bare_table_and_column_list() {
        let p = Publication::parse("publication p\nt: id\n").unwrap();
        let cols = vec!["id".to_string(), "ssn".to_string()];
        let mask = p.mask_for("t", &cols).expect("a published table was refused");
        assert!(mask.publishes("id") && !mask.publishes("ssn"));
        assert_eq!(
            p.mask_for("other", &cols).expect_err("an undecided table dumped rows").reason,
            RefusalReason::TableNotPublished
        );
    }

    /// A refusal renders as an explanation with the publication, the table and what to do — a
    /// message an operator reads once, at the point the feed stalls.
    #[test]
    fn a_refusal_says_which_publication_refused_what() {
        let p = Publication::parse("publication analytics\nt: id\n").unwrap();
        let r = p
            .check(&event("audit_log", &["id"], insert(vec![Value::Integer(1)])))
            .expect_err("not refused");
        let msg = format!("{r}");
        assert!(msg.contains("analytics") && msg.contains("audit_log"), "{msg}");
        assert!(msg.contains("replayed"), "the message does not say the commit is not lost: {msg}");
        // And it converts into the error class a caller propagates.
        let e: FerroError = r.into();
        assert!(matches!(e, FerroError::Publication(_)), "{e:?}");
        assert!(format!("{e}").contains("publication refused"), "{e}");
    }
}
