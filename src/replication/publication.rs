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
//! * A **column** the publication does not name is **withheld**: its key is left out of every
//!   emitted image, and nothing on the wire names it — see [`super::jsonl`] for why no `withheld`
//!   list is emitted, and why projecting the `CREATE_TABLE` shape through the same mask is what makes
//!   the absence honest rather than silent. A guard whose purpose is that a column cannot leave must
//!   not ship the name of the column it is protecting.
//! * A **table** the publication does not name is **refused**. No event for it is emitted at all,
//!   and the feed stops there rather than stepping over it. An absent table is a question the
//!   policy has not answered, and the only safe answer to an unanswered question about egress is
//!   no. This is the "could not be evaluated → hard reject" rule the verification gate uses, in the
//!   one place where the cost of guessing is unrecoverable.
//!
//! The asymmetry is the point. Withholding a column still delivers a row a consumer can apply.
//! There is no such thing as delivering a row of a table nobody has decided about.
//!
//! # `exclude`, and why a refusal needed an alternative
//!
//! A third form, `exclude <table>`, drops that table's changes and lets the feed carry on. It is not
//! a hole in the allowlist — it is the operator making the decision that a refusal says has not been
//! made — and without it the guard is barely deployable. An adversarial review of this lane found why:
//! a refusal stops the cursor, a stopped cursor is a [`super::stream::Subscription`] whose pin sits on
//! that commit, and `WalManager::truncate` reclaims nothing at or above the oldest pin. So an
//! undecided table is not a quiet feed, it is a **log that grows without bound** until someone edits
//! the policy — and the only edit the language offered was to publish the table that was being kept
//! in. `exclude` is the other edit, and the refusal message now names both.
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
//! # a line whose first non-blank character is # is a comment
//! inventory: id, qty
//! orders: id, total
//! ```
//!
//! **Every name it accepts is exactly the name in the file**, and that property is what makes a
//! line-oriented format usable as a security guard. It rests on two things rather than on hope:
//!
//! * every table and column name this database can create is an identifier — the scanner accepts
//!   `[A-Za-z0-9_]` and there are no quoted identifiers (`src/parser/scanner.rs`, `identifier`) — and
//!   this parser accepts exactly that set and **refuses** anything else, by name, at load time. So no
//!   accepted name can contain a separator, and a name that contains one cannot be silently split
//!   into something shorter that happens to match a real column.
//! * `#` starts a comment only at the beginning of a line. This is the correction that matters, and
//!   it was found by an adversarial review of this file rather than by writing it: with `#` honoured
//!   mid-line, `customers: id, ssn#hash` truncated to `customers: id, ssn` and **published `ssn`** —
//!   a widening, in the exact column the docs here use as the thing that must never leave, and one
//!   both this parser and the Go one made identically, so reading the file twice could not see it.
//!
//! An earlier version of this doc claimed instead that mis-parsing "can only ever narrow" because
//! splitting on `,` cannot invent a comma. That was false in one direction and unfounded in the
//! other: the `#` case widens, and the justification rested on this database permitting a comma in a
//! column name, which it does not — SQL cannot express such a name at all. The claim is recorded here
//! as corrected rather than quietly replaced, because it was the stated reason for choosing the
//! format.
//!
//! Refused at parse time, rather than at the first event: a file with no `publication` header, a file
//! that names no table, a table named twice, a table with an empty column list, a duplicated column,
//! a name that is not an identifier, or a line that is neither a comment nor `table: cols`. A
//! publication that is wrong is a configuration error, and the moment to say so is when it is loaded.

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
    /// An allowlist. A table that is in neither `tables` nor `excluded` is refused, never published;
    /// a column that is not in its table's set is withheld.
    Allowlist {
        /// For the refusal messages, so an operator learns *which* publication refused.
        name: String,
        tables: BTreeMap<String, BTreeSet<String>>,
        /// Tables the operator has decided **not** to publish, which is a different thing from a
        /// table the policy has never heard of — see [`Publication::excludes`]. This exists because a
        /// refusal is not free: it stops the feed, and a stopped feed's subscription pins the WAL, so
        /// an undecided table is a growing log and not merely a quiet one. Without a way to say "do
        /// not publish this and keep going", the only escape from that stall would be to publish the
        /// very table someone was trying to keep in.
        excluded: BTreeSet<String>,
    },
}

/// The decision for one event or one table dump: which columns may become bytes.
///
/// Carries the publication and table names so the byte renderer can mint a refusal without being
/// handed the policy a second time, and the `withheld` list so a refusal and an operator's report can
/// name what was held back. The list is never written to the feed — see [`Mask::withheld`].
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
    ///
    /// For refusal messages and for an operator-facing report — `table_dump` prints it to stderr, to
    /// the terminal of the person who passed the policy file. **Never written to the feed.** The
    /// names would be egress in their own right, and a consumer has no use for them: the shape it is
    /// given is projected too, so it never learns that the column exists.
    pub fn withheld(&self) -> &[String] {
        &self.withheld
    }

    /// Refuse an image whose every column was withheld.
    ///
    /// Minted from the mask rather than recomputed, so the refusal names the same withheld set the
    /// renderer was working from. The byte renderer needs this for a `CREATE_TABLE` whose declared
    /// shape projects to nothing: an empty column list is not "no columns withheld", it is a claim
    /// that the table HAS no columns, and the Go consumer refuses that outright.
    pub fn refuse_nothing_publishable(&self) -> Refusal {
        Refusal {
            publication: self.publication.to_string(),
            table: self.table.to_string(),
            reason: RefusalReason::NothingPublishable { withheld: self.withheld.clone() },
        }
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
    /// The publication names this table under `exclude`, and something asked to render it anyway.
    ///
    /// A refusal rather than a silent drop, because the paths that reach it name one table explicitly
    /// — a snapshot of it, a dump of it — so the caller has asked for something the policy forbids and
    /// wants to hear so. The **stream** does not reach it: [`super::stream::FeedStreamer::pump`] drops
    /// an excluded table's events and keeps moving, which is the whole point of the form.
    TableExcluded,
    /// The table is published but not one of the columns this event carries is, so the honest
    /// rendering of the row is `{}` — an object claiming the row has no columns at all. That is a
    /// false statement about the data rather than a redacted true one.
    NothingPublishable { withheld: Vec<String> },
    /// A value arrived at a position with no column name (more values than names). A guard cannot
    /// decide about a column it cannot name, and a guard that cannot evaluate its own input must
    /// refuse rather than fall through to allow.
    UnnamedValue { index: usize },
    /// Two of the event's columns have the same name, so a name-keyed policy cannot tell them apart.
    ///
    /// This is not hypothetical and it is not a hand-built shape: `CREATE TABLE t (id INTEGER, name
    /// VARCHAR(8), name VARCHAR(8))` is accepted by this database today, `SELECT *` returns all three
    /// values, and the binder itself calls the name ambiguous when asked for it. Found by an
    /// adversarial review, which got a denied value all the way into a SQLite destination: the mask
    /// decides per POSITION by looking that position's NAME up in the allowlist, both positions
    /// answered yes, and Go's `encoding/json` keeps the LAST duplicate key — so the withheld
    /// column's value overwrote the published one in the destination row.
    ///
    /// There is no declaration that fixes it: `t: id, name, name` is refused as a duplicate, and
    /// `t: id` withholds both. So the answer is a refusal, which is what a guard owes a question it
    /// cannot answer.
    AmbiguousColumns { name: String },
}

impl fmt::Display for Refusal {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.reason {
            RefusalReason::TableNotPublished => write!(
                f,
                "publication {} says nothing about table {}, so its changes are refused rather than \
                 emitted. This is an allowlist: a table it has not been told about is undecided, not \
                 permitted. No row is lost — the feed will not advance past this commit, and replays \
                 it once the policy decides — but a stalled feed is not merely a quiet one: the \
                 consumer's subscription pins the log at this commit, so the WAL cannot be reclaimed \
                 while the stall lasts. Decide, either way: add `{}: <columns>` to publish it, or \
                 `exclude {}` to drop its changes and keep the feed moving.",
                self.publication, self.table, self.table, self.table
            ),
            RefusalReason::TableExcluded => write!(
                f,
                "publication {} excludes table {}, and this asked to render it anyway. A stream drops \
                 an excluded table's changes and carries on; a snapshot or a dump names one table \
                 explicitly, so being handed an excluded one is a caller asking for what the policy \
                 forbids rather than a row to skip. Remove the `exclude {}` line to publish it, or do \
                 not ask for that table.",
                self.publication, self.table, self.table
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
            RefusalReason::AmbiguousColumns { name } => write!(
                f,
                "table {} has two columns named {}, so publication {} cannot decide about either: an \
                 allowlist is keyed by name and these two positions share one. Refused rather than \
                 guessed — publishing the name would ship both positions, one of which nobody \
                 decided about, and a consumer parsing JSON keeps only one of the two values. No \
                 publication text distinguishes them; the table itself has to.",
                self.table, name, self.publication
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
        Publication::Allowlist {
            name: name.to_string(),
            tables: BTreeMap::new(),
            excluded: BTreeSet::new(),
        }
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
            Publication::Allowlist { tables, excluded, .. } => {
                let set: BTreeSet<String> =
                    columns.into_iter().map(|c| c.as_ref().to_string()).collect();
                excluded.remove(table);
                tables.insert(table.to_string(), set);
            }
        }
        self
    }

    /// Exclude `table`: decided, and decided against. Chainable onto [`Publication::named`].
    pub fn excluding(mut self, table: &str) -> Self {
        match &mut self {
            Publication::Unrestricted => panic!(
                "Publication::unrestricted() cannot exclude {table}: it publishes everything by \
                 definition. Start from Publication::named(..) to build an allowlist."
            ),
            Publication::Allowlist { tables, excluded, .. } => {
                tables.remove(table);
                excluded.insert(table.to_string());
            }
        }
        self
    }

    /// Whether the operator has decided this table must **not** be published.
    ///
    /// Distinct from "not published" in the refusal sense, and the whole reason the form exists:
    /// [`super::stream::FeedStreamer::pump`] drops an excluded table's events and keeps the cursor
    /// moving, where an undecided table stops it. Always false under [`Publication::Unrestricted`].
    pub fn excludes(&self, table: &str) -> bool {
        match self {
            Publication::Unrestricted => false,
            Publication::Allowlist { excluded, .. } => excluded.contains(table),
        }
    }

    /// Tables this publication explicitly excludes, in sorted order.
    pub fn excluded_tables(&self) -> Vec<&str> {
        match self {
            Publication::Unrestricted => Vec::new(),
            Publication::Allowlist { excluded, .. } => excluded.iter().map(String::as_str).collect(),
        }
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
    /// Tables this publication publishes, in sorted order. Empty for [`Publication::Unrestricted`],
    /// which names none because it covers all.
    ///
    /// A table whose column set is empty is **not** listed, because `check` refuses it: an accessor
    /// that reported a table as published while every event for it was refused would put an
    /// operator-facing report in direct contradiction with the enforced policy.
    pub fn tables(&self) -> Vec<&str> {
        match self {
            Publication::Unrestricted => Vec::new(),
            Publication::Allowlist { tables, .. } => {
                tables.iter().filter(|(_, c)| !c.is_empty()).map(|(t, _)| t.as_str()).collect()
            }
        }
    }

    /// Columns published for `table`, or `None` when the table is not in the publication at all.
    pub fn columns_of(&self, table: &str) -> Option<Vec<&str>> {
        match self {
            Publication::Unrestricted => None,
            Publication::Allowlist { tables, .. } => tables
                .get(table)
                .filter(|c| !c.is_empty())
                .map(|c| c.iter().map(String::as_str).collect()),
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
        let mut excluded: BTreeSet<String> = BTreeSet::new();

        for (i, raw) in text.lines().enumerate() {
            let lineno = i + 1;
            let line = raw.trim();
            // A comment is a WHOLE line. Honouring `#` mid-line let `t: id, ssn#hash` truncate to
            // `t: id, ssn` and publish `ssn` — see the module docs; that is a widening, and it is the
            // one direction this format must not have.
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            // Matched on the WORD, not on "publication " with its space: a header line whose name
            // was deleted trims to a bare `publication`, which the prefix form misses entirely and
            // then reports as "neither a comment nor `table: col, col`" - a message that sends the
            // reader looking for a missing colon on a line that plainly says publication.
            if line == "publication" || line.starts_with("publication ") {
                let declared = line["publication".len()..].trim();
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
            // `exclude <table>`: the operator deciding NOT to publish, which is a different thing
            // from the policy never having heard of the table. See the module docs for why a refusal
            // needed an alternative at all - a stalled feed pins the WAL.
            if line == "exclude" || line.starts_with("exclude ") {
                let table = line["exclude".len()..].trim();
                if table.is_empty() {
                    return Err(bad_publication(lineno, "`exclude` with no table name"));
                }
                if !is_identifier(table) {
                    return Err(bad_publication(lineno, &not_an_identifier("table", table)));
                }
                if name.is_none() {
                    return Err(bad_publication(
                        lineno,
                        &format!(
                            "`exclude {table}` appears before any `publication <name>` header, so \
                             there is no publication for it to belong to"
                        ),
                    ));
                }
                if tables.contains_key(table) {
                    return Err(bad_publication(
                        lineno,
                        &format!(
                            "table `{table}` is both published and excluded; one of the two lines is \
                             a mistake and guessing which would be the wrong kind of helpful"
                        ),
                    ));
                }
                if !excluded.insert(table.to_string()) {
                    return Err(bad_publication(
                        lineno,
                        &format!("table `{table}` is excluded twice"),
                    ));
                }
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
            if !is_identifier(table) {
                return Err(bad_publication(lineno, &not_an_identifier("table", table)));
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
                if !is_identifier(col) {
                    return Err(bad_publication(lineno, &not_an_identifier("column", col)));
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
            if excluded.contains(table) {
                return Err(bad_publication(
                    lineno,
                    &format!(
                        "table `{table}` is both excluded and published; one of the two lines is a \
                         mistake and guessing which would be the wrong kind of helpful"
                    ),
                ));
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
                    "publication `{name}` publishes no table{}, so nothing would ever be emitted. If \
                     that is the intent, do not attach a publication at all; if it is not, the table \
                     lines are missing",
                    if excluded.is_empty() { "" } else { " (only exclusions)" }
                ),
            ));
        }
        Ok(Publication::Allowlist { name, tables, excluded })
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
        // **Two columns with one name cannot be decided about.** This database accepts
        // `CREATE TABLE t (id INTEGER, name VARCHAR(8), name VARCHAR(8))`, and the mask is keyed by
        // name, so both positions would answer the same way - shipping a position nobody decided
        // about. Checked here rather than in `check` so the table-dump path is covered by the same
        // decision.
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for c in columns {
            if !seen.insert(c.as_str()) {
                return Err(Refusal {
                    publication: self.name().to_string(),
                    table: table.to_string(),
                    reason: RefusalReason::AmbiguousColumns { name: c.clone() },
                });
            }
        }
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
            Publication::Allowlist { name, tables, excluded } => {
                if excluded.contains(table) {
                    return Err(Refusal {
                        publication: name.clone(),
                        table: table.to_string(),
                        reason: RefusalReason::TableExcluded,
                    });
                }
                match tables.get(table) {
                // An empty set can only arrive through `publishing(t, [])`, since `parse` refuses
                // it. Treated as the refusal it is rather than as a table with nothing to send: the
                // two are the same policy and one message for both is one place to fix.
                    Some(cols) if !cols.is_empty() => Ok(Some(cols)),
                    _ => Err(Refusal {
                        publication: name.clone(),
                        table: table.to_string(),
                        reason: RefusalReason::TableNotPublished,
                    }),
                }
            }
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

/// Whether `s` is a name this database could have created: the scanner's identifier rule, which is
/// `[A-Za-z0-9_]+` with no quoted-identifier form (`src/parser/scanner.rs`).
///
/// Restricting the declaration to exactly that set is what makes "the name in the file is the name
/// being matched" true rather than hoped for: nothing the parser accepts can contain a separator, so
/// no accepted name can be a fragment of a longer one.
fn is_identifier(s: &str) -> bool {
    !s.is_empty() && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

fn not_an_identifier(kind: &str, name: &str) -> String {
    format!(
        "{kind} name `{name}` is not an identifier. Names here are [A-Za-z0-9_], which is everything \
         this database's scanner can create; anything else is refused rather than trimmed, because a \
         name containing a separator or a `#` would otherwise be silently shortened into a DIFFERENT \
         name that may match a real column and publish it"
    )
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
        // Two guards reach this, and both are exercised: a table line met before any header is
        // refused where it sits, and a file with no table lines at all falls through to the check at
        // the end. The first is what a lost header looks like in a real file; the second is what
        // pointing the flag at the wrong file looks like.
        let err = Publication::parse("customers: id, name\n").expect_err("headerless file accepted");
        assert!(
            format!("{err}").contains("before any `publication <name>` header"),
            "wrong reason: {err}"
        );
        let err = Publication::parse("# not a publication at all\n").expect_err("accepted");
        assert!(format!("{err}").contains("no `publication <name>` header"), "wrong reason: {err}");

        // Anti-vacuity: the same file with a header parses.
        Publication::parse("publication p\ncustomers: id, name\n").expect("header form refused");
    }

    /// A header with no tables refuses every event, so it is refused at load instead.
    #[test]
    fn a_publication_that_names_no_table_is_refused_at_parse() {
        let err = Publication::parse("publication p\n# nothing yet\n").expect_err("accepted");
        assert!(format!("{err}").contains("publishes no table"), "wrong reason: {err}");

        // Exclusions alone are the same emptiness with a more misleading look: the file names tables,
        // and publishes none of them.
        let err = Publication::parse("publication p\nexclude t\n").expect_err("accepted");
        let msg = format!("{err}");
        assert!(msg.contains("publishes no table"), "wrong reason: {msg}");
        assert!(msg.contains("only exclusions"), "the message does not say what it saw: {msg}");
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

    /// A header whose name was deleted must be reported as a nameless header, not as a line the
    /// parser cannot read: the message is what an operator uses to find the edit that broke it.
    #[test]
    fn a_header_with_no_name_is_refused_as_a_nameless_header() {
        let err = Publication::parse("publication \nt: id\n").expect_err("accepted");
        assert!(format!("{err}").contains("header has no name"), "wrong reason: {err}");
        let err = Publication::parse("publication\nt: id\n").expect_err("accepted");
        assert!(format!("{err}").contains("header has no name"), "wrong reason: {err}");
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

    /// A name the declaration cannot express is withheld rather than published.
    ///
    /// The event here carries a column literally named `qty, secret`, which no line in the file can
    /// name. Such a name cannot be created through SQL — the scanner's identifiers are `[A-Za-z0-9_]`
    /// — so this is a hand-built event standing in for any name outside that set: it is not in the
    /// published set, so it does not ship.
    ///
    /// This test used to carry the claim that mis-parsing the file "can only ever narrow" the
    /// allowlist. That was wrong — see `a_name_containing_the_comment_character_is_refused_rather
    /// _than_shortened` for the case that widened it — and the protection is not the splitting, it is
    /// that a non-identifier name in the FILE is now refused outright.
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

    /// Comments are WHOLE lines, and the blank-line handling.
    #[test]
    fn whole_line_comments_and_blank_lines_are_ignored() {
        let p = Publication::parse(
            "# leading comment\n\n   # indented comment\npublication p\nt: id, qty\n",
        )
        .unwrap();
        assert_eq!(p.name(), "p");
        assert_eq!(p.columns_of("t"), Some(vec!["id", "qty"]));
    }

    /// **The widening this format used to have, kept as a regression test.**
    ///
    /// Breaking shape: `customers: id, ssn#hash` — a column whose name contains the comment
    /// character. With `#` honoured mid-line the declaration truncated to `customers: id, ssn` and
    /// **published `ssn`**, the column the docs here use as the example of what must never leave. Both
    /// parsers made the same truncation, so reading the file twice could not catch it. Found by an
    /// adversarial review, not by writing the parser.
    ///
    /// The fix has two halves and both are asserted: `#` starts a comment only at the start of a line,
    /// and a name that is not an identifier is refused by name instead of being trimmed into one.
    #[test]
    fn a_name_containing_the_comment_character_is_refused_rather_than_shortened() {
        let err = Publication::parse("publication p\ncustomers: id, ssn#hash\n")
            .expect_err("a name containing # was accepted");
        let msg = format!("{err}");
        assert!(msg.contains("not an identifier"), "wrong reason: {msg}");
        assert!(msg.contains("ssn#hash"), "the message does not name the offender: {msg}");

        // The widening the old parser produced, asserted as absent: `ssn` must NOT be published.
        assert!(
            Publication::parse("publication p\ncustomers: id, ssn#hash\n").is_err(),
            "if this ever parses again, check that `ssn` is not in the set before relaxing anything"
        );

        // Anti-vacuity: the same file with a real comment on its own line parses, and publishes
        // exactly the two names written.
        let p = Publication::parse("publication p\n# ssn#hash must not leave\ncustomers: id, name\n")
            .expect("a whole-line comment was refused");
        assert_eq!(p.columns_of("customers"), Some(vec!["id", "name"]));
    }

    /// Every name the declaration accepts is one this database could have created. Anything else is a
    /// refusal rather than a trim, in both the table and the column position.
    #[test]
    fn a_name_that_is_not_an_identifier_is_refused() {
        for decl in [
            "publication p\nt: id, qty-2\n",
            "publication p\nt: id, \"qty\"\n",
            "publication p\nt able: id\n",
            "publication p\nt: id, a b\n",
            "publication p\ncustomers.ssn: id\n",
        ] {
            let err = Publication::parse(decl).unwrap_err();
            assert!(
                format!("{err}").contains("not an identifier"),
                "`{decl}` was refused for the wrong reason: {err}"
            );
        }
        // Anti-vacuity: underscores and digits are identifiers and must still be accepted.
        let p = Publication::parse("publication p\naudit_log_2: id, actor_2\n").unwrap();
        assert_eq!(p.columns_of("audit_log_2"), Some(vec!["actor_2", "id"]));
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

    /// **`exclude` is a decision, and it is not the same decision as silence.**
    ///
    /// Breaking shape: any table the policy has not been told about, which stalls the feed AND pins
    /// the WAL at the stalled commit. Before this form existed the only escape was to publish the
    /// table - which is the opposite of what an operator excluding it wants.
    #[test]
    fn an_excluded_table_is_decided_against_rather_than_undecided() {
        let p = Publication::parse("publication analytics\ncustomers: id, name\nexclude audit_log\n")
            .expect("a declaration with an exclusion was refused");
        assert!(p.excludes("audit_log"), "the exclusion was not recorded");
        assert!(!p.excludes("customers"), "a published table was reported as excluded");
        assert!(!p.excludes("never_mentioned"), "an undecided table was reported as excluded");
        assert_eq!(p.excluded_tables(), vec!["audit_log"]);
        assert_eq!(p.tables(), vec!["customers"], "an excluded table is not a published one");

        // Rendering one is still a refusal - the stream drops them, but a snapshot or a dump names
        // one table explicitly and must be told it may not have it.
        let r = p
            .check(&event("audit_log", &["id"], insert(vec![Value::Integer(1)])))
            .expect_err("an excluded table was rendered");
        assert_eq!(r.reason, RefusalReason::TableExcluded);
        let msg = format!("{r}");
        assert!(msg.contains("excludes table audit_log"), "{msg}");
        assert!(msg.contains("stream drops"), "the message does not distinguish the paths: {msg}");

        // Anti-vacuity: the published table still publishes.
        p.check(&event("customers", &["id", "name"], insert(vec![
            Value::Integer(1),
            Value::Varchar("ada".into()),
        ])))
        .expect("the published table was refused");
    }

    /// A table cannot be both published and excluded: guessing which line won would silently pick a
    /// policy nobody wrote.
    #[test]
    fn a_table_that_is_both_published_and_excluded_is_refused_either_way_round() {
        for decl in [
            "publication p\nt: id\nexclude t\n",
            "publication p\nexclude t\nt: id\n",
        ] {
            let err = Publication::parse(decl).unwrap_err();
            assert!(
                format!("{err}").contains("mistake"),
                "`{decl}` was refused for the wrong reason: {err}"
            );
        }
        let err = Publication::parse("publication p\nt: id\nexclude u\nexclude u\n").unwrap_err();
        assert!(format!("{err}").contains("excluded twice"), "wrong reason: {err}");
        let err = Publication::parse("publication p\nt: id\nexclude\n").unwrap_err();
        assert!(format!("{err}").contains("no table name"), "wrong reason: {err}");
    }

    /// **Two columns with one name cannot be decided about, so the event is refused.**
    ///
    /// Breaking shape, and it is real SQL rather than a hand-built event:
    /// `CREATE TABLE t (id INTEGER, name VARCHAR(8), name VARCHAR(8))` is accepted by this database
    /// and `SELECT *` returns all three values. The mask is keyed by NAME and applied per POSITION, so
    /// publishing `name` shipped both positions - and an adversarial review followed the denied value
    /// into a SQLite destination, where `encoding/json` kept the last duplicate key and the withheld
    /// value overwrote the published one.
    ///
    /// No declaration fixes it: `t: id, name, name` is refused as a duplicate and `t: id` withholds
    /// both. A guard that cannot answer must refuse.
    #[test]
    fn two_columns_with_one_name_are_refused_rather_than_guessed() {
        let p = Publication::parse("publication analytics\npatients: id, name\n").unwrap();
        let e = event("patients", &["id", "name", "name"], insert(vec![
            Value::Integer(1),
            Value::Varchar("ada".into()),
            Value::Varchar("000-11-2222".into()),
        ]));
        let r = p.check(&e).expect_err("a table with two columns of one name was published");
        assert_eq!(r.reason, RefusalReason::AmbiguousColumns { name: "name".into() });
        assert!(format!("{r}").contains("two columns named name"), "{r}");

        // The dump path is the same decision, because it goes through the same mask.
        let cols = vec!["id".to_string(), "name".to_string(), "name".to_string()];
        assert_eq!(
            p.mask_for("patients", &cols).expect_err("the dump published it").reason,
            RefusalReason::AmbiguousColumns { name: "name".into() }
        );

        // Anti-vacuity: distinct names are fine, and an unrestricted feed is unaffected - it has no
        // name-keyed decision to make, and refusing there would break a feed that was working.
        p.check(&event("patients", &["id", "name"], insert(vec![
            Value::Integer(1),
            Value::Varchar("ada".into()),
        ])))
        .expect("distinct names were refused");
        let none = Publication::unrestricted();
        none.check(&e).expect("unrestricted refused a duplicate-named table");
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
        assert!(msg.contains("No row is lost"), "the message does not say the commit survives: {msg}");
        // And it must say the two things an operator needs: that a stall is not free, and what the
        // two ways out are. The first version said only "nothing is lost meanwhile", which is true
        // about rows and quietly false about the log - a stalled feed pins the WAL.
        assert!(msg.contains("WAL cannot be reclaimed"), "the message hides the cost: {msg}");
        assert!(
            msg.contains("exclude audit_log"),
            "the message does not offer the remedy that does NOT publish the table: {msg}"
        );
        // And it converts into the error class a caller propagates.
        let e: FerroError = r.into();
        assert!(matches!(e, FerroError::Publication(_)), "{e:?}");
        assert!(format!("{e}").contains("publication refused"), "{e}");
    }
}
