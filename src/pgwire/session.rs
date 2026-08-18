//! Session parameters (`SET`/`SHOW`/`RESET`) and the handful of statements a driver runs on its
//! own behalf rather than the application's.
//!
//! ## Why this is not SQL
//!
//! `SET`, `SHOW` and `RESET` never reach the ferrodb parser. They are not statements about data;
//! they are statements about the connection, and this server's connection state lives here rather
//! than in the engine. Keeping them out of `src/parser` also keeps them out of the WAL, the
//! planner and the branch runtime, none of which have any business knowing that a client asked for
//! a different `DateStyle`.
//!
//! ## The allowlist, and what is deliberately absent
//!
//! A real driver runs a small, fixed set of statements of its own: asyncpg's connection pool
//! releases a connection by sending `SELECT pg_advisory_unlock_all(); CLOSE ALL; UNLISTEN *;
//! RESET ALL;` as one simple query, and clients probe `version()` and `current_setting()`. Those
//! are answered here, by name.
//!
//! Everything else that looks like `pg_catalog` is **refused**, not faked. There is no `pg_type`,
//! no `pg_class` and no `pg_attribute` in this server, and answering an introspection query with
//! plausible-looking rows would be worse than refusing: a driver that builds a type cache from
//! invented rows misdecodes every value afterwards, with no error to trace it back to. A refusal
//! is one error message at the point of the mistake.

use std::collections::BTreeMap;

use crate::pgwire::message::Field;
use crate::pgwire::types::oid;

/// A statement about the connection rather than about the data.
#[derive(Clone, Debug, PartialEq)]
pub enum SessionCommand {
    Set { name: String, value: String },
    Reset { name: Option<String> },
    Show { name: String },
    ShowAll,
    /// `CLOSE ALL` — every open portal.
    CloseAll,
    /// `DEALLOCATE ALL` / `DEALLOCATE name` — prepared statements.
    Deallocate { name: Option<String> },
    /// `DISCARD ALL` — portals, statements and parameters at once.
    DiscardAll,
    /// `UNLISTEN *`. There is no LISTEN in this server, so having no listeners to remove is not a
    /// pretence: the post-condition the client wants already holds.
    Unlisten,
    /// A one-column, one-row answer this server knows by name.
    Probe { column: &'static str, value: ProbeValue },
}

#[derive(Clone, Debug, PartialEq)]
pub enum ProbeValue {
    Literal(String),
    /// `current_setting('x')` — resolved against the live session parameters, not at parse time.
    Setting(String),
    /// `pg_advisory_unlock_all()` returns `void`, which renders as the empty string.
    Void,
}

/// The session parameters of one connection.
pub struct SessionParams {
    values: BTreeMap<String, String>,
}

/// `(canonical name, default)`. The canonical spelling is what a client sees in `ParameterStatus`
/// and `SHOW`; lookups are case-insensitive, as they are in Postgres.
const DEFAULTS: &[(&str, &str)] = &[
    ("application_name", ""),
    ("client_encoding", "UTF8"),
    ("DateStyle", "ISO, MDY"),
    ("default_transaction_isolation", "read committed"),
    ("default_transaction_read_only", "off"),
    ("extra_float_digits", "1"),
    ("integer_datetimes", "on"),
    ("IntervalStyle", "postgres"),
    ("is_superuser", "on"),
    ("search_path", "\"$user\", public"),
    ("server_encoding", "UTF8"),
    ("session_authorization", "ferro"),
    // True, and not a guess: `Scanner::string` copies a string literal's bytes through unchanged,
    // so a backslash in one is a backslash. Reporting `off` would tell a client to double them.
    ("standard_conforming_strings", "on"),
    ("TimeZone", "UTC"),
    ("transaction_isolation", "read committed"),
    ("transaction_read_only", "off"),
    ("bytea_output", "hex"),
    ("statement_timeout", "0"),
    ("lock_timeout", "0"),
    ("idle_in_transaction_session_timeout", "0"),
];

/// The parameters sent unprompted at startup, in `ParameterStatus` messages. This is the set real
/// Postgres reports, minus the ones that describe machinery this server does not have.
const REPORTED: &[&str] = &[
    "application_name",
    "client_encoding",
    "DateStyle",
    "integer_datetimes",
    "IntervalStyle",
    "is_superuser",
    "server_encoding",
    "server_version",
    "session_authorization",
    "standard_conforming_strings",
    "TimeZone",
];

impl SessionParams {
    pub fn new() -> Self {
        let mut values = BTreeMap::new();
        for (k, v) in DEFAULTS {
            values.insert(k.to_ascii_lowercase(), v.to_string());
        }
        values.insert("server_version".into(), super::SERVER_VERSION.into());
        // Postgres reports this as a five-digit integer; drivers that parse it prefer it to the
        // text form. 9.6.0 is 90600 under the pre-10 scheme.
        values.insert("server_version_num".into(), "90600".into());
        SessionParams { values }
    }

    /// Apply the key/value pairs from the startup packet.
    ///
    /// A client is entitled to set parameters there instead of with `SET`, and asyncpg does:
    /// `client_encoding` arrives this way on every connection. Ignoring them would make `SHOW
    /// application_name` answer with a default the client never chose.
    pub fn apply_startup(&mut self, key: &str, value: &str) {
        match key.to_ascii_lowercase().as_str() {
            // Not parameters: they name who is connecting.
            "user" => {
                self.values.insert("session_authorization".into(), value.to_string());
            }
            "database" | "replication" | "options" => {}
            other => {
                // Quoted the way asyncpg sends it: `client_encoding` arrives as `'utf-8'`, quotes
                // included. Stripping them here keeps `SHOW client_encoding` from answering with
                // a quoted string that no other client would produce.
                let v = value.trim_matches('\'');
                if self.values.contains_key(other) {
                    let normalised = if other == "client_encoding" {
                        normalise_encoding(v).unwrap_or_else(|| v.to_string())
                    } else {
                        v.to_string()
                    };
                    self.values.insert(other.to_string(), normalised);
                }
            }
        }
    }

    pub fn get(&self, name: &str) -> Option<&str> {
        self.values.get(&name.to_ascii_lowercase()).map(|s| s.as_str())
    }

    pub fn set(&mut self, name: &str, value: &str) -> Result<(), (&'static str, String)> {
        let key = name.to_ascii_lowercase();
        let value = value.trim_matches('\'').to_string();
        match key.as_str() {
            "client_encoding" => {
                let enc = normalise_encoding(&value).ok_or((
                    "0A000",
                    format!(
                        "this server encodes everything as UTF8 and cannot convert to `{value}`; \
                         a client that is told otherwise would decode every string wrongly"
                    ),
                ))?;
                self.values.insert(key, enc);
                return Ok(());
            }
            // Accepted only where the value states what is actually true. A timeout this server
            // does not enforce, accepted silently, is a promise to abort a statement that will run
            // to completion — the client would wait for a cancellation that never comes.
            "statement_timeout" | "lock_timeout" | "idle_in_transaction_session_timeout" => {
                let off = value.trim().eq_ignore_ascii_case("0")
                    || value.trim().eq_ignore_ascii_case("0ms")
                    || value.trim().is_empty();
                if !off {
                    return Err((
                        "0A000",
                        format!(
                            "this server does not implement `{name}`; setting it to `{value}` \
                             would promise a cancellation that never arrives. Only 0 (disabled) \
                             is accepted."
                        ),
                    ));
                }
            }
            "transaction_read_only" | "default_transaction_read_only" => {
                if !value.trim().eq_ignore_ascii_case("off")
                    && !value.trim().eq_ignore_ascii_case("false")
                {
                    return Err((
                        "0A000",
                        format!(
                            "this server has no read-only transaction mode, so `{name} = {value}` \
                             would not be enforced"
                        ),
                    ));
                }
            }
            _ => {}
        }
        if self.values.contains_key(&key) {
            self.values.insert(key, value);
            return Ok(());
        }
        // A name with a dot is a customised option — Postgres allows any of those, because an
        // extension owns the namespace. Anything else is a name this server does not know, and
        // saying so beats storing a setting that will never be read.
        if key.contains('.') {
            self.values.insert(key, value);
            return Ok(());
        }
        Err(("42704", format!("unrecognized configuration parameter \"{name}\"")))
    }

    pub fn reset(&mut self, name: &str) -> Result<(), (&'static str, String)> {
        let key = name.to_ascii_lowercase();
        if let Some((_, default)) = DEFAULTS.iter().find(|(k, _)| k.to_ascii_lowercase() == key) {
            self.values.insert(key, default.to_string());
            return Ok(());
        }
        if self.values.contains_key(&key) || key.contains('.') {
            return Ok(()); // server_version and friends have no other value to go back to
        }
        Err(("42704", format!("unrecognized configuration parameter \"{name}\"")))
    }

    pub fn reset_all(&mut self) {
        for (k, v) in DEFAULTS {
            self.values.insert(k.to_ascii_lowercase(), v.to_string());
        }
        self.values.retain(|k, _| {
            DEFAULTS.iter().any(|(d, _)| d.to_ascii_lowercase() == *k)
                || k == "server_version"
                || k == "server_version_num"
        });
    }

    /// The `ParameterStatus` messages a client is sent before its first `ReadyForQuery`.
    pub fn startup_status(&self) -> Vec<(&'static str, String)> {
        REPORTED
            .iter()
            .map(|name| (*name, self.get(name).unwrap_or_default().to_string()))
            .collect()
    }

    /// Every parameter, in `SHOW ALL` order.
    pub fn all(&self) -> Vec<(String, String)> {
        let mut out: Vec<(String, String)> = self
            .values
            .iter()
            .map(|(k, v)| (canonical(k).to_string(), v.clone()))
            .collect();
        out.sort_by(|a, b| a.0.to_ascii_lowercase().cmp(&b.0.to_ascii_lowercase()));
        out
    }
}

/// The spelling a client should see for a parameter it asked about in any case.
fn canonical(lower: &str) -> String {
    DEFAULTS
        .iter()
        .find(|(k, _)| k.to_ascii_lowercase() == lower)
        .map(|(k, _)| k.to_string())
        .unwrap_or_else(|| lower.to_string())
}

fn normalise_encoding(v: &str) -> Option<String> {
    let cleaned: String = v.chars().filter(|c| c.is_ascii_alphanumeric()).collect();
    match cleaned.to_ascii_uppercase().as_str() {
        "UTF8" | "UTF-8" | "UTF8MB4" | "UNICODE" => Some("UTF8".into()),
        _ => None,
    }
}

impl Default for SessionParams {
    fn default() -> Self {
        Self::new()
    }
}

/// The columns a session command answers with, and its `CommandComplete` tag.
pub fn describe(cmd: &SessionCommand) -> (Option<Vec<Field>>, String) {
    match cmd {
        SessionCommand::Set { .. } => (None, "SET".into()),
        SessionCommand::Reset { .. } => (None, "RESET".into()),
        SessionCommand::Show { name } => {
            (Some(vec![Field::text(canonical(&name.to_ascii_lowercase()), oid::TEXT)]), "SHOW".into())
        }
        SessionCommand::ShowAll => (
            Some(vec![
                Field::text("name", oid::TEXT),
                Field::text("setting", oid::TEXT),
                Field::text("description", oid::TEXT),
            ]),
            "SHOW".into(),
        ),
        SessionCommand::CloseAll => (None, "CLOSE CURSOR ALL".into()),
        SessionCommand::Deallocate { .. } => (None, "DEALLOCATE".into()),
        SessionCommand::DiscardAll => (None, "DISCARD ALL".into()),
        SessionCommand::Unlisten => (None, "UNLISTEN".into()),
        SessionCommand::Probe { column, .. } => {
            (Some(vec![Field::text(*column, oid::TEXT)]), "SELECT 1".into())
        }
    }
}

/// Recognise a session command, or decide this is ordinary SQL.
///
/// `None` means "not one of mine" and the statement goes to the ferrodb parser. `Some(Err(..))`
/// means it *is* one of mine and it is malformed, which must not fall through to the parser: a
/// mangled `SET` reaching the SQL parser produces "expected a statement", which sends the reader
/// looking in the wrong place entirely.
pub fn parse(sql: &str) -> Option<Result<SessionCommand, (&'static str, String)>> {
    let words = tokenise(sql);
    let w: Vec<&str> = words.iter().map(|s| s.as_str()).collect();
    if w.is_empty() {
        return None;
    }
    let head = w[0].to_ascii_uppercase();
    match head.as_str() {
        "SET" => Some(parse_set(&w[1..])),
        "RESET" => Some(match w.get(1) {
            None => Err(("42601", "RESET needs a parameter name, or ALL".into())),
            Some(n) if n.eq_ignore_ascii_case("all") => Ok(SessionCommand::Reset { name: None }),
            Some(n) => Ok(SessionCommand::Reset { name: Some(join_name(&w[1..])) }).map(|c| {
                let _ = n;
                c
            }),
        }),
        "SHOW" => Some(match w.get(1) {
            None => Err(("42601", "SHOW needs a parameter name, or ALL".into())),
            Some(n) if n.eq_ignore_ascii_case("all") => Ok(SessionCommand::ShowAll),
            Some(_) => Ok(SessionCommand::Show { name: join_name(&w[1..]) }),
        }),
        "CLOSE" if w.get(1).is_some_and(|s| s.eq_ignore_ascii_case("all")) => {
            Some(Ok(SessionCommand::CloseAll))
        }
        "DEALLOCATE" => Some(match w.get(1) {
            None => Err(("42601", "DEALLOCATE needs a statement name, or ALL".into())),
            Some(n) if n.eq_ignore_ascii_case("all") => {
                Ok(SessionCommand::Deallocate { name: None })
            }
            Some(n) => Ok(SessionCommand::Deallocate { name: Some((*n).to_string()) }),
        }),
        "DISCARD" if w.get(1).is_some_and(|s| s.eq_ignore_ascii_case("all")) => {
            Some(Ok(SessionCommand::DiscardAll))
        }
        "UNLISTEN" => Some(Ok(SessionCommand::Unlisten)),
        "SELECT" => parse_probe(&w[1..]).map(Ok),
        _ => None,
    }
}

fn parse_set(w: &[&str]) -> Result<SessionCommand, (&'static str, String)> {
    // `SET SESSION x = v` and `SET LOCAL x = v` differ only in transaction scope, and this server
    // has no transaction-scoped settings to differ about, so both mean the same thing here.
    let w = match w.first() {
        Some(k) if k.eq_ignore_ascii_case("session") || k.eq_ignore_ascii_case("local") => &w[1..],
        _ => w,
    };
    if w.is_empty() {
        return Err(("42601", "SET needs a parameter name".into()));
    }
    // `SET TIME ZONE 'x'` and `SET NAMES 'x'` are the two spellings that do not name their own
    // parameter. Both are ordinary SQL and both appear in real drivers.
    if w[0].eq_ignore_ascii_case("time") && w.get(1).is_some_and(|s| s.eq_ignore_ascii_case("zone"))
    {
        let value = w.get(2).copied().unwrap_or("UTC").to_string();
        return Ok(SessionCommand::Set { name: "TimeZone".into(), value });
    }
    if w[0].eq_ignore_ascii_case("names") {
        let value = w.get(1).copied().unwrap_or("UTF8").to_string();
        return Ok(SessionCommand::Set { name: "client_encoding".into(), value });
    }
    let name = w[0].to_string();
    let rest = &w[1..];
    let value_words = match rest.first() {
        Some(op) if *op == "=" || op.eq_ignore_ascii_case("to") => &rest[1..],
        _ => {
            return Err((
                "42601",
                format!("SET {name} needs `=` or `TO` and a value"),
            ))
        }
    };
    if value_words.is_empty() {
        return Err(("42601", format!("SET {name} needs a value")));
    }
    // A list value (`SET search_path TO a, b`) is one value with commas in it, which is how
    // Postgres stores it too.
    let value = value_words.join(" ").replace(" ,", ",");
    Ok(SessionCommand::Set { name, value })
}

/// The `SELECT`s this server answers without going near the query engine.
///
/// This is an allowlist and is meant to stay one: each entry is a statement some real client sends
/// on its own initiative, and the list is short enough to read. A `SELECT` that is not on it goes
/// to the ferrodb parser like any other query.
fn parse_probe(w: &[&str]) -> Option<SessionCommand> {
    let joined = w.join(" ");
    let squashed: String = joined.chars().filter(|c| !c.is_whitespace()).collect();
    let lower = squashed.to_ascii_lowercase();
    let strip = |s: &str| -> String { s.trim_start_matches("pg_catalog.").to_string() };
    let bare = strip(&lower);

    if bare == "version()" {
        return Some(SessionCommand::Probe {
            column: "version",
            value: ProbeValue::Literal(format!(
                "PostgreSQL {} on ferrodb, a hand-written engine speaking the v3 wire protocol",
                super::SERVER_VERSION
            )),
        });
    }
    if bare == "pg_advisory_unlock_all()" {
        // asyncpg's pool sends this on every release. This server takes no advisory locks, so
        // "all of them are now released" is true without doing anything.
        return Some(SessionCommand::Probe { column: "pg_advisory_unlock_all", value: ProbeValue::Void });
    }
    if bare == "current_database()" {
        return Some(SessionCommand::Probe {
            column: "current_database",
            value: ProbeValue::Literal("ferro".into()),
        });
    }
    if bare == "current_schema()" || bare == "current_schema" {
        return Some(SessionCommand::Probe {
            column: "current_schema",
            value: ProbeValue::Literal("public".into()),
        });
    }
    if bare == "current_user" || bare == "user" || bare == "session_user" {
        return Some(SessionCommand::Probe {
            column: "current_user",
            value: ProbeValue::Setting("session_authorization".into()),
        });
    }
    if let Some(rest) = bare.strip_prefix("current_setting(") {
        if let Some(arg) = rest.strip_suffix(')') {
            let name = arg.trim().trim_matches('\'').to_string();
            if !name.is_empty() {
                return Some(SessionCommand::Probe {
                    column: "current_setting",
                    value: ProbeValue::Setting(name),
                });
            }
        }
    }
    // `SELECT 1` — the oldest connection-liveness check there is, and one no pool can be talked
    // out of. A bare literal has no FROM clause, so the query engine cannot answer it at all.
    if w.len() == 1 {
        let t = w[0];
        if !t.is_empty() && t.chars().all(|c| c.is_ascii_digit()) {
            return Some(SessionCommand::Probe {
                column: "?column?",
                value: ProbeValue::Literal(t.to_string()),
            });
        }
        if t.len() >= 2 && t.starts_with('\'') && t.ends_with('\'') {
            return Some(SessionCommand::Probe {
                column: "?column?",
                value: ProbeValue::Literal(t.trim_matches('\'').to_string()),
            });
        }
    }
    None
}

/// A hint for a statement that failed and mentions something only `pg_catalog` would have.
///
/// The engine's own error already says the table does not exist. What it cannot say is *why* one
/// would never exist here, which is the sentence a driver author actually needs.
pub fn pg_catalog_hint(sql: &str) -> Option<&'static str> {
    let lower = sql.to_ascii_lowercase();
    if lower.contains("pg_catalog.") || lower.contains("pg_type") || lower.contains("pg_class")
        || lower.contains("pg_attribute") || lower.contains("pg_namespace")
    {
        return Some(
            " — this server implements no pg_catalog tables; the only catalog-ish statements it \
             answers are SHOW, SET, RESET, version(), current_setting(), current_database(), \
             current_schema() and current_user",
        );
    }
    None
}

fn join_name(w: &[&str]) -> String {
    // `SHOW transaction isolation level` is three words for one parameter, which Postgres spells
    // with underscores everywhere else.
    if w.len() > 1 {
        return w.join("_");
    }
    w.first().copied().unwrap_or_default().to_string()
}

/// Split a statement into words, keeping quoted strings whole and `=` and `,` separate.
///
/// Only ever used on the statements above; it is not a SQL tokeniser and does not pretend to be.
fn tokenise(sql: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut cur = String::new();
    let mut in_string = false;
    for c in sql.chars() {
        if in_string {
            cur.push(c);
            if c == '\'' {
                in_string = false;
                out.push(std::mem::take(&mut cur));
            }
            continue;
        }
        match c {
            '\'' => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
                cur.push(c);
                in_string = true;
            }
            ';' => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            '=' | ',' => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
                out.push(c.to_string());
            }
            c if c.is_whitespace() => {
                if !cur.is_empty() {
                    out.push(std::mem::take(&mut cur));
                }
            }
            c => cur.push(c),
        }
    }
    if !cur.is_empty() {
        out.push(cur);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_show_round_trip_a_parameter() {
        let mut p = SessionParams::new();
        assert_eq!(p.get("DateStyle"), Some("ISO, MDY"));
        p.set("datestyle", "ISO, DMY").unwrap();
        assert_eq!(p.get("DATESTYLE"), Some("ISO, DMY"), "parameter names are case-insensitive");
        p.reset("DateStyle").unwrap();
        assert_eq!(p.get("DateStyle"), Some("ISO, MDY"));
    }

    #[test]
    fn reset_all_puts_every_parameter_back() {
        let mut p = SessionParams::new();
        p.set("application_name", "x").unwrap();
        p.set("custom.thing", "y").unwrap();
        p.reset_all();
        assert_eq!(p.get("application_name"), Some(""));
        assert_eq!(p.get("custom.thing"), None, "a customised option is dropped, not defaulted");
        assert_eq!(
            p.get("server_version"),
            Some(super::super::SERVER_VERSION),
            "the server version is not a setting anyone can reset away"
        );
    }

    /// Breaking shape: `SET client_encoding TO 'LATIN1'`. Accepting it silently tells the client
    /// its strings will be transcoded; every string it then reads is decoded with the wrong table
    /// and there is no error anywhere.
    #[test]
    fn an_encoding_this_server_cannot_produce_is_refused() {
        let mut p = SessionParams::new();
        assert!(p.set("client_encoding", "LATIN1").is_err());
        assert!(p.set("client_encoding", "'utf-8'").is_ok(), "quotes and case are accepted");
        assert_eq!(p.get("client_encoding"), Some("UTF8"));
    }

    /// Same shape, different lie: a timeout that is stored and never enforced.
    #[test]
    fn a_timeout_this_server_does_not_enforce_is_refused_unless_it_is_off() {
        let mut p = SessionParams::new();
        assert!(p.set("statement_timeout", "5000").is_err());
        assert!(p.set("statement_timeout", "0").is_ok());
        assert!(p.set("lock_timeout", "0ms").is_ok());
        assert!(p.set("transaction_read_only", "on").is_err());
        assert!(p.set("transaction_read_only", "off").is_ok());
    }

    #[test]
    fn an_unknown_parameter_is_refused_but_a_namespaced_one_is_kept() {
        let mut p = SessionParams::new();
        let (code, _msg) = p.set("no_such_parameter", "1").unwrap_err();
        assert_eq!(code, "42704");
        assert!(p.set("myext.option", "1").is_ok());
        assert_eq!(p.get("myext.option"), Some("1"));
    }

    #[test]
    fn the_startup_packet_can_set_parameters_too() {
        let mut p = SessionParams::new();
        p.apply_startup("user", "alice");
        p.apply_startup("application_name", "pytest");
        p.apply_startup("client_encoding", "'utf-8'");
        p.apply_startup("database", "ferro");
        assert_eq!(p.get("session_authorization"), Some("alice"));
        assert_eq!(p.get("application_name"), Some("pytest"));
        assert_eq!(p.get("client_encoding"), Some("UTF8"));
    }

    #[test]
    fn the_reported_parameters_all_have_values() {
        let p = SessionParams::new();
        for (name, value) in p.startup_status() {
            assert!(
                !value.is_empty() || name == "application_name",
                "{name} is reported at startup with no value"
            );
        }
        assert!(p.startup_status().iter().any(|(n, _)| *n == "server_version"));
    }

    #[test]
    fn set_parses_the_spellings_drivers_actually_send() {
        let cases = [
            ("SET client_encoding TO 'UTF8'", "client_encoding", "UTF8"),
            ("set DateStyle = 'ISO'", "DateStyle", "ISO"),
            ("SET SESSION extra_float_digits = 3", "extra_float_digits", "3"),
            ("SET LOCAL application_name TO 'x'", "application_name", "x"),
            ("SET TIME ZONE 'UTC'", "TimeZone", "UTC"),
            ("SET NAMES 'UTF8'", "client_encoding", "UTF8"),
        ];
        for (sql, name, value) in cases {
            match parse(sql) {
                Some(Ok(SessionCommand::Set { name: n, value: v })) => {
                    assert_eq!(n.to_ascii_lowercase(), name.to_ascii_lowercase(), "{sql}");
                    assert_eq!(v.trim_matches('\''), value, "{sql}");
                }
                other => panic!("{sql} parsed as {other:?}"),
            }
        }
    }

    #[test]
    fn a_multi_word_value_survives() {
        match parse("SET search_path TO \"$user\", public") {
            Some(Ok(SessionCommand::Set { value, .. })) => {
                assert!(value.contains("public"), "{value}");
                assert!(value.contains("$user"), "{value}");
            }
            other => panic!("{other:?}"),
        }
    }

    /// A malformed SET must not fall through to the SQL parser, which would report "expected a
    /// statement" and point the reader at the wrong layer.
    #[test]
    fn a_malformed_set_is_refused_here_and_not_passed_on() {
        assert!(matches!(parse("SET"), Some(Err(_))));
        assert!(matches!(parse("SET x"), Some(Err(_))));
        assert!(matches!(parse("SET x ="), Some(Err(_))));
        assert!(matches!(parse("SHOW"), Some(Err(_))));
    }

    #[test]
    fn ordinary_sql_is_not_a_session_command() {
        for sql in [
            "SELECT * FROM t",
            "INSERT INTO t VALUES (1)",
            "UPDATE t SET a = 1",
            "CREATE TABLE t (a INTEGER)",
            "BEGIN AGENT SESSION AS 'x' RUN 'r'",
        ] {
            assert!(parse(sql).is_none(), "{sql} was taken for a session command");
        }
    }

    /// `UPDATE t SET a = 1` starts with UPDATE, but a naive "does it contain SET" check would
    /// claim it. This is the shape that would break every UPDATE on the server.
    #[test]
    fn an_update_with_a_set_clause_is_not_a_session_set() {
        assert!(parse("UPDATE inventory SET qty = qty - 1 WHERE id = 1").is_none());
    }

    #[test]
    fn the_probes_a_real_driver_sends_are_recognised() {
        let sqls = [
            "SELECT pg_advisory_unlock_all()",
            "SELECT version()",
            "SELECT pg_catalog.version()",
            "SELECT current_setting('search_path')",
            "SELECT current_database()",
            "SELECT current_schema()",
            "SELECT current_user",
            "SELECT 1",
        ];
        for sql in sqls {
            assert!(
                matches!(parse(sql), Some(Ok(SessionCommand::Probe { .. }))),
                "{sql} was not recognised"
            );
        }
        assert!(matches!(parse("CLOSE ALL"), Some(Ok(SessionCommand::CloseAll))));
        assert!(matches!(parse("UNLISTEN *"), Some(Ok(SessionCommand::Unlisten))));
        assert!(matches!(parse("DEALLOCATE ALL"), Some(Ok(SessionCommand::Deallocate { name: None }))));
        assert!(matches!(parse("DISCARD ALL"), Some(Ok(SessionCommand::DiscardAll))));
    }

    #[test]
    fn a_select_against_a_real_table_is_not_a_probe() {
        assert!(parse("SELECT version FROM releases").is_none());
        assert!(parse("SELECT 1 FROM t").is_none());
    }

    #[test]
    fn pg_catalog_queries_get_an_explanation_rather_than_invented_rows() {
        assert!(pg_catalog_hint("SELECT typname FROM pg_catalog.pg_type").is_some());
        assert!(pg_catalog_hint("SELECT * FROM inventory").is_none());
    }
}
