//! E79b — `BEGIN AGENT SESSION ... PROMPT '<text>'`, and what reaches `RunEntity::prompt_hash`.
//!
//! # The gap this closes
//!
//! `prompt_digest` has been correct since B5 and `RunEntity::prompt_hash` has been documented as
//! "hash of the prompt that produced the run" for as long as it has existed. Nothing was ever
//! hashed into it over SQL: `begin_session_with_model` had no prompt parameter and
//! `BEGIN AGENT SESSION` had no clause carrying one, so **every** run the SQL surface interned
//! passed `[0u8; 32]`. That is not a weak hash, it is no hash — and it made every run's prompt
//! identity identical, so `same_actor` could not tell two prompts apart while the column looked
//! populated.
//!
//! # The two values that must not collapse into each other
//!
//! `[0u8; 32]` means **no prompt was declared**. `prompt_digest("")` — `e3b0c442…` — means **the
//! declared prompt was empty**. They are different facts about a run and a reader of `ferro_runs`
//! has to be able to tell them apart, so the clause stays optional, an omitted clause stays
//! all-zero, and `PROMPT ''` is hashed like any other string rather than special-cased back to the
//! placeholder. Most of the file is about that pair.
//!
//! # And the reason the field is a hash at all
//!
//! `sha256.rs` states the privacy boundary: a prompt containing customer data must not become a
//! durable copy of it. `the_prompt_text_never_reaches_the_durable_provenance_file` is the test that
//! holds the boundary rather than restating it — it drives a canary phrase through the SQL surface
//! into a real `DurableProvenanceStore` and then reads the bytes on disk.

use std::fs::OpenOptions;
use std::sync::Arc;

use ferrodb::agent_sql::runtime::AgentRuntime;
use ferrodb::branch::types::BranchId;
use ferrodb::error::FerroError;
use ferrodb::execution::executor::{run, Outcome};
use ferrodb::execution::session::Session;
use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::provenance::sha256::prompt_digest;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

struct Db {
    catalog: Catalog,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    runtime: Arc<AgentRuntime>,
    _dir: tempfile::TempDir,
}

impl Db {
    fn new() -> Self {
        Db::build(|rt| rt)
    }

    fn build(f: impl FnOnce(AgentRuntime) -> AgentRuntime) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(dir.path().join("prompt.db"))
            .unwrap();
        let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
        let catalog = Catalog::create(bp.clone()).unwrap();
        let wal = Arc::new(WalManager::new(dir.path().join("prompt.wal")).unwrap());
        let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
        bp.attach_wal(wal);
        let runtime = Arc::new(f(AgentRuntime::new()));
        Db { catalog, bp, txn, runtime, _dir: dir }
    }

    fn session(&self) -> Session {
        Session::with_runtime(self.runtime.clone())
    }

    fn exec(&mut self, sql: &str, s: &mut Session) -> Result<Outcome, FerroError> {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens()?;
        let mut parser = Parser::new(tokens);
        let mut stmts = parser.parse();
        if !parser.errors.is_empty() {
            return Err(FerroError::SqlParseError(
                parser.errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "),
            ));
        }
        assert_eq!(stmts.len(), 1, "expected one statement: {sql}");
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), s)
    }

    fn ok(&mut self, sql: &str, s: &mut Session) -> Outcome {
        self.exec(sql, s).unwrap_or_else(|e| panic!("`{sql}` failed: {e}"))
    }

    /// The hash the SQL statement actually interned, read back the way `ferro_runs` reads it.
    fn hash_after(&mut self, sql: &str) -> [u8; 32] {
        let mut s = self.session();
        self.ok(sql, &mut s);
        let branch = s.agent.as_ref().expect("no agent session opened").branch;
        self.runtime.run_of(branch).expect("no run interned for the branch").prompt_hash
    }
}

const ZERO: [u8; 32] = [0u8; 32];

// ---------------------------------------------------------------------------------------------
// The rule: a prompt declared in SQL reaches provenance as its digest.
// ---------------------------------------------------------------------------------------------

/// **Breaking shape:** assert only that the hash is non-zero and a wrong-but-constant value passes
/// — the same mistake the all-zero placeholder was, one step along. So the value is asserted
/// against `prompt_digest` of the exact text, computed here from the standard's function and not by
/// asking the subject what it produced.
#[test]
fn a_prompt_declared_in_sql_becomes_the_runs_digest() {
    let mut db = Db::new();
    let prompt = "top up everything below reorder";
    let got = db.hash_after(&format!(
        "BEGIN AGENT SESSION AS 'restock' RUN 'r_7' MODEL 'claude-opus-5/2026-05' PROMPT '{prompt}';"
    ));
    assert_ne!(got, ZERO, "the prompt hash is still the all-zero placeholder over the SQL path");
    assert_eq!(got, prompt_digest(prompt), "the interned hash is not this prompt's digest");
}

/// **The rule the clause's optionality rests on: absent is not empty.**
///
/// Two runs, one with no clause and one with `PROMPT ''`. A reader of `ferro_runs` must be able to
/// tell "nobody declared a prompt" from "the prompt was the empty string", and the only thing that
/// distinguishes them is that the first is `[0u8; 32]` and the second is `e3b0c442…`. Collapsing
/// them — defaulting the absent clause to `Some("")`, or special-casing an empty prompt back to the
/// placeholder — is invisible in any test that looks at one of the two alone.
#[test]
fn an_omitted_clause_is_all_zero_and_an_empty_prompt_is_not() {
    let mut db = Db::new();

    let absent = db.hash_after("BEGIN AGENT SESSION AS 'a' RUN 'r_absent';");
    assert_eq!(absent, ZERO, "omitting PROMPT must leave the hash unset, not hash something");

    let empty = db.hash_after("BEGIN AGENT SESSION AS 'b' RUN 'r_empty' PROMPT '';");
    assert_eq!(empty, prompt_digest(""), "PROMPT '' must hash the empty string");
    assert_ne!(empty, ZERO, "an empty prompt was special-cased back to the placeholder");
    assert_ne!(absent, empty, "'no prompt' and 'the empty prompt' collapsed into one value");
}

/// The field carries information, not merely a non-zero constant: two prompts that differ produce
/// two hashes, which is what makes `same_actor` able to tell two runs apart at all.
#[test]
fn two_prompts_that_differ_produce_two_hashes() {
    let mut db = Db::new();
    let a = db.hash_after("BEGIN AGENT SESSION AS 'a' RUN 'r1' PROMPT 'refund account 4471';");
    let b = db.hash_after("BEGIN AGENT SESSION AS 'a' RUN 'r2' PROMPT 'refund account 4472';");
    assert_ne!(a, b, "one digest for two prompts");
    // Including a difference that only whitespace makes: the prompt is hashed as typed, so
    // normalising it here would merge two actors into one interned slot.
    let padded = db.hash_after("BEGIN AGENT SESSION AS 'a' RUN 'r3' PROMPT ' refund account 4471';");
    assert_ne!(a, padded, "the prompt was trimmed before hashing");
}

/// **The swap detector.** `RUN` and `PROMPT` both take a quoted string and sit in one clause list;
/// a transposition between them is accepted by every type in the path and has no symptom except
/// that every row of the run is attributed to a hash of the run id. This is the test that would
/// have caught it, and the reason `begin_session_as` takes a named [`RunIdentity`] rather than two
/// more positional `Option<&str>` parameters.
#[test]
fn the_run_id_and_the_prompt_are_not_transposed() {
    let mut db = Db::new();
    let mut s = db.session();
    db.ok("BEGIN AGENT SESSION AS 'a' RUN 'r_id' PROMPT 'p_text';", &mut s);
    let branch = s.agent.as_ref().unwrap().branch;
    let entity = db.runtime.run_of(branch).unwrap();
    assert_eq!(entity.run_id, "r_id", "the run id is not the run id");
    assert_eq!(entity.prompt_hash, prompt_digest("p_text"));
    assert_ne!(entity.prompt_hash, prompt_digest("r_id"), "the run id was hashed as the prompt");
}

/// Re-beginning one run under a different prompt is refused rather than silently reusing the first
/// prompt's slot.
///
/// `RunEntity::same_actor` counts `prompt_hash`, and until this row that clause could never fire —
/// every entity carried the same all-zero hash, so the check was structurally dead. Attribution is
/// run-level, so the store returns the SAME `ProvId` for a repeat; what it must not do is answer
/// "same run" for a run whose prompt has changed, because every row already stamped with that slot
/// would then be attributed to a prompt that did not produce it.
#[test]
fn resuming_a_run_under_a_different_prompt_is_refused_and_under_the_same_one_is_not() {
    let mut db = Db::new();

    let mut first = db.session();
    db.ok("BEGIN AGENT SESSION AS 'resumer' RUN 'r_same' PROMPT 'restock';", &mut first);
    let prov = first.agent.as_ref().unwrap().prov;

    // Same actor, same prompt: a lookup, and the same slot.
    let mut again = db.session();
    db.ok("BEGIN AGENT SESSION AS 'resumer' RUN 'r_same' PROMPT 'restock';", &mut again);
    assert_eq!(again.agent.as_ref().unwrap().prov, prov, "a repeat must reuse the run's slot");

    // Same actor, different prompt: refused, and the message says what disagreed.
    let mut third = db.session();
    let err = match db.exec("BEGIN AGENT SESSION AS 'resumer' RUN 'r_same' PROMPT 'liquidate';", &mut third) {
        Err(e) => e,
        Ok(_) => panic!("a run resumed under a different prompt must be refused"),
    };
    assert!(
        err.to_string().contains("different actor tuple"),
        "refused for the wrong reason: {err}"
    );

    // And declaring a prompt where the first session declared none is the same disagreement.
    db.ok("BEGIN AGENT SESSION AS 'other' RUN 'r_none';", &mut db.session());
    let mut fifth = db.session();
    assert!(
        db.exec("BEGIN AGENT SESSION AS 'other' RUN 'r_none' PROMPT 'x';", &mut fifth).is_err(),
        "adding a prompt to a run interned without one is a different actor"
    );
}

// ---------------------------------------------------------------------------------------------
// The privacy boundary the digest exists for.
// ---------------------------------------------------------------------------------------------

/// **The claim `sha256.rs` makes, held rather than restated: the prompt itself is never written.**
///
/// A canary phrase goes through the ordinary SQL surface into a `DurableProvenanceStore` — the
/// store a real database runs, which appends a `Run` record on intern — and then the file's bytes
/// are read.
///
/// **Anti-vacuity, and it is the half that makes this a detector rather than a tautology:** absence
/// of the canary would also be satisfied by a file that received nothing at all. So the 32 digest
/// bytes must be *present* in the same file. Storing the prompt as text fails the first assertion;
/// interning nothing, or interning it somewhere else, fails the second.
#[test]
fn the_prompt_text_never_reaches_the_durable_provenance_file() {
    const CANARY: &str = "refund the customer at 4471 Elm Street, card ending 9021";

    let dir = tempfile::tempdir().unwrap();
    let prov_path = dir.path().join("prov.log");
    let mut db = Db::build(|rt| rt.with_durable_provenance(&prov_path).unwrap());

    let got = db.hash_after(&format!("BEGIN AGENT SESSION AS 'refunder' RUN 'r_9' PROMPT '{CANARY}';"));
    assert_eq!(got, prompt_digest(CANARY));

    let bytes = std::fs::read(&prov_path).expect("the durable provenance file was never written");
    assert!(
        !contains(&bytes, CANARY.as_bytes()),
        "the prompt was written to {} in plain text",
        prov_path.display()
    );
    assert!(
        contains(&bytes, &prompt_digest(CANARY)),
        "the digest is not in {} either — this file recorded nothing, so the assertion above \
         proved nothing",
        prov_path.display()
    );
    // Nor does any fragment of it survive: a truncated or partially-escaped copy would slip past a
    // whole-phrase search.
    assert!(!contains(&bytes, b"4471 Elm Street"), "a fragment of the prompt reached the file");
    assert!(!contains(&bytes, b"card ending"), "a fragment of the prompt reached the file");
}

/// Neither does the session the client holds. `AgentSession` is what `BEGIN AGENT SESSION` returns
/// and what the connection keeps until `MERGE`; if the prompt lived there it would outlive the
/// statement inside the server for as long as the task ran.
#[test]
fn the_open_session_holds_no_prompt_text() {
    const CANARY: &str = "customer 4471 wants a refund";
    let mut db = Db::new();
    let mut s = db.session();
    let out = db.ok(&format!("BEGIN AGENT SESSION AS 'a' RUN 'r' PROMPT '{CANARY}';"), &mut s);

    let session = format!("{:?}", s.agent.as_ref().unwrap());
    assert!(!session.contains(CANARY), "the open session carries the prompt text: {session}");

    // And neither does what goes back to the client, in either of its two renderings: the sentence
    // a terminal prints, and the typed columns a driver reads off the wire.
    let agent_out = match &out {
        Outcome::Agent(a) => a,
        other => panic!("expected an agent outcome, got {other:?}", other = std::mem::discriminant(other)),
    };
    let sentence = agent_out.to_string();
    assert!(!sentence.contains(CANARY), "the rendered result carries the prompt text: {sentence}");
    let wire = format!("{:?}", agent_out.to_rows().rows);
    assert!(!wire.contains(CANARY), "the wire rows carry the prompt text: {wire}");

    // Anti-vacuity: the session really is the one that declared the canary.
    let branch = s.agent.as_ref().unwrap().branch;
    assert_eq!(db.runtime.run_of(branch).unwrap().prompt_hash, prompt_digest(CANARY));
}

// ---------------------------------------------------------------------------------------------
// The Rust API under the SQL surface, so the two cannot drift.
// ---------------------------------------------------------------------------------------------

/// `begin_session` and `begin_session_with_model` are the shapes that predate the clause. They
/// declare no prompt, so they must keep producing the all-zero hash — the single-node path that
/// 1300-odd tests already exercise must not start hashing something nobody passed.
#[test]
fn the_prompt_free_entry_points_still_intern_the_unset_hash() {
    let rt = AgentRuntime::new();
    let a = rt.begin_session("a", Some("r_a"), BranchId::TRUNK).unwrap();
    assert_eq!(rt.run_of(a.branch).unwrap().prompt_hash, ZERO);

    let b = rt
        .begin_session_with_model("b", Some("r_b"), Some(("m", "1")), BranchId::TRUNK)
        .unwrap();
    assert_eq!(rt.run_of(b.branch).unwrap().prompt_hash, ZERO);

    // And the full form with no prompt agrees with them, so the three entry points cannot disagree
    // about what "no prompt" means.
    let c = rt
        .begin_session_as(
            ferrodb::agent_sql::runtime::RunIdentity {
                agent_id: "c",
                run_id: Some("r_c"),
                model: Some(("m", "1")),
                prompt: None,
            },
            BranchId::TRUNK,
        )
        .unwrap();
    assert_eq!(rt.run_of(c.branch).unwrap().prompt_hash, ZERO);
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack.windows(needle.len()).any(|w| w == needle)
}
