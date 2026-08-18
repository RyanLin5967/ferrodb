//! B7 — the publication allowlist, end to end and against an independent consumer.
//!
//! Three questions, and the unit tests can only answer the first two on their own terms:
//!
//! 1. Does a denied column survive a **real SQL workload that writes it** — insert, update, delete,
//!    plus the schema declaration — all the way to the bytes? Asserted on the file, not on a
//!    renderer's return value.
//! 2. Is the feed still worth having? Every refusal test here has a half that shows the published
//!    columns arriving, the feed non-empty, and an independently written consumer accepting it.
//! 3. Does the **consumer's** copy of the rule work? That one cannot be answered by driving the
//!    producer, because a correct producer never emits an event for the consumer to refuse. So the
//!    feed is produced a second time with the guard deliberately not in force — which is exactly what
//!    a bypassed, older or mis-configured producer emits — and handed to the shipped Go binary with
//!    the policy attached.
//!
//! The fourth thing here is the cursor, and it is the one that cost the most: a refusal must be
//! atomic against it. `tests` in `src/replication/stream.rs` pin that for a whole-table refusal; this
//! file pins the shape those cannot reach, because it needs a catalog-backed decoder and a real
//! transaction — **one commit that writes a published row and a refused row**. Truncating the batch
//! at the offending event rather than at its commit emits the published sibling, advances the cursor
//! past the commit, and loses the refused row for ever. That is the third instance of one bug class
//! here: E74 and E75 both lost rows because a commit_lsn identifies a transaction and not a row.

//! # The fire-checks, and what each mutant turned red
//!
//! Every guard below was mutated so the property was gone, the build was confirmed to SUCCEED (a
//! mutant that does not compile prints nothing and looks exactly like a surviving one — that has
//! produced four false passes in this repo), the tests were run, and the mutation reverted. Each
//! mutant removes a property outright rather than one of two sufficient sites for it.
//!
//! | mutant | property removed | build | what went red |
//! |---|---|---|---|
//! | M1  | cursor computed over every DECODED event and refused events merely filtered — the naive refusal | 0 | 3 `stream` unit tests + this file's cursor test; pump reported `emitted: 3, cursor: 821`, past the refused row |
//! | M1b | truncate at the offending EVENT instead of at its commit boundary | 0 | this file's cursor test only — invisible to every unit test, which is why it lives here |
//! | M2  | the allowlist inside `jsonl::row_into` | 0 | 6 `jsonl` unit tests + 2 tests here, one of them via the **Go** binary: *line 3: the after image of customers carries column "ssn"* |
//! | M3  | an undecided table falls through to publishing everything | 0 | 11 lib tests + this file's cursor test |
//! | M5  | the `CREATE_TABLE` shape is no longer projected | 0 | 1 `jsonl` test + 2 here; the Go consumer named it: *CREATE_TABLE declares column "ssn"* |
//! | M6  | `write_feed` decides per event inside the write loop instead of up front | 0 | `write_feed_writes_nothing_at_all_when_any_event_is_refused` |
//! | M7  | a value with no column name no longer refuses | 0 | 2 `publication` unit tests |
//! | M4  | the Go consumer's forward check (`decodeLine`) | 0 | 5 Go tests + `the_independent_consumer_refuses_...` here |
//! | M4b | the Go consumer's narrower-producer check | 0 | `TestConsumerRefusesAShapeNarrowerThanItsOwnPolicy` |
//!
//! M1b is the one worth keeping in mind. It is the mistake a careful reader of `pump` would make —
//! truncate the batch at the refusal, which sounds exactly like what the fix is — and **no unit test
//! in this repo can see it**. Measured under the mutant rather than argued about: pump one emitted
//! `grace` (the published sibling of the refused row), the cursor landed on 635, that commit's own
//! `commit_end_lsn`, and after widening the publication the refused row was **never delivered by any
//! subsequent pump**. Under the real code the same probe delivers it: pump one emits 1 and refuses 3,
//! the cursor stays at 365, and the resume carries all three events.

use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use ferrodb::buffer::buffer_pool::BufferPoolManager;
use ferrodb::catalog::catalog::Catalog;
use ferrodb::execution::executor::run;
use ferrodb::execution::session::Session;
use ferrodb::parser::parser::Parser;
use ferrodb::parser::scanner::Scanner;
use ferrodb::replication::jsonl::write_feed;
use ferrodb::replication::logical::LogicalDecoder;
use ferrodb::replication::publication::Publication;
use ferrodb::replication::stream::FeedStreamer;
use ferrodb::storage::disk_manager::DiskManager;
use ferrodb::wal::log::WalManager;
use ferrodb::wal::txn::TxnManager;

/// The declaration both ends read. `ssn` is absent from `customers`, so it is withheld; `audit_log`
/// is named here because this feed carries its DDL too, and an undecided table is refused rather
/// than projected.
const POLICY: &str = "publication analytics\n\
                      # ssn must never leave the database\n\
                      customers: id, name\n\
                      audit_log: id, actor\n";

/// The two distinctive values the workload writes into the denied column. Distinctive so that
/// "did it leave?" is a question about these exact bytes rather than about a plausible substring.
const SSN_FIRST: &str = "000-11-2222";
const SSN_UPDATED: &str = "999-88-7777";

fn go_bin() -> String {
    for c in ["go", "/opt/homebrew/bin/go", "/usr/local/go/bin/go"] {
        if Command::new(c).arg("version").output().map(|o| o.status.success()).unwrap_or(false) {
            return c.to_string();
        }
    }
    panic!("Go is required: the point of this file is a consumer that shares no code with the producer");
}

struct Db {
    _dir: tempfile::TempDir,
    catalog: Catalog,
    wal: Arc<WalManager>,
    bp: Arc<BufferPoolManager>,
    txn: Arc<TxnManager>,
    session: Session,
}

fn db(dir: &Path, tag: &str) -> Db {
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.join(format!("{tag}.db")))
        .unwrap();
    let bp = Arc::new(BufferPoolManager::new(Arc::new(DiskManager::new(file).unwrap())));
    let catalog = Catalog::create(bp.clone()).unwrap();
    let wal = Arc::new(WalManager::new(dir.join(format!("{tag}.wal"))).unwrap());
    let txn = Arc::new(TxnManager::new(wal.clone(), bp.clone()));
    bp.attach_wal(wal.clone());
    Db { _dir: tempfile::tempdir().unwrap(), catalog, wal, bp, txn, session: Session::new() }
}

impl Db {
    fn sql(&mut self, sql: &str) {
        let tokens = Scanner::new(sql.chars().collect(), Vec::new()).scan_tokens().unwrap();
        let mut p = Parser::new(tokens);
        let mut stmts = p.parse();
        assert!(p.errors.is_empty(), "parse error in `{sql}`: {:?}", p.errors);
        run(stmts.remove(0), &mut self.catalog, self.bp.clone(), self.txn.clone(), &mut self.session)
            .unwrap_or_else(|e| panic!("`{sql}` failed: {e}"));
    }

    /// Decode the whole retained log.
    fn decode_all(&self) -> Vec<ferrodb::replication::logical::ChangeEvent> {
        use std::sync::atomic::Ordering;
        self.wal.flush().unwrap();
        let decoder = LogicalDecoder::new(&self.catalog);
        let out = decoder
            .decode(
                &self.wal,
                self.wal.base_lsn.load(Ordering::SeqCst),
                self.wal.next_lsn.load(Ordering::SeqCst),
            )
            .expect("decode");
        assert!(!out.events.is_empty(), "nothing was decoded; every assertion below would be vacuous");
        out.events
    }
}

/// A workload that really writes the denied column: declared in the schema, inserted, updated, and
/// carried in a delete's before image. Each of those is a different path through the renderer, and a
/// projection applied to only one of them passes the others' tests.
fn workload(d: &mut Db) {
    d.sql("CREATE TABLE customers (id INTEGER NOT NULL, name VARCHAR(32), ssn VARCHAR(16));");
    d.sql("CREATE TABLE audit_log (id INTEGER NOT NULL, actor VARCHAR(16));");
    d.sql(&format!("INSERT INTO customers VALUES (1, 'ada', '{SSN_FIRST}');"));
    d.sql("INSERT INTO customers VALUES (2, 'grace', '111-22-3333');");
    d.sql(&format!("UPDATE customers SET ssn = '{SSN_UPDATED}' WHERE id = 1;"));
    d.sql("DELETE FROM customers WHERE id = 2;");
    d.sql("INSERT INTO audit_log VALUES (1, 'root');");
}

fn write_policy(dir: &Path) -> PathBuf {
    let p = dir.join("publication.txt");
    std::fs::write(&p, POLICY).unwrap();
    p
}

/// Run the independent consumer. Returns (success, stdout, stderr).
fn consumer(args: &[&str]) -> (bool, String, String) {
    let out = Command::new(go_bin())
        .current_dir("cdc-consumer")
        .args(["run", "."])
        .args(args)
        .output()
        .expect("failed to run the Go consumer");
    (
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).to_string(),
        String::from_utf8_lossy(&out.stderr).to_string(),
    )
}

/// **Exit criterion 1, plus its anti-vacuity half.**
///
/// Breaking shape: a table with a denied column written by every statement shape there is. Note what
/// is asserted about the denied column — not merely that no `"ssn":` key appears, but that the three
/// characters do not appear **anywhere in the file**. A redaction marker, a null-valued key or a
/// `withheld` list would each satisfy the weaker form while shipping the name of the column the guard
/// exists to protect.
#[test]
fn a_denied_column_appears_in_zero_events_across_a_workload_that_writes_it() {
    let dir = tempfile::tempdir().unwrap();
    let mut d = db(dir.path(), "guarded");
    workload(&mut d);
    let events = d.decode_all();

    let policy = Publication::parse(POLICY).expect("the test's own policy does not parse");
    let mut buf: Vec<u8> = Vec::new();
    let lines = write_feed(&events, &policy, &mut buf).expect("the publication refused the feed");
    let feed = String::from_utf8(buf).unwrap();

    // The premise: the workload really did write the denied column. Without this the test could pass
    // against a workload that never produced one, which is exactly how two data-loss bugs survived
    // here — the generator never made the shape.
    let mut leaky: Vec<u8> = Vec::new();
    write_feed(&events, &Publication::unrestricted(), &mut leaky).unwrap();
    let unguarded = String::from_utf8(leaky).unwrap();
    for v in [SSN_FIRST, SSN_UPDATED] {
        assert!(
            unguarded.contains(v),
            "the workload never wrote {v}, so this test proves nothing about withholding it"
        );
    }
    assert!(unguarded.contains("\"ssn\""), "the unguarded feed does not name the column either");

    // The criterion.
    for v in [SSN_FIRST, SSN_UPDATED, "111-22-3333"] {
        assert!(!feed.contains(v), "a denied value left the database:\n{feed}");
    }
    assert!(!feed.contains("ssn"), "the denied column is named in the feed:\n{feed}");

    // Anti-vacuity: the feed is not simply empty, and the published columns are all there.
    assert!(lines >= 6, "only {lines} events; the workload ran seven statements");
    assert_eq!(feed.lines().count(), lines, "write_feed miscounted its own output");
    for present in ["\"name\":\"ada\"", "\"name\":\"grace\"", "\"actor\":\"root\"", "\"id\":1"] {
        assert!(feed.contains(present), "a published value is missing: {present}\n{feed}");
    }
    // Every op the workload produced is still in the feed: withholding a column must not have
    // swallowed an event.
    for op in ["CREATE_TABLE", "INSERT", "UPDATE", "DELETE"] {
        assert!(feed.contains(&format!("\"op\":\"{op}\"")), "the {op} events vanished:\n{feed}");
    }
    // The declared shape is projected too, which is what makes the absence honest: the consumer is
    // told about exactly the columns it will receive.
    let create = feed
        .lines()
        .find(|l| l.contains("CREATE_TABLE") && l.contains("customers"))
        .expect("no CREATE_TABLE for customers");
    assert!(create.contains("\"name\":\"id\"") && create.contains("\"name\":\"name\""), "{create}");
    assert!(!create.contains("ssn"), "the shape declared the denied column: {create}");

    // And an independently written consumer, holding the same policy, accepts it.
    let path = dir.path().join("guarded.jsonl");
    std::fs::write(&path, &feed).unwrap();
    let policy_file = write_policy(dir.path());
    let (ok, stdout, stderr) = consumer(&[
        "validate",
        path.to_str().unwrap(),
        "-publication",
        policy_file.to_str().unwrap(),
    ]);
    assert!(ok, "the independent consumer rejected a compliant feed:\n{stderr}\n{feed}");
    assert!(stdout.starts_with(&format!("OK {lines}")), "unexpected report: {stdout}");
}

/// **The consumer's half of the guard, forced to fire on the real binary.**
///
/// A correct producer never hands this check anything to refuse, so the feed is produced with the
/// producer's guard deliberately not in force — `Publication::unrestricted()`, which is what a
/// build predating B7, a bypassed path or a mis-configured server emits. The consumer must refuse it,
/// name the column, and exit non-zero, in the mode that only validates AND in the mode that writes to
/// a destination.
#[test]
fn the_independent_consumer_refuses_a_feed_the_producer_should_not_have_written() {
    let dir = tempfile::tempdir().unwrap();
    let mut d = db(dir.path(), "leaky");
    workload(&mut d);
    let events = d.decode_all();

    let mut buf: Vec<u8> = Vec::new();
    write_feed(&events, &Publication::unrestricted(), &mut buf).expect("write feed");
    let leaky = dir.path().join("leaky.jsonl");
    std::fs::write(&leaky, &buf).unwrap();
    let policy_file = write_policy(dir.path());
    let policy_arg = policy_file.to_str().unwrap().to_string();

    let (ok, _, stderr) =
        consumer(&["validate", leaky.to_str().unwrap(), "-publication", &policy_arg]);
    assert!(!ok, "the consumer validated a feed carrying a column the policy denies");
    assert!(stderr.contains("ssn"), "the refusal does not name the column: {stderr}");
    assert!(stderr.contains("analytics"), "the refusal does not name the publication: {stderr}");

    // The sink is the mode that writes somewhere, so the guard has to hold there too — "whichever
    // sink is attached" is the claim, and a check that only ran in `validate` would not support it.
    let out_db = dir.path().join("leaky.sqlite");
    let (ok, stdout, stderr) = consumer(&[
        "sink",
        leaky.to_str().unwrap(),
        "-db",
        out_db.to_str().unwrap(),
        "-key",
        "id",
        "-publication",
        &policy_arg,
    ]);
    assert!(!ok, "the sink landed a feed carrying a denied column: {stdout}");
    assert!(stderr.contains("ssn"), "the sink's refusal does not name the column: {stderr}");

    // Anti-vacuity, on the same binary and the same policy: the projected feed both validates and
    // lands. Without this the test would pass against a consumer that refused every feed.
    let mut guarded: Vec<u8> = Vec::new();
    write_feed(&events, &Publication::parse(POLICY).unwrap(), &mut guarded).unwrap();
    let good = dir.path().join("guarded.jsonl");
    std::fs::write(&good, &guarded).unwrap();
    let (ok, _, stderr) = consumer(&["validate", good.to_str().unwrap(), "-publication", &policy_arg]);
    assert!(ok, "the projected feed was refused: {stderr}");
    let good_db = dir.path().join("guarded.sqlite");
    let (ok, stdout, stderr) = consumer(&[
        "sink",
        good.to_str().unwrap(),
        "-db",
        good_db.to_str().unwrap(),
        "-key",
        "id",
        "-publication",
        &policy_arg,
    ]);
    assert!(ok, "the projected feed did not land: {stderr}");
    assert!(stdout.contains("APPLIED"), "the sink reported nothing landed: {stdout}");
    assert!(
        !stdout.contains("APPLIED 0"),
        "the sink landed nothing at all, so accepting it proves nothing: {stdout}"
    );
}

/// **Exit criterion 3, in the shape the unit tests cannot reach.**
///
/// One commit writes a published row and a refused row. Truncating the batch at the offending event
/// rather than at the start of its commit emits the published sibling, computes the cursor from it —
/// landing on that commit's own `commit_end_lsn` — and steps over the refused row permanently: no
/// later pump can reach it, not even after the publication is amended, and nothing reports a gap.
///
/// So this asserts both halves. First that the refusal holds the cursor below the whole commit and
/// delivers none of it, then that resuming from that cursor with a wider publication delivers every
/// event the refusal held back, each exactly once.
#[test]
fn a_refusal_mid_commit_replays_the_whole_commit_rather_than_skipping_the_refused_row() {
    use std::sync::atomic::Ordering;

    let dir = tempfile::tempdir().unwrap();
    let mut d = db(dir.path(), "cursor");
    d.sql("CREATE TABLE customers (id INTEGER NOT NULL, name VARCHAR(32));");
    d.sql("CREATE TABLE audit_log (id INTEGER NOT NULL, actor VARCHAR(16));");

    // The cursor starts ABOVE both DDLs, the way a consumer joining after a snapshot does. Otherwise
    // `audit_log`'s own CREATE_TABLE is the first refusal and the interesting commit is never reached.
    d.wal.flush().unwrap();
    let start = d.wal.flushed_lsn.load(Ordering::SeqCst);

    d.sql("INSERT INTO customers VALUES (1, 'ada');");
    // One transaction, two tables: the published row comes first, so an event-granular truncation
    // would emit it and take the cursor with it.
    d.sql("BEGIN;");
    d.sql("INSERT INTO customers VALUES (2, 'grace');");
    d.sql("INSERT INTO audit_log VALUES (7, 'root');");
    d.sql("COMMIT;");
    d.sql("INSERT INTO customers VALUES (3, 'hopper');");
    d.wal.flush().unwrap();

    // **The premise, asserted rather than assumed.** If ferrodb ever stopped putting both inserts
    // under one commit_lsn, this test would quietly stop exercising the defect — which is precisely
    // how the two earlier instances of this bug class survived.
    let decoder = LogicalDecoder::new(&d.catalog);
    let decoded = decoder
        .decode(&d.wal, start, d.wal.flushed_lsn.load(Ordering::SeqCst))
        .expect("decode");
    let mixed = decoded
        .events
        .iter()
        .filter(|e| e.table == "audit_log")
        .filter_map(|a| {
            decoded
                .events
                .iter()
                .find(|c| c.table == "customers" && c.commit_lsn == a.commit_lsn)
                .map(|c| (a.commit_lsn, c.lsn < a.lsn))
        })
        .next();
    let (mixed_commit, published_first) =
        mixed.expect("no commit carries both a published and a refused row; the shape is not present");
    assert!(
        published_first,
        "the published row does not come first inside the mixed commit, so an event-granular \
         truncation would refuse before emitting anything and this test would pass for free"
    );

    let narrow = FeedStreamer::new(
        LogicalDecoder::new(&d.catalog),
        Publication::named("analytics").publishing("customers", ["id", "name"]),
    );
    let mut first: Vec<u8> = Vec::new();
    let p1 = narrow.pump(&d.wal, start, 0, &mut first).expect("pump");
    let text1 = String::from_utf8(first.clone()).unwrap();

    assert_eq!(p1.emitted, 1, "only the commit before the refusal may be delivered: {p1:?}");
    assert_eq!(
        p1.refused, 3,
        "the refusal must hold back the offending row, its published sibling in the same commit, and \
         the commit after it: {p1:?}"
    );
    let refusal = p1.refusal.as_ref().expect("no refusal was reported");
    assert_eq!(refusal.table, "audit_log");
    assert!(!p1.is_clean(), "a stalled feed reported itself clean: {p1:?}");
    assert!(text1.contains("'ada'") || text1.contains("ada"), "the first row is missing: {text1}");
    assert!(
        !text1.contains("grace"),
        "the published SIBLING of the refused row was emitted, so the cursor is now past its commit \
         and the refused row is unreachable: {text1}"
    );
    assert!(!text1.contains("hopper"), "work after the refusal was delivered: {text1}");
    assert!(
        p1.cursor < mixed_commit,
        "the cursor {} reached or passed the refused commit {mixed_commit}; the refused row can \
         never be emitted",
        p1.cursor
    );

    // Amend the publication; resume from exactly what the refusal left behind.
    let wide = FeedStreamer::new(
        LogicalDecoder::new(&d.catalog),
        Publication::named("analytics")
            .publishing("customers", ["id", "name"])
            .publishing("audit_log", ["id", "actor"]),
    );
    let mut second: Vec<u8> = Vec::new();
    let p2 = wide.pump(&d.wal, p1.cursor, p1.emitted_through, &mut second).expect("pump");
    let text2 = String::from_utf8(second).unwrap();

    assert_eq!(p2.refused, 0, "the widened publication still refused something: {p2:?}");
    assert!(
        text2.contains("root"),
        "THE REFUSED ROW WAS LOST. The cursor advanced past its commit while it was being refused, \
         so no later pump can reach it. Feed after widening:\n{text2}"
    );
    assert!(text2.contains("grace"), "the refused row's sibling was lost: {text2}");
    assert!(text2.contains("hopper"), "the commit after the refusal was lost: {text2}");
    assert_eq!(p2.emitted, 3, "the replay delivered the wrong number of events: {p2:?}");

    // Exactly once, across both pumps: the replay must not duplicate what pump one delivered.
    let whole = format!("{text1}{text2}");
    for (needle, what) in [("ada", "the row before the refusal"), ("grace", "the refused row's sibling")]
    {
        assert_eq!(
            whole.matches(needle).count(),
            1,
            "{what} appears {} times across the two pumps",
            whole.matches(needle).count()
        );
    }

    // The whole feed, both pumps concatenated, is acceptable to the independent consumer under the
    // amended policy — so what the replay produced is a valid feed and not just the right substrings.
    let path = dir.path().join("resumed.jsonl");
    std::fs::write(&path, &whole).unwrap();
    let pol = dir.path().join("wide.txt");
    std::fs::write(&pol, "publication analytics\ncustomers: id, name\naudit_log: id, actor\n").unwrap();
    let (ok, stdout, stderr) =
        consumer(&["validate", path.to_str().unwrap(), "-publication", pol.to_str().unwrap()]);
    assert!(ok, "the resumed feed was refused by the independent consumer:\n{stderr}\n{whole}");
    assert!(stdout.starts_with("OK 4"), "the resumed feed is not four events: {stdout}");
}
