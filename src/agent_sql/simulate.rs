//! `SIMULATE` — K candidate branches off one base, every one scored, the winners admitted.
//!
//! Design authority: DESIGN.md sections 1 and 4, and exit criteria 1, 5, 7 and 8.
//!
//! ```text
//! SIMULATE AS 'pricing-agent' RUN 'r_9'
//!   CANDIDATE 'cut-5'  ( UPDATE inventory SET qty = qty - 5 WHERE id = 1; )
//!   CANDIDATE 'cut-8'  ( UPDATE inventory SET qty = qty - 8 WHERE id = 1; )
//!   ASSERT ON inventory (qty >= 0)
//!   ADMIT ALL;
//! ```
//!
//! # Why this feature is the one the architecture exists for
//!
//! A fork copies **zero** pages — the child's root *is* the parent's root — and a branch's first
//! write copies only the root-to-leaf path it touches. `bench/branch_scaling.txt` measured that
//! path cost on this machine: **2 pages on a 2,000-row trunk, 3 on a 40,000-row trunk, and 21 when
//! each branch writes 20 scattered rows**, with read latency flat from 10 to 1000 branches. So K
//! candidates cost K path-copies, not K database copies, and the difference is what makes running
//! twelve variants of a change a normal thing to do rather than a capacity decision. (Those are
//! ferrodb's numbers against itself at three branch counts; nothing there is a comparison against
//! another system.)
//!
//! # The two hard parts, and where each is solved
//!
//! **1. Scoring one candidate must not move the base under the next.** Merging is split in two:
//! [`AgentRuntime::evaluate_merge`] decides everything and touches nothing, and
//! [`AgentRuntime::publish_evaluation`] applies a decision already made. The scoring pass here
//! evaluates every candidate before anything is published, so all K verdicts are about one
//! identical base — and every verdict carries the fingerprint of the base it was computed against,
//! so that claim is checkable rather than asserted. The split opens a TOCTOU window, which
//! `publish_evaluation` closes by refusing to publish against a base that has moved.
//!
//! **2. Admission is greedy and must be re-checked per admitted set.** Pairwise composition
//! implies nothing about a triple: three candidates that each take 8 from a counter of 20 compose
//! fine in any pair (20 → 12 → 4) and break the invariant as a triple (−4). So admission does not
//! consult the scoring pass's verdict — it **re-evaluates** each candidate against the base as it
//! stands with every already-admitted candidate applied, and admits it only if it is still
//! admissible then. The scoring pass ranks; it does not admit.
//!
//! # What happens to the losers
//!
//! Nothing. They are left **Live**, holding their lease, and the ordinary lease reaper reclaims
//! their pages with no client cooperation at all (exit criterion 8) — that is why a simulation of
//! twelve candidates does not need a client to tidy up after it, and why it costs nothing
//! permanently.
//!
//! A loser is deliberately **not quarantined**, even though a production `MERGE` quarantines a
//! branch the gate declines. Quarantine is a *hold for inspection*: it takes the branch out of
//! `live_branches`, which is exactly the set the lease scan walks, so quarantining every losing
//! candidate would pin the pages of every simulation ever run. A candidate that lost is not being
//! held for anyone to look at; it is being discarded, and the lease is how this database discards
//! things.

use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};

use crate::agent_sql::gate::{AssertionResult, GateOutcome};
use crate::agent_sql::runtime::{AgentRuntime, ExecCtx, MergeEvaluation, DEFAULT_LEASE_MILLIS};
use crate::branch::types::{BranchId, LeaseDeadline};
use crate::error::FerroError;
use crate::parser::parser::{Expr, Stmt};
use crate::tel::merge::MergeOutcome;

/// A declared invariant: a predicate over one table's columns, which must hold in the state a
/// merge would leave behind.
///
/// **Not a guard.** A `Guard` is the `WHERE` that made a write legal, re-evaluated as a
/// *precondition* against the image the ops are about to land on; DESIGN.md section 3 is explicit
/// that a precondition cannot see a post-op violation, and that writing an invariant as a guard
/// does not enforce it. An assertion is evaluated against the result. Both exist, and they answer
/// different questions.
#[derive(Debug, Clone)]
pub struct Assertion {
    /// The table the predicate ranges over. Every row of it, as the merge would leave it, is
    /// checked — not only the rows the candidate touched, because a candidate that touches nothing
    /// would otherwise satisfy every assertion.
    pub table: String,
    pub predicate: Expr,
}

impl Assertion {
    pub fn new(table: impl Into<String>, predicate: Expr) -> Self {
        Assertion { table: table.into(), predicate }
    }

    /// The predicate as SQL text, which is what a violated assertion hands back.
    pub fn source(&self) -> String {
        self.predicate.to_sql()
    }
}

/// One candidate: a name and the statements to run on its own branch.
#[derive(Debug, Clone)]
pub struct Candidate {
    pub name: String,
    pub body: Vec<Stmt>,
}

/// How many winners to admit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmitPolicy {
    /// Every candidate that is still admissible when its turn comes.
    All,
    /// At most `n`. `AtMost(0)` is a dry run: everything is scored, nothing is published, and the
    /// base is provably untouched at the end.
    AtMost(usize),
}

impl AdmitPolicy {
    fn budget(&self, candidates: usize) -> usize {
        match self {
            AdmitPolicy::All => candidates,
            AdmitPolicy::AtMost(n) => (*n).min(candidates),
        }
    }
}

/// A whole simulation, resolved: no names left to look up.
#[derive(Debug, Clone)]
pub struct SimulationPlan {
    pub agent_id: String,
    /// The run behind the simulation. Each candidate gets its own run id derived from it, because
    /// provenance is interned per run and two candidates are two tasks — asking "which agent wrote
    /// this row" after admission must name the candidate, not the simulation.
    pub run_id: Option<String>,
    pub model: Option<(String, String)>,
    pub candidates: Vec<Candidate>,
    pub assertions: Vec<Assertion>,
    pub admit: AdmitPolicy,
    /// Lease handed to every candidate branch. The losers are reclaimed when it expires.
    pub lease_millis: u64,
}

impl SimulationPlan {
    pub fn new(agent_id: impl Into<String>) -> Self {
        SimulationPlan {
            agent_id: agent_id.into(),
            run_id: None,
            model: None,
            candidates: Vec::new(),
            assertions: Vec::new(),
            // The DRY RUN, not `All`. The SQL grammar has no default at all — how much of a
            // simulation's output to publish is the caller's decision — and a builder that has to
            // pick one must pick the value that publishes nothing. A caller who forgets `.admit()`
            // gets a scored report, not twelve merges nobody asked for.
            admit: AdmitPolicy::AtMost(0),
            lease_millis: DEFAULT_LEASE_MILLIS,
        }
    }

    pub fn candidate(mut self, name: impl Into<String>, body: Vec<Stmt>) -> Self {
        self.candidates.push(Candidate { name: name.into(), body });
        self
    }

    pub fn assert_on(mut self, table: impl Into<String>, predicate: Expr) -> Self {
        self.assertions.push(Assertion::new(table, predicate));
        self
    }

    pub fn admit(mut self, admit: AdmitPolicy) -> Self {
        self.admit = admit;
        self
    }

    pub fn run(mut self, run_id: impl Into<String>) -> Self {
        self.run_id = Some(run_id.into());
        self
    }

    pub fn lease_millis(mut self, ms: u64) -> Self {
        self.lease_millis = ms;
        self
    }
}

/// What one evaluation decided, kept after the evaluation itself is dropped.
///
/// The evaluation cannot be kept: it names a base state, and holding one across a publication is
/// the stale-verdict bug the fingerprint exists to refuse. This is the part worth reporting.
#[derive(Debug, Clone)]
pub struct Verdict {
    pub gate: GateOutcome,
    pub outcome: MergeOutcome,
    pub assertions: Vec<AssertionResult>,
    /// Assertions that held over assertions declared.
    pub score: Option<f64>,
    /// The gate passed and nothing conflicted.
    pub admissible: bool,
    /// The base this verdict was computed against. Two verdicts with the same fingerprint were
    /// scored against the same database.
    pub base_fingerprint: u64,
}

impl Verdict {
    fn of(e: &MergeEvaluation) -> Self {
        Verdict {
            gate: e.gate.clone(),
            outcome: e.outcome.clone(),
            assertions: e.assertions.clone(),
            score: e.score(),
            admissible: e.is_admissible(),
            base_fingerprint: e.base_fingerprint(),
        }
    }

    /// Every declared assertion that did not hold, by the predicate as written.
    pub fn failed_assertions(&self) -> Vec<String> {
        crate::agent_sql::gate::failed_assertions(&self.assertions)
    }
}

/// One candidate's fate.
#[derive(Debug, Clone)]
pub struct CandidateScore {
    pub name: String,
    pub branch: BranchId,
    pub branch_name: String,
    pub run_id: String,
    /// Rows the candidate's body WROTE on its own branch. Rows a `SELECT` returned are counted
    /// separately: `rows_written == 0` has to mean the candidate changed nothing, or nothing can
    /// be read off it.
    pub rows_written: usize,
    /// Rows the candidate's body READ. Not a cost measure — it is what the read-premise check at
    /// admission is about.
    pub rows_read: usize,
    /// Set when the candidate's body failed to run at all. Such a candidate is never scored and
    /// never admitted; it is reported and left for the reaper.
    pub error: Option<String>,
    /// The verdict from the scoring pass, against the untouched base. Every candidate's scoring
    /// verdict has the same `base_fingerprint`.
    pub scored: Option<Verdict>,
    /// The verdict from the re-evaluation at this candidate's turn in the greedy admission, which
    /// is the **only** verdict admission acts on. `None` when the admission budget ran out first.
    pub rechecked: Option<Verdict>,
    pub admitted: bool,
    pub merge_id: Option<String>,
}

impl CandidateScore {
    /// A candidate that was scored admissible and then refused at admission, which happens exactly
    /// when an earlier admission moved the base out from under it.
    pub fn refused_after_recheck(&self) -> bool {
        !self.admitted
            && self.scored.as_ref().is_some_and(|v| v.admissible)
            && self.rechecked.as_ref().is_some_and(|v| !v.admissible)
    }
}

/// What `SIMULATE` hands back. Structured, never rendered text.
#[derive(Debug, Clone)]
pub struct SimulationReport {
    pub base: BranchId,
    pub candidates: Vec<CandidateScore>,
    /// Live page count immediately before the K forks, and immediately after them. `None` when the
    /// runtime has no page store. Equal is the criterion, and `simulate` refuses if they are not.
    pub pages_before_fork: Option<u32>,
    pub pages_after_fork: Option<u32>,
    /// Live page count after every candidate's body has run. Reported so the *cost* of K
    /// candidates is a measurement rather than a claim.
    pub pages_after_bodies: Option<u32>,
}

impl SimulationReport {
    pub fn admitted(&self) -> Vec<&CandidateScore> {
        self.candidates.iter().filter(|c| c.admitted).collect()
    }

    /// The branches left behind for the lease reaper.
    pub fn losers(&self) -> Vec<&CandidateScore> {
        self.candidates.iter().filter(|c| !c.admitted).collect()
    }

    pub fn get(&self, name: &str) -> Option<&CandidateScore> {
        self.candidates.iter().find(|c| c.name == name)
    }

    /// Pages the K candidate bodies cost in total, when the runtime is page-backed.
    pub fn pages_written_by_candidates(&self) -> Option<u32> {
        match (self.pages_after_bodies, self.pages_after_fork) {
            (Some(a), Some(b)) => Some(a.saturating_sub(b)),
            _ => None,
        }
    }
}

impl Display for SimulationReport {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "simulated {} candidate(s) off {}: {} admitted",
            self.candidates.len(),
            self.base,
            self.admitted().len()
        )?;
        if let (Some(b), Some(a)) = (self.pages_before_fork, self.pages_after_fork) {
            write!(f, " (fork cost {} page(s))", a as i64 - b as i64)?;
        }
        for c in &self.candidates {
            write!(f, "\n  {} [{}]", c.name, c.branch_name)?;
            if let Some(e) = &c.error {
                write!(f, " ERRORED: {e}")?;
                continue;
            }
            if let Some(v) = &c.scored {
                write!(f, " scored {:.2}", v.score.unwrap_or(0.0))?;
            }
            match (&c.rechecked, c.admitted) {
                (Some(_), true) => {
                    write!(f, " -> ADMITTED as {}", c.merge_id.as_deref().unwrap_or("?"))?
                }
                (Some(v), false) => {
                    write!(f, " -> refused at admission: {}", describe(v))?;
                }
                (None, _) => write!(f, " -> not reached (admission budget spent)")?,
            }
        }
        Ok(())
    }
}

fn describe(v: &Verdict) -> String {
    let mut parts: Vec<String> = Vec::new();
    if !v.gate.is_pass() {
        parts.extend(v.gate.findings().iter().map(|f| f.detail.clone()));
    }
    if v.outcome.is_conflict() {
        parts.push(format!("merge conflict ({})", v.outcome.name()));
    }
    if parts.is_empty() {
        parts.push("admissible".into());
    }
    parts.join("; ")
}

impl AgentRuntime {
    /// Fork K candidates off `base`, run each one, score every one, admit the winners.
    ///
    /// See the module documentation for the shape of the algorithm and why it is that shape. The
    /// order of operations is load-bearing, so it is stated here as well:
    ///
    /// 1. **Fork K branches**, and check that the live page count did not move. The claim that a
    ///    fork copies zero pages is the reason this feature is affordable, so it is measured on
    ///    every run rather than assumed and refused if it is ever false.
    /// 2. **Run each candidate's body** on its own branch. Writes land in that branch's workspace
    ///    and on its own copy-on-write pages; nothing is visible to the base or to a sibling.
    /// 3. **Score every candidate against the identical base.** Nothing is published in this pass,
    ///    so the K-th candidate is scored against exactly the base the first one was.
    /// 4. **Admit greedily, re-evaluating at every step.** The scoring pass ranks the candidates;
    ///    the admission decision is taken from a fresh evaluation against the base *including
    ///    everything already admitted*, because two candidates composing says nothing about three.
    ///
    /// The losers are left alive with their leases running. The reaper takes them.
    pub fn simulate(
        &self,
        ctx: &mut ExecCtx,
        base: BranchId,
        plan: &SimulationPlan,
    ) -> Result<SimulationReport, FerroError> {
        if plan.candidates.is_empty() {
            return Err(FerroError::Bind(
                "SIMULATE needs at least one CANDIDATE: a simulation with nothing to compare \
                 admits nothing and proves nothing"
                    .into(),
            ));
        }
        if plan.assertions.is_empty() {
            // Refusing rather than defaulting. A simulation with no declared assertion scores
            // every candidate perfectly against nothing, which is the shape of a run that
            // collected no evidence and reported a pass.
            return Err(FerroError::Bind(
                "SIMULATE needs at least one ASSERT: with nothing declared, every candidate \
                 scores 1.00 against no evidence and admission would be decided by declaration \
                 order alone"
                    .into(),
            ));
        }
        let mut seen: BTreeSet<&str> = BTreeSet::new();
        for c in &plan.candidates {
            if c.name.trim().is_empty() {
                return Err(FerroError::Bind("a CANDIDATE must be named".into()));
            }
            if !seen.insert(c.name.as_str()) {
                return Err(FerroError::Bind(format!(
                    "two candidates are both named '{}'; the report would attribute one \
                     candidate's result to the other",
                    c.name
                )));
            }
        }

        // Losing candidates are left alive for the lease reaper, and the reaper reclaims their
        // PAGES without telling this runtime — so their workspaces would accumulate here for the
        // life of the process. Sweeping at the start of a simulation rather than at the end keeps
        // a loser queryable for as long as its branch is alive, which is the point of leaving it
        // alive, while bounding what a server that runs simulations all day retains.
        self.forget_reaped_branches();

        // ---- 1. fork K, and prove the fork copied nothing ---------------------------------
        let pages_before_fork = self.live_page_count()?;
        let mut forked: Vec<(usize, crate::agent_sql::session::AgentSession)> =
            Vec::with_capacity(plan.candidates.len());
        for (i, c) in plan.candidates.iter().enumerate() {
            // `<unnamed>` is what `begin_session` already records for a task whose caller
            // declared no run, and it means exactly that here too. Note what it does NOT mean:
            // two simulations that both omit RUN and share a candidate name intern to the SAME
            // run, so `who_wrote_row` cannot tell them apart. That is the honest reading of "no
            // run was declared" rather than a unique id invented on the caller's behalf — declare
            // RUN if the provenance has to distinguish one simulation from another.
            let run = match &plan.run_id {
                Some(r) => format!("{r}/{}", c.name),
                None => format!("<unnamed>/{}", c.name),
            };
            let model = plan.model.as_ref().map(|(n, v)| (n.as_str(), v.as_str()));
            let session =
                self.begin_session_with_model(&plan.agent_id, Some(&run), model, base)?;
            self.branches()
                .renew_lease(session.branch, LeaseDeadline::from_now(plan.lease_millis))?;
            forked.push((i, session));
        }
        let pages_after_fork = self.live_page_count()?;
        if let (Some(before), Some(after)) = (pages_before_fork, pages_after_fork) {
            if after != before {
                // Not a warning. K candidates are affordable *because* a fork copies nothing; if
                // that stops being true the cost model this feature rests on is gone, and the
                // honest response is to refuse rather than to run K database copies.
                //
                // **Stated blind spot:** `live_page_count` is a store-wide counter, so a page
                // another connection allocated between the two readings is attributed to the
                // forks and refuses this simulation. That direction is the safe one — a refusal
                // before any candidate has run costs a retry, while a silent K-copy fork costs
                // the cost model — but it is a false positive, and the same scope means
                // `pages_after_bodies` is a measurement of every writer in the process rather
                // than of these candidates alone.
                return Err(FerroError::Branch(format!(
                    "forking {} candidate branches changed the live page count from {before} to \
                     {after}; a fork must copy zero pages, so the cost model SIMULATE depends on \
                     no longer holds",
                    plan.candidates.len()
                )));
            }
        }

        // ---- 2. run each candidate on its own branch --------------------------------------
        let mut scores: Vec<CandidateScore> = Vec::with_capacity(forked.len());
        for (i, session) in &forked {
            let c = &plan.candidates[*i];
            let mut rows_written = 0usize;
            let mut rows_read = 0usize;
            let mut error = None;
            for stmt in &c.body {
                // Reads and writes are counted apart. Folding a SELECT's row count into
                // `rows_written` made every reading candidate report rows it never wrote.
                let outcome = match stmt {
                    Stmt::Select { .. } => {
                        // Reads are executed and their read-set retained, because the read-set is
                        // what the read-premise check at admission is about.
                        self.select(ctx, session.branch, stmt, Some(session.branch))
                            .map(|r| (0, r.len()))
                    }
                    other => self.write(ctx, session.branch, other.clone()).map(|n| (n, 0)),
                };
                match outcome {
                    Ok((w, r)) => {
                        rows_written += w;
                        rows_read += r;
                    }
                    Err(e) => {
                        error = Some(e.to_string());
                        break;
                    }
                }
            }
            scores.push(CandidateScore {
                name: c.name.clone(),
                branch: session.branch,
                branch_name: session.branch_name.clone(),
                run_id: session.run_id.clone(),
                rows_written,
                rows_read,
                error,
                scored: None,
                rechecked: None,
                admitted: false,
                merge_id: None,
            });
        }
        let pages_after_bodies = self.live_page_count()?;

        // ---- 3. score every candidate against ONE base ------------------------------------
        //
        // Every evaluation here is thrown away after its verdict is extracted. Keeping one and
        // publishing it later is precisely the stale-verdict bug, and `publish_evaluation` would
        // refuse it — this pass exists to rank, not to admit.
        for s in scores.iter_mut() {
            if s.error.is_some() {
                continue;
            }
            let eval = self.evaluate_merge(ctx, s.branch, &plan.assertions)?;
            s.scored = Some(Verdict::of(&eval));
        }

        // Ranked: admissible first, then by score, then by declaration order. A candidate the
        // scoring pass refused is still re-evaluated when its turn comes — the base may have moved
        // in the direction that makes it legal — it simply queues behind the ones that passed.
        //
        // **Among admissible candidates this IS declaration order, and that is not an accident to
        // be fixed by sorting harder.** A candidate is admissible only if every assertion held, so
        // every admissible candidate scores exactly 1.00 and the score cannot separate them.
        // SIMULATE has no objective function: it can say which candidates are legal, never which
        // legal candidate is best. So `ADMIT 1` over ten admissible candidates publishes the one
        // declared first — including an empty "change nothing" control, if that is what you put
        // first. Declare them in preference order, or admit them all.
        let mut order: Vec<usize> = (0..scores.len()).collect();
        order.sort_by(|a, b| {
            let (x, y) = (&scores[*a], &scores[*b]);
            let key = |s: &CandidateScore| {
                let v = s.scored.as_ref();
                (
                    !v.is_some_and(|v| v.admissible),
                    -(v.and_then(|v| v.score).unwrap_or(0.0) * 1000.0) as i64,
                )
            };
            key(x).cmp(&key(y)).then(a.cmp(b))
        });

        // ---- 4. greedy admission, re-evaluated against each admitted set ------------------
        let budget = plan.admit.budget(plan.candidates.len());
        let mut admitted = 0usize;
        for idx in order {
            if admitted >= budget {
                break;
            }
            if scores[idx].error.is_some() {
                continue;
            }
            let branch = scores[idx].branch;
            // The fresh evaluation is the whole mechanism. It sees every candidate admitted before
            // it — through the target's rows and through the ops those merges recorded — so a
            // third candidate is judged against the pair, not against the base the pair started
            // from.
            let eval = self.evaluate_merge(ctx, branch, &plan.assertions)?;
            let verdict = Verdict::of(&eval);
            let ok = eval.is_admissible();
            scores[idx].rechecked = Some(verdict);
            if ok {
                let report = self.publish_evaluation(ctx, eval)?;
                scores[idx].admitted = report.applied_to_target;
                scores[idx].merge_id = Some(report.merge_id);
                admitted += 1;
            }
        }

        Ok(SimulationReport {
            base,
            candidates: scores,
            pages_before_fork,
            pages_after_fork,
            pages_after_bodies,
        })
    }
}
