//! The verification gate: tiered checks between a branch and its merge.
//!
//! Design authority: DESIGN.md section 4. Three rules from it drive every decision in this file,
//! and each is load-bearing rather than stylistic.
//!
//! **1. Ordering is cost ÷ rejection-probability, not cheapest-first.** A free check that never
//! fires contributes zero information to every branch that passes it, so it is not free — it is
//! pure latency. The order is therefore *computed* from each tier's cost and how often it rejects,
//! not hard-coded. Hard-coding the order DESIGN.md happens to list would make the rule decorative:
//! it would still produce the right sequence while being unable to notice if the inputs changed.
//!
//! **2. Short-circuit BETWEEN tiers, but run ALL checks within a tier.** Stopping at the first
//! failing check inside a tier teaches the agent one violation per round trip, so N defects cost N
//! round trips. Stopping between tiers is what keeps the expensive tiers from running at all.
//!
//! **3. The outcome is chosen by the epistemic status of what fired, not by its severity.** A
//! check that is sound and crisp can hand back a predicate and ask for a retry. A heuristic has no
//! business rejecting anything — it can only quarantine. A check that could not be evaluated is
//! the dangerous case: not knowing is not the same as passing, so it hard-rejects.

use std::fmt;

use crate::tel::ids::{RowId, TableId};

/// How much a tier's checks are trusted, which decides what may be done about them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum CheckStatus {
    /// Decidable and exact. May demand a retry, and hands back the predicate it violated.
    SoundAndCrisp,
    /// Indicative, not decisive. May quarantine; must never reject on its own.
    Heuristic,
    /// The check could not run. Ordered last because it is the most severe: not knowing whether a
    /// merge is safe is strictly worse than knowing it is not.
    NotEvaluable,
}

impl fmt::Display for CheckStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            CheckStatus::SoundAndCrisp => "sound",
            CheckStatus::Heuristic => "heuristic",
            CheckStatus::NotEvaluable => "not-evaluable",
        };
        f.write_str(s)
    }
}

/// A group of checks that share a cost profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tier {
    /// Shape of the changeset: malformed ops, unknown tables, undecodable rows.
    Structural,
    /// How much the change touches, and how much of it the agent never looked at.
    BlastRadius,
    /// Declared invariants re-checked against the state the merge would produce.
    Invariants,
}

impl Tier {
    /// Relative cost of running the tier. Unitless; only the ratio with [`Self::rejection_rate`]
    /// is ever used.
    pub fn cost(&self) -> f64 {
        match self {
            // Reading the changeset you already have in hand.
            Tier::Structural => 1.0,
            // Set arithmetic over the retained read-set.
            Tier::BlastRadius => 4.0,
            // Re-evaluating predicates against merged state, which has to be composed first.
            Tier::Invariants => 20.0,
        }
    }

    /// How often this tier is expected to reject. These are estimates, and they are stated here as
    /// numbers precisely so they can be argued with and replaced by measurements — a rule whose
    /// inputs are invisible cannot be checked.
    pub fn rejection_rate(&self) -> f64 {
        match self {
            Tier::Structural => 0.05,
            Tier::BlastRadius => 0.15,
            Tier::Invariants => 0.30,
        }
    }

    /// DESIGN.md's ordering key: cost ÷ rejection-probability. Lower runs first.
    pub fn order_key(&self) -> f64 {
        self.cost() / self.rejection_rate()
    }

    /// Every tier, in the order the gate will run them.
    pub fn ordered() -> Vec<Tier> {
        let mut all = vec![Tier::Invariants, Tier::BlastRadius, Tier::Structural];
        all.sort_by(|a, b| a.order_key().partial_cmp(&b.order_key()).expect("costs are finite"));
        all
    }
}

/// One check that fired.
#[derive(Debug, Clone, PartialEq)]
pub struct Finding {
    pub tier: Tier,
    pub check: String,
    pub status: CheckStatus,
    /// For a sound check this is the violated predicate, which is what the agent retries against.
    pub detail: String,
}

/// What the gate decided.
#[derive(Debug, Clone, PartialEq)]
pub enum GateOutcome {
    /// Nothing fired.
    Pass,
    /// A sound, crisp check fired. The agent can fix this and try again.
    Retry(Vec<Finding>),
    /// A heuristic fired. Not merged, not discarded — see the quarantine row in the ledger; until
    /// that exists this outcome is reported and the caller decides.
    Quarantine(Vec<Finding>),
    /// A check could not be evaluated. Refused, because not knowing is not passing.
    HardReject(Vec<Finding>),
}

impl GateOutcome {
    pub fn findings(&self) -> &[Finding] {
        match self {
            GateOutcome::Pass => &[],
            GateOutcome::Retry(f) | GateOutcome::Quarantine(f) | GateOutcome::HardReject(f) => f,
        }
    }

    pub fn is_pass(&self) -> bool {
        matches!(self, GateOutcome::Pass)
    }

    pub fn name(&self) -> &'static str {
        match self {
            GateOutcome::Pass => "Pass",
            GateOutcome::Retry(_) => "Retry",
            GateOutcome::Quarantine(_) => "Quarantine",
            GateOutcome::HardReject(_) => "HardReject",
        }
    }
}

/// A check the gate can run. Returning `None` means it passed.
pub trait Check: Send + Sync {
    fn name(&self) -> String;
    fn tier(&self) -> Tier;
    fn status(&self) -> CheckStatus;
    /// `None` = passed. `Some(detail)` = fired, with the predicate or reason.
    fn evaluate(&self) -> Option<String>;
}

/// Runs checks tier by tier.
#[derive(Default)]
pub struct VerificationGate {
    checks: Vec<Box<dyn Check>>,
}

impl VerificationGate {
    pub fn new() -> Self {
        VerificationGate { checks: Vec::new() }
    }

    pub fn with(mut self, c: Box<dyn Check>) -> Self {
        self.checks.push(c);
        self
    }

    pub fn len(&self) -> usize {
        self.checks.len()
    }

    pub fn is_empty(&self) -> bool {
        self.checks.is_empty()
    }

    /// Run the gate.
    ///
    /// Tiers run in `cost ÷ rejection-probability` order. Within a tier every check runs even after
    /// one has fired, so the agent learns all of that tier's defects in a single round trip. The
    /// moment a tier produces any finding, no later tier runs.
    pub fn run(&self) -> GateOutcome {
        for tier in Tier::ordered() {
            let mut fired = Vec::new();
            for c in self.checks.iter().filter(|c| c.tier() == tier) {
                // Deliberately not `break`ing on the first hit: see rule 2 in the module doc.
                if let Some(detail) = c.evaluate() {
                    fired.push(Finding {
                        tier,
                        check: c.name(),
                        status: c.status(),
                        detail,
                    });
                }
            }
            if !fired.is_empty() {
                return Self::decide(fired);
            }
        }
        GateOutcome::Pass
    }

    /// Outcome from epistemic status, taking the most severe that fired.
    ///
    /// `CheckStatus` orders `SoundAndCrisp < Heuristic < NotEvaluable`, and that order *is* the
    /// severity order: a heuristic cannot be allowed to force the retry a sound check would, and
    /// an unevaluable check outranks both because it is the only one that leaves the question open.
    fn decide(fired: Vec<Finding>) -> GateOutcome {
        let worst = fired.iter().map(|f| f.status).max().expect("fired is non-empty");
        match worst {
            CheckStatus::SoundAndCrisp => GateOutcome::Retry(fired),
            CheckStatus::Heuristic => GateOutcome::Quarantine(fired),
            CheckStatus::NotEvaluable => GateOutcome::HardReject(fired),
        }
    }
}

/// **A premise that changed under the branch while it was working.**
///
/// The branch retained the exact versions it READ. This compares each of them against the version the
/// base holds now, at merge admission. If a row the branch reasoned from has been rewritten since the
/// fork, the conclusion was drawn from state that no longer exists — even though the branch may have
/// written somewhere else entirely, so nothing the conflict resolver looks at overlaps.
///
/// That gap is the reason this check exists. The merge engine validates the cells a branch **wrote**;
/// the read-set was retained and nothing consulted it, so two agents could each read one row, each act
/// on it, and both merge clean. Two hospital agents each reading that one on-call physician remains,
/// each releasing a different one, is the canonical shape.
///
/// # Why the status is a property of the READ, not of this check
///
/// `Tier::Invariants`, and the status varies per instance, which is what `CheckStatus` is for and what
/// nothing had used it for. A point or index read retains exact versions, so the comparison is an exact
/// set intersection: `SoundAndCrisp`, and it may demand a retry. A scan retains a predicate summary with
/// unbounded bounds — a whole-table over-approximation — so it can only say "something in this table
/// moved": `Heuristic`, which the gate will only ever let quarantine. Over-approximating staleness
/// produces false aborts, and a false abort on an agent's finished work is worse than a late one.
pub struct ReadPremiseCheck {
    /// `(table, row, version read, version the base holds now)` for every premise that moved.
    moved: Vec<(TableId, RowId, u64, u64)>,
    /// True when any part of the branch's read-set was a scan, which makes the answer approximate.
    approximate: bool,
}

impl ReadPremiseCheck {
    pub fn new(moved: Vec<(TableId, RowId, u64, u64)>, approximate: bool) -> Self {
        ReadPremiseCheck { moved, approximate }
    }
}

impl Check for ReadPremiseCheck {
    fn name(&self) -> String {
        "read-premise".into()
    }
    fn tier(&self) -> Tier {
        Tier::Invariants
    }
    fn status(&self) -> CheckStatus {
        // Exact when every read named its versions; approximate the moment one of them was a scan.
        if self.approximate {
            CheckStatus::Heuristic
        } else {
            CheckStatus::SoundAndCrisp
        }
    }
    fn evaluate(&self) -> Option<String> {
        if self.moved.is_empty() {
            return None;
        }
        let rows: Vec<String> = self
            .moved
            .iter()
            .map(|(t, r, was, now)| format!("t{}:r{} (read version {was}, base now {now})", t.0, r.0))
            .collect();
        Some(format!(
            "{} row(s) this branch read changed in the base before it merged: {}",
            self.moved.len(),
            rows.join(", ")
        ))
    }
}

/// The blind-write metric as a gate check: rows the branch changed without ever reading them.
///
/// `BlastRadius` tier, and `Heuristic` status — which is the whole reason the taxonomy exists. A
/// blind write is genuinely suspicious and genuinely not proof of anything: the agent may have had
/// every right to set that row without looking. So it may quarantine and may not reject, and the
/// gate enforces that from the status rather than from the caller remembering to.
pub struct BlindWriteCheck {
    blind: Vec<(TableId, RowId)>,
}

impl BlindWriteCheck {
    pub fn new(blind: Vec<(TableId, RowId)>) -> Self {
        BlindWriteCheck { blind }
    }
}

impl Check for BlindWriteCheck {
    fn name(&self) -> String {
        "blind-writes".into()
    }
    fn tier(&self) -> Tier {
        Tier::BlastRadius
    }
    fn status(&self) -> CheckStatus {
        CheckStatus::Heuristic
    }
    fn evaluate(&self) -> Option<String> {
        if self.blind.is_empty() {
            return None;
        }
        let rows: Vec<String> =
            self.blind.iter().map(|(t, r)| format!("t{}:r{}", t.0, r.0)).collect();
        Some(format!(
            "{} row(s) written without being read: {}",
            self.blind.len(),
            rows.join(", ")
        ))
    }
}

/// **A declarative assertion, re-evaluated against the state a merge would produce.**
///
/// This is what `Tier::Invariants` was named for and what nothing had yet put in it: "declared
/// invariants re-checked against the state the merge would produce".
///
/// # Why this is not a guard, and why the difference is the whole point
///
/// A `Guard` is a **precondition**: the `WHERE qty >= 5` that made a write legal, re-evaluated
/// against the state the ops are about to be applied *to*. DESIGN.md section 3 is explicit that a
/// precondition cannot see a post-op violation — start at 20, two agents each take 12 under
/// `WHERE qty >= 0`, the second merge tests `8 >= 0`, that passes, and main ends at −4. "An
/// invariant written as an invariant is not enforced by this mechanism at all."
///
/// An assertion is the other half: it is evaluated against the image the merge would leave behind,
/// so `qty >= 0` over a merged value of −4 fires. Guards keep their meaning (they are what the
/// agent's write claimed), and assertions are what a caller declares about the *result*.
///
/// # Epistemic status, which is not fixed
///
/// `SoundAndCrisp` when every row it named could be evaluated: the answer is exact and the caller
/// gets the predicate back to retry against. `NotEvaluable` — which the gate hard-rejects — in two
/// cases, and the second one is the one that matters:
///
/// * a row where the predicate could not be evaluated at all (the column is gone, the cell is
///   absent), because not knowing is not the same as passing; and
/// * **an assertion that examined ZERO rows.** A predicate that ran over nothing did not hold, it
///   simply never ran, and reporting that as a pass is how a simulation admits every candidate on
///   the strength of an empty table. This is the vacuity trap in the form the type system can
///   refuse.
#[derive(Debug, Clone, PartialEq)]
pub struct AssertionResult {
    /// The predicate as it was written, verbatim, for handing back on violation.
    pub source: String,
    /// The table the predicate ranges over.
    pub table: String,
    pub tbl: TableId,
    /// How many rows the predicate was actually evaluated over. Zero is a failure, not a pass.
    pub rows_checked: usize,
    /// `(row, the cells the predicate referred to)` for every row where it is false.
    pub violations: Vec<(RowId, String)>,
    /// `(row, why)` for every row where it could not be evaluated at all.
    pub unevaluable: Vec<(RowId, String)>,
}

impl AssertionResult {
    /// True only when the predicate ran over at least one row and held on every one of them.
    pub fn holds(&self) -> bool {
        self.rows_checked > 0 && self.violations.is_empty() && self.unevaluable.is_empty()
    }

    /// The epistemic status of this result. See the type's documentation for why zero rows is
    /// `NotEvaluable` rather than a pass.
    pub fn status(&self) -> CheckStatus {
        if self.rows_checked == 0 || !self.unevaluable.is_empty() {
            CheckStatus::NotEvaluable
        } else {
            CheckStatus::SoundAndCrisp
        }
    }
}

/// One [`AssertionResult`] presented to the gate as a check.
///
/// One check per assertion rather than one check for all of them, because the gate runs every
/// check within a tier even after one has fired: a caller with three broken assertions learns all
/// three in one round trip instead of one per retry.
pub struct AssertionCheck {
    result: AssertionResult,
}

impl AssertionCheck {
    pub fn new(result: AssertionResult) -> Self {
        AssertionCheck { result }
    }

    pub fn result(&self) -> &AssertionResult {
        &self.result
    }
}

impl Check for AssertionCheck {
    fn name(&self) -> String {
        format!("assert on {}", self.result.table)
    }
    fn tier(&self) -> Tier {
        Tier::Invariants
    }
    fn status(&self) -> CheckStatus {
        self.result.status()
    }
    fn evaluate(&self) -> Option<String> {
        if self.result.holds() {
            return None;
        }
        if self.result.rows_checked == 0 {
            return Some(format!(
                "`{}` examined 0 row(s) of {}: an assertion that never ran is not an assertion \
                 that held",
                self.result.source, self.result.table
            ));
        }
        let mut parts = Vec::new();
        if !self.result.violations.is_empty() {
            let rows: Vec<String> = self
                .result
                .violations
                .iter()
                .map(|(r, img)| format!("r{} ({})", r.0, img))
                .collect();
            parts.push(format!(
                "`{}` is false for {} of {} row(s) of {}: {}",
                self.result.source,
                self.result.violations.len(),
                self.result.rows_checked,
                self.result.table,
                rows.join(", ")
            ));
        }
        if !self.result.unevaluable.is_empty() {
            let rows: Vec<String> = self
                .result
                .unevaluable
                .iter()
                .map(|(r, why)| format!("r{} ({})", r.0, why))
                .collect();
            parts.push(format!(
                "`{}` could not be evaluated on {} row(s) of {}: {}",
                self.result.source,
                self.result.unevaluable.len(),
                self.result.table,
                rows.join(", ")
            ));
        }
        Some(parts.join("; "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A check that records whether it ran, so short-circuiting can be observed rather than assumed.
    struct Probe {
        name: String,
        tier: Tier,
        status: CheckStatus,
        fires: bool,
        ran: Arc<AtomicUsize>,
    }

    impl Check for Probe {
        fn name(&self) -> String {
            self.name.clone()
        }
        fn tier(&self) -> Tier {
            self.tier
        }
        fn status(&self) -> CheckStatus {
            self.status
        }
        fn evaluate(&self) -> Option<String> {
            self.ran.fetch_add(1, Ordering::SeqCst);
            self.fires.then(|| format!("{} fired", self.name))
        }
    }

    fn probe(
        name: &str,
        tier: Tier,
        status: CheckStatus,
        fires: bool,
    ) -> (Box<dyn Check>, Arc<AtomicUsize>) {
        let ran = Arc::new(AtomicUsize::new(0));
        (
            Box::new(Probe {
                name: name.into(),
                tier,
                status,
                fires,
                ran: Arc::clone(&ran),
            }),
            ran,
        )
    }

    /// The expected sequence here is taken from DESIGN.md section 4, which names the tiers as
    /// structural -> blast-radius -> declared invariants. It is NOT read off the implementation:
    /// the point of the test is that the cost ÷ rejection-probability ratio *reproduces* the
    /// order the design states. If someone retunes the cost or rejection numbers and the order
    /// changes, this fails and the disagreement with DESIGN.md has to be resolved deliberately.
    #[test]
    fn tier_order_is_computed_from_cost_over_rejection_probability() {
        assert_eq!(
            Tier::ordered(),
            vec![Tier::Structural, Tier::BlastRadius, Tier::Invariants],
            "computed order disagrees with DESIGN.md section 4: {:?}",
            Tier::ordered().iter().map(|t| (*t, t.order_key())).collect::<Vec<_>>()
        );
    }

    /// The rule's actual content, shown with explicit numbers rather than with whichever tiers
    /// happen to exist: a tier can be the cheapest available and still be ordered last, because
    /// the key is cost ÷ rejection-probability and not cost.
    ///
    /// Written this way after the first version asserted the real tiers came out in a different
    /// order and was simply wrong about them — the arithmetic, not the rule, was what I had
    /// guessed at.
    #[test]
    fn a_cheap_check_that_almost_never_fires_sorts_last() {
        let cheap_and_useless = 1.0f64 / 0.001; // free, fires one time in a thousand
        let dear_and_decisive = 20.0f64 / 0.30; // expensive, rejects often

        assert!(
            cheap_and_useless > dear_and_decisive,
            "cost ÷ rejection-probability must put a free check that never fires behind an \
             expensive one that usually does; cheapest-first would invert this"
        );
        // And the real tiers use that same key rather than raw cost.
        for t in [Tier::Structural, Tier::BlastRadius, Tier::Invariants] {
            assert_eq!(t.order_key(), t.cost() / t.rejection_rate());
        }
    }

    /// Rule 2, the half that costs round trips.
    #[test]
    fn every_check_in_a_tier_runs_even_after_one_has_fired() {
        let (a, a_ran) = probe("a", Tier::Structural, CheckStatus::SoundAndCrisp, true);
        let (b, b_ran) = probe("b", Tier::Structural, CheckStatus::SoundAndCrisp, true);
        let (c, c_ran) = probe("c", Tier::Structural, CheckStatus::SoundAndCrisp, false);
        let out = VerificationGate::new().with(a).with(b).with(c).run();

        assert_eq!(a_ran.load(Ordering::SeqCst), 1);
        assert_eq!(b_ran.load(Ordering::SeqCst), 1, "the tier stopped at the first failure");
        assert_eq!(c_ran.load(Ordering::SeqCst), 1, "a passing check after a failure was skipped");
        assert_eq!(out.findings().len(), 2, "the agent would learn one defect per round trip");
    }

    /// Rule 2, the half that saves work.
    #[test]
    fn a_later_tier_does_not_run_once_an_earlier_one_has_fired() {
        // BlastRadius sorts first, Invariants last.
        let (early, early_ran) = probe("early", Tier::BlastRadius, CheckStatus::Heuristic, true);
        let (late, late_ran) = probe("late", Tier::Invariants, CheckStatus::SoundAndCrisp, true);
        let out = VerificationGate::new().with(late).with(early).run();

        assert_eq!(early_ran.load(Ordering::SeqCst), 1);
        assert_eq!(
            late_ran.load(Ordering::SeqCst),
            0,
            "the expensive tier ran anyway; short-circuiting between tiers is what makes the \
             ordering rule worth anything"
        );
        assert!(matches!(out, GateOutcome::Quarantine(_)), "got {}", out.name());
    }

    #[test]
    fn an_empty_gate_and_a_gate_where_nothing_fires_both_pass() {
        assert_eq!(VerificationGate::new().run(), GateOutcome::Pass);
        let (a, _) = probe("a", Tier::Structural, CheckStatus::SoundAndCrisp, false);
        let (b, _) = probe("b", Tier::Invariants, CheckStatus::SoundAndCrisp, false);
        assert_eq!(VerificationGate::new().with(a).with(b).run(), GateOutcome::Pass);
    }

    #[test]
    fn outcome_follows_epistemic_status_not_severity() {
        for (status, expect) in [
            (CheckStatus::SoundAndCrisp, "Retry"),
            (CheckStatus::Heuristic, "Quarantine"),
            (CheckStatus::NotEvaluable, "HardReject"),
        ] {
            let (c, _) = probe("c", Tier::Structural, status, true);
            let out = VerificationGate::new().with(c).run();
            assert_eq!(out.name(), expect, "{status} should give {expect}");
        }
    }

    /// A heuristic firing alongside a sound check must not be able to downgrade the outcome to a
    /// retry, and must not be able to upgrade a retry into a rejection either.
    #[test]
    fn the_most_severe_status_in_a_tier_decides() {
        let (sound, _) = probe("sound", Tier::Structural, CheckStatus::SoundAndCrisp, true);
        let (heur, _) = probe("heur", Tier::Structural, CheckStatus::Heuristic, true);
        let out = VerificationGate::new().with(sound).with(heur).run();
        assert!(matches!(out, GateOutcome::Quarantine(_)), "got {}", out.name());
        assert_eq!(out.findings().len(), 2, "both findings must reach the caller");

        let (heur2, _) = probe("heur", Tier::Structural, CheckStatus::Heuristic, true);
        let (dead, _) = probe("dead", Tier::Structural, CheckStatus::NotEvaluable, true);
        let out = VerificationGate::new().with(heur2).with(dead).run();
        assert!(
            matches!(out, GateOutcome::HardReject(_)),
            "a check that could not be evaluated must outrank a heuristic; got {}",
            out.name()
        );
    }

    /// The gate composed with a real check, not a probe: D2's metric must arrive as a
    /// quarantine and never as a rejection, and that has to follow from its status rather than
    /// from a caller choosing correctly.
    #[test]
    fn the_blind_write_metric_quarantines_and_cannot_reject() {
        let none = BlindWriteCheck::new(Vec::new());
        assert_eq!(VerificationGate::new().with(Box::new(none)).run(), GateOutcome::Pass);

        let some = BlindWriteCheck::new(vec![(TableId(1), RowId(7)), (TableId(1), RowId(9))]);
        let out = VerificationGate::new().with(Box::new(some)).run();
        assert!(
            matches!(out, GateOutcome::Quarantine(_)),
            "a heuristic produced {}, which lets a guess block a merge",
            out.name()
        );
        let f = &out.findings()[0];
        assert_eq!(f.tier, Tier::BlastRadius);
        assert_eq!(f.status, CheckStatus::Heuristic);
        assert!(f.detail.contains("t1:r7"), "the finding must name the rows: {f:?}");
        assert!(f.detail.contains("t1:r9"), "the finding must name every row: {f:?}");
    }

    fn assertion(rows_checked: usize, violations: Vec<(RowId, &str)>) -> AssertionResult {
        AssertionResult {
            source: "qty >= 0".into(),
            table: "inventory".into(),
            tbl: TableId(1),
            rows_checked,
            violations: violations.into_iter().map(|(r, s)| (r, s.to_string())).collect(),
            unevaluable: Vec::new(),
        }
    }

    /// The declarative assertion, both halves.
    ///
    /// **Breaking shape:** an assertion that is false in the merged state — `qty >= 0` over a
    /// composed value of −4, which is exactly the bounded-counter case DESIGN.md section 3 says a
    /// *guard* cannot catch, because a precondition sees the pre-op image. If `evaluate` reported
    /// `None` for a violated assertion the merge would be admitted and the invariant broken with a
    /// `Clean` verdict.
    #[test]
    fn an_assertion_fires_when_it_is_false_and_stays_quiet_when_it_holds() {
        // The half that must stay quiet: three rows examined, none violated.
        let held = AssertionCheck::new(assertion(3, Vec::new()));
        assert_eq!(held.status(), CheckStatus::SoundAndCrisp);
        assert_eq!(held.evaluate(), None, "an assertion that holds must not fire");
        assert_eq!(VerificationGate::new().with(Box::new(held)).run(), GateOutcome::Pass);

        // The half that must fire, with the predicate handed back.
        let broken = AssertionCheck::new(assertion(3, vec![(RowId(1), "qty = -4")]));
        let out = VerificationGate::new().with(Box::new(broken)).run();
        assert!(
            matches!(out, GateOutcome::Retry(_)),
            "a sound assertion must demand a retry, got {}",
            out.name()
        );
        let f = &out.findings()[0];
        assert_eq!(f.tier, Tier::Invariants);
        assert!(f.detail.contains("qty >= 0"), "the predicate was not handed back: {f:?}");
        assert!(f.detail.contains("qty = -4"), "the failing image was not named: {f:?}");
        assert!(f.detail.contains("r1"), "the failing row was not named: {f:?}");
    }

    /// **Breaking shape:** a simulation whose assertion names a table the candidates never wrote a
    /// row to — or an empty table. Every predicate then holds over the empty set, every candidate
    /// scores perfectly, and the gate admits work nothing checked. Zero rows examined is a fact
    /// about the scope of the check, so it is reported as "could not be evaluated" and hard-rejects.
    #[test]
    fn an_assertion_that_examined_no_rows_is_a_hard_reject_not_a_pass() {
        let empty = AssertionCheck::new(assertion(0, Vec::new()));
        assert_eq!(empty.status(), CheckStatus::NotEvaluable);
        let detail = empty.evaluate().expect("an assertion over zero rows must fire");
        assert!(detail.contains("0 row"), "the finding must say it ran over nothing: {detail}");

        let out = VerificationGate::new().with(Box::new(empty)).run();
        assert!(
            matches!(out, GateOutcome::HardReject(_)),
            "an assertion that never ran was reported as {}",
            out.name()
        );
    }

    /// **Breaking shape:** one row of the asserted table whose cell the merge removed, alongside a
    /// hundred rows where the predicate held. Counting only violations would call that a pass; not
    /// knowing whether a merge is safe is worse than knowing it is not, so it hard-rejects.
    #[test]
    fn a_row_that_could_not_be_evaluated_outranks_every_row_that_held() {
        let mut r = assertion(100, Vec::new());
        r.unevaluable.push((RowId(7), "no such cell tbl1.qty[row7]".into()));
        let check = AssertionCheck::new(r);
        assert_eq!(check.status(), CheckStatus::NotEvaluable);
        let out = VerificationGate::new().with(Box::new(check)).run();
        assert!(
            matches!(out, GateOutcome::HardReject(_)),
            "an unevaluable row produced {}",
            out.name()
        );
        assert!(out.findings()[0].detail.contains("could not be evaluated"));
    }

    /// Rule 2 applied to assertions: N broken assertions must cost ONE round trip, not N.
    ///
    /// **Breaking shape:** a caller declaring three assertions, two of which the candidate breaks.
    /// One check holding all three would report a single finding and the caller would fix one,
    /// re-run the whole simulation, and learn the next.
    #[test]
    fn every_broken_assertion_reaches_the_caller_in_one_round_trip() {
        let mut a = assertion(2, vec![(RowId(1), "qty = -1")]);
        a.source = "qty >= 0".into();
        let mut b = assertion(2, vec![(RowId(2), "price = 0")]);
        b.source = "price > 0".into();
        let held = assertion(2, Vec::new());

        let out = VerificationGate::new()
            .with(Box::new(AssertionCheck::new(a)))
            .with(Box::new(AssertionCheck::new(b)))
            .with(Box::new(AssertionCheck::new(held)))
            .run();

        assert_eq!(out.findings().len(), 2, "the caller would learn one violation per retry");
        let all = out.findings().iter().map(|f| f.detail.clone()).collect::<Vec<_>>().join(" | ");
        assert!(all.contains("qty >= 0") && all.contains("price > 0"), "got {all}");
    }

    /// A sound check hands back what it violated — that is the difference between a retry the
    /// agent can act on and one it can only guess at.
    #[test]
    fn a_sound_finding_carries_the_violated_predicate() {
        let (c, _) = probe("qty floor", Tier::Invariants, CheckStatus::SoundAndCrisp, true);
        let out = VerificationGate::new().with(c).run();
        let f = &out.findings()[0];
        assert_eq!(f.check, "qty floor");
        assert!(f.detail.contains("fired"), "no detail handed back: {f:?}");
        assert_eq!(f.tier, Tier::Invariants);
    }
}
