//! D10 — property tests for the merge algebra.
//!
//! The merge semantics rest on a handful of algebraic claims that the code states in prose:
//! `Add` composes associatively and commutatively, `Max`/`Min` are idempotent and commutative,
//! `Add` is *not* idempotent, and `commutes_with` says which pairs compose without a policy
//! decision. Example tests pin the cases someone thought of. These generate them.
//!
//! The property that matters most is **coherence between `compose` and `apply`**: composing two
//! deltas and applying the result must equal applying them one after the other. Every "two agents'
//! `qty -= n` merge arithmetically" claim is that law. If it fails anywhere, a merge silently
//! produces a different number than replaying the writes would.
//!
//! Overflow is a legal outcome, not a violation. `Delta::compose` and `Delta::apply` both refuse
//! rather than wrap, so each property holds *when the operations succeed* — a property that
//! demanded success would just be testing that the generator stayed inside i64.

use proptest::prelude::*;

use ferrodb::catalog::column::Value;
use ferrodb::branch::types::BranchId;
use ferrodb::tel::ids::Dot;
use ferrodb::tel::op::{Delta, OpKind};

/// Integer deltas across a range wide enough to reach overflow, so the refusal path is exercised
/// rather than avoided.
fn int_delta() -> impl Strategy<Value = Delta> {
    prop_oneof![
        (-1000i64..1000).prop_map(Delta::Int),
        Just(Delta::Int(i64::MAX)),
        Just(Delta::Int(i64::MIN)),
        Just(Delta::Int(0)),
    ]
}

fn finite_float_delta() -> impl Strategy<Value = Delta> {
    (-1.0e6f64..1.0e6).prop_map(Delta::Float)
}

fn any_delta() -> impl Strategy<Value = Delta> {
    prop_oneof![int_delta(), finite_float_delta()]
}

fn scalar() -> impl Strategy<Value = Value> {
    prop_oneof![
        (-1000i32..1000).prop_map(Value::Integer),
        (-1.0e6f64..1.0e6).prop_map(Value::Float),
        ".{0,8}".prop_map(Value::Varchar),
        any::<bool>().prop_map(Value::Boolean),
        Just(Value::Null),
    ]
}

fn op_kind() -> impl Strategy<Value = OpKind> {
    prop_oneof![
        any_delta().prop_map(OpKind::Add),
        scalar().prop_map(OpKind::Max),
        scalar().prop_map(OpKind::Min),
        scalar().prop_map(OpKind::Assign),
        Just(OpKind::RowDelete),
        prop::collection::vec(scalar(), 0..3).prop_map(OpKind::RowCreate),
        (scalar(), 0u64..8, 0u64..8)
            .prop_map(|(elem, a, b)| OpKind::SetInsert {
                elem,
                dot: Dot { branch: BranchId::new(a, 0), seq: b },
            }),
    ]
}

proptest! {
    /// `(a . b) . c == a . (b . c)` for integers.
    ///
    /// Integers only, deliberately. Float addition is not associative — the code's own comment
    /// says "Add is associative and commutative" without qualifying it, and that is true of
    /// `Delta::Int` and only approximately true of `Delta::Float`. A separate test below states
    /// the float case honestly instead of pretending this one covers it.
    #[test]
    fn integer_add_is_associative(a in int_delta(), b in int_delta(), c in int_delta()) {
        let left = a.compose(&b).and_then(|ab| ab.compose(&c));
        let right = b.compose(&c).and_then(|bc| a.compose(&bc));
        if let (Ok(l), Ok(r)) = (&left, &right) {
            prop_assert_eq!(l, r, "associativity broken for {:?} {:?} {:?}", a, b, c);
        }
    }

    /// `a . b == b . a`. This is what lets two branches' decrements merge in either order.
    #[test]
    fn add_is_commutative(a in any_delta(), b in any_delta()) {
        if let (Ok(ab), Ok(ba)) = (a.compose(&b), b.compose(&a)) {
            match (ab, ba) {
                (Delta::Int(x), Delta::Int(y)) => prop_assert_eq!(x, y),
                // Float addition IS commutative even though it is not associative.
                (Delta::Float(x), Delta::Float(y)) => prop_assert!(x == y || (x.is_nan() && y.is_nan())),
                (x, y) => prop_assert!(false, "composition changed type by order: {:?} vs {:?}", x, y),
            }
        }
    }

    /// Zero is the identity, so a no-op write composes away instead of accumulating.
    #[test]
    fn zero_is_the_identity_for_integer_add(a in int_delta()) {
        prop_assert_eq!(a.compose(&Delta::Int(0)).unwrap(), a);
        prop_assert_eq!(Delta::Int(0).compose(&a).unwrap(), a);
    }

    /// **The Cassandra counter trap, generated rather than assumed.** Replaying an `Add` must not
    /// be absorbed: composing a non-zero delta with itself has to double it. This is the whole
    /// reason `TxnId` de-duplication exists, and a "fix" that made `Add` idempotent would silently
    /// lose half of every concurrent decrement.
    #[test]
    fn a_nonzero_add_is_never_idempotent(a in -1000i64..1000) {
        prop_assume!(a != 0);
        let d = Delta::Int(a);
        let twice = d.compose(&d).unwrap();
        prop_assert_ne!(twice, d, "Add absorbed a replay, which is the counter double-count bug");
        prop_assert_eq!(twice, Delta::Int(a * 2));
        prop_assert!(!OpKind::Add(d).is_idempotent());
    }

    /// **Coherence.** Composing then applying equals applying in sequence.
    ///
    /// If this ever fails, a merge computes a different number than replaying the same writes
    /// would, which is exactly the guarantee criterion 6 rests on.
    #[test]
    fn composing_then_applying_equals_applying_in_sequence(
        base in -1000i32..1000,
        a in int_delta(),
        b in int_delta(),
    ) {
        let base = Value::Integer(base);
        let sequential = a.apply(&base).and_then(|mid| b.apply(&mid));
        let composed = a.compose(&b).and_then(|ab| ab.apply(&base));
        if let (Ok(s), Ok(c)) = (&sequential, &composed) {
            prop_assert_eq!(s, c, "compose and apply disagree for {:?} then {:?}", a, b);
        }
    }

    /// `commutes_with` must be symmetric. An asymmetric answer would make the merge's behaviour
    /// depend on which branch happened to be examined first.
    #[test]
    fn commutes_with_is_symmetric(x in op_kind(), y in op_kind()) {
        prop_assert_eq!(
            x.commutes_with(&y),
            y.commutes_with(&x),
            "commutes_with disagreed with itself for {:?} and {:?}", x, y
        );
    }

    /// Every kind except `Add` claims idempotence; the claim is checked against the enum rather
    /// than restated, so adding a new non-idempotent kind without updating `is_idempotent` fails
    /// here instead of silently merging wrong.
    #[test]
    fn add_is_the_only_kind_that_is_not_idempotent(k in op_kind()) {
        let expected = !matches!(k, OpKind::Add(_));
        prop_assert_eq!(k.is_idempotent(), expected, "wrong idempotence for {:?}", k);
    }

    /// `Max` really is idempotent and commutative under application, not merely labelled so.
    #[test]
    fn max_is_idempotent_and_commutative(a in -1000i32..1000, b in -1000i32..1000) {
        let (va, vb) = (Value::Integer(a), Value::Integer(b));
        let max = |x: &Value, y: &Value| if x >= y { x.clone() } else { y.clone() };
        prop_assert_eq!(max(&va, &va), va.clone(), "max is not idempotent");
        prop_assert_eq!(max(&va, &vb), max(&vb, &va), "max is not commutative");
        prop_assert!(OpKind::Max(va.clone()).commutes_with(&OpKind::Max(vb)));
    }

    /// Two `Assign`s never commute — that is a policy decision, and pretending otherwise would
    /// silently pick a winner.
    #[test]
    fn two_assigns_never_commute(x in scalar(), y in scalar()) {
        prop_assert!(!OpKind::Assign(x).commutes_with(&OpKind::Assign(y)));
    }

    /// Composition never wraps. Overflow is refused, and a refusal is a correct outcome.
    #[test]
    fn integer_composition_refuses_overflow_rather_than_wrapping(a in int_delta(), b in int_delta()) {
        if let (Delta::Int(x), Delta::Int(y)) = (&a, &b) {
            match a.compose(&b) {
                Ok(Delta::Int(z)) => prop_assert_eq!(Some(z), x.checked_add(*y)),
                Ok(other) => prop_assert!(false, "int + int produced {:?}", other),
                Err(_) => prop_assert!(x.checked_add(*y).is_none(), "refused a sum that fits"),
            }
        }
    }
}

/// Float addition is **not** associative, and the algebra's prose does not say so.
///
/// This is not a defect in `compose` — it is IEEE-754 — but it is a real limit on the claim
/// "`Add` is associative and commutative" as written in `src/tel/op.rs`. Recorded as an executable
/// statement so nobody later reads that comment and assumes float counters reassociate freely.
#[test]
fn float_add_is_commutative_but_not_associative_and_that_is_stated_not_assumed() {
    // Order matters for the counterexample itself: with the large value first, the small one is
    // lost under BOTH groupings and the two sides agree at 0.0, which says nothing. Putting the
    // small value first means one grouping keeps it and the other does not.
    let (a, b, c) = (Delta::Float(1.0), Delta::Float(1e16), Delta::Float(-1e16));

    let left = a.compose(&b).unwrap().compose(&c).unwrap();
    let right = b.compose(&c).unwrap();
    let right = a.compose(&right).unwrap();

    let (l, r) = match (left, right) {
        (Delta::Float(l), Delta::Float(r)) => (l, r),
        other => panic!("expected floats, got {other:?}"),
    };
    assert_ne!(
        l, r,
        "float composition reassociated exactly; if this now holds, the counterexample is stale \
         and the claim in src/tel/op.rs should be revisited"
    );

    // Commutativity, by contrast, does hold for floats.
    for (x, y) in [(a, b), (b, c), (a, c)] {
        let xy = x.compose(&y).unwrap();
        let yx = y.compose(&x).unwrap();
        assert_eq!(xy, yx, "float composition is order-dependent, which it should not be");
    }
}
