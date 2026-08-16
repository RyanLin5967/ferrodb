package main

import (
	"math"
	"strconv"
	"strings"
	"testing"
)

// The `precision` subcommand is a DETECTOR, and a detector is only worth its output if it is right
// in both directions. These tests pin both: it must fire on digits a float64 genuinely destroys,
// and it must stay silent on digits a float64 carries perfectly. A checker that cries LOSSY on
// `0.1` teaches a consumer to ignore it, which costs more than having no checker at all.

// A FLOAT column ships as a bare JSON number on purpose — f64 is exactly what a float64 holds, so
// nothing is lost and nothing should be reported.
//
// Rust's `value_into` prints a float with `{}`, which is Rust's shortest round-trip form: the
// fewest digits that parse back to the identical double. `0.1` is therefore not "the wire being
// sloppy about 1/10" — it is the exact NAME of the double that was sent, and the consumer that
// parses it holds bit-for-bit the value the producer held.
//
// Comparing the wire text to the double as exact rationals gets this backwards: 1/10 is not
// representable in binary, so the exact-rational test reports a mismatch for every finite decimal
// that is not a dyadic rational — which is nearly all of them.
func TestSameNumberAcceptsAFloatThatSurvivedItsRoundTrip(t *testing.T) {
	cases := []struct {
		wire string
		held float64
	}{
		{"0.1", 0.1},
		{"-0.1", -0.1},
		{"0.2", 0.2},
		{"0.3", 0.3},
		{"1.1", 1.1},
		{"3.14159", 3.14159},
		{"2.675", 2.675},
		// Trailing zeros are formatting, not precision: `1.50` and the double 1.5 are one number.
		{"1.50", 1.5},
		{"1.5", 1.5},
		{"2", 2.0},
		{"0", 0.0},
		{"-0", math.Copysign(0, -1)},
		// Values that need every one of a double's 17 significant digits still round-trip.
		{"1.7976931348623157e+308", math.MaxFloat64},
		{"5e-324", math.SmallestNonzeroFloat64},
	}
	for _, c := range cases {
		// Guard the fixture itself: if the text does not parse to the double, the case is
		// mislabelled and would be testing something other than what it claims.
		if got, err := strconv.ParseFloat(c.wire, 64); err != nil || got != c.held {
			t.Fatalf("fixture is wrong: ParseFloat(%q) = %v, %v; want %v", c.wire, got, err, c.held)
		}
		if !sameNumber(c.wire, c.held) {
			t.Errorf("sameNumber(%q, %v) = false; a float64 holds this text exactly, so reporting "+
				"it LOSSY is a false alarm", c.wire, c.held)
		}
	}
}

// The other direction, and the reason the subcommand exists: digits that no double can hold. These
// must still be caught, or the fix above would have bought silence by disabling the detector.
func TestSameNumberStillCatchesDigitsNoDoubleCanHold(t *testing.T) {
	cases := []struct {
		wire string
		held float64
		why  string
	}{
		{"9223372036854775807", 9223372036854775808.0, "i64::MAX comes back one larger"},
		{"-9223372036854775808", -9223372036854775808.0, "i64::MIN is representable, see below"},
		{"9007199254740993", 9007199254740992.0, "2^53+1 collapses onto 2^53"},
		{"123456789012345678901234567890.12345678901234567890", 1.2345678901234568e+29,
			"a 50-digit decimal has nowhere near enough room in a double"},
	}
	for _, c := range cases {
		// i64::MIN is exactly -2^63, which IS a double, so it is the one case here that is not a
		// corruption. Keeping it in the table documents that the checker is not simply saying
		// "big number = bad".
		want := c.wire != "-9223372036854775808"
		if got := sameNumber(c.wire, c.held); got == want {
			t.Errorf("sameNumber(%q, %v) = %v; want %v (%s)", c.wire, c.held, got, !want, c.why)
		}
	}
}

// exactFloat's doc says it "renders a float64's exact value, not its shortest round-trip form",
// and gives the reason: understating the corruption makes the report less useful than saying
// nothing. `big.Rat.FloatString(40)` ROUNDS to 40 decimal places, so it does not do that.
//
// Every double below 1e-40 is flattened to the same "0" — including every subnormal, and including
// values that are provably different numbers. A report that prints two different corrupted values
// identically is exactly the understatement the doc forbids.
func TestExactFloatIsExactForSubnormals(t *testing.T) {
	tiny := math.SmallestNonzeroFloat64 // 2^-1074, the smallest positive double
	two := 2 * tiny                     // 2^-1073, a provably different number

	for _, v := range []float64{tiny, two, 3 * tiny} {
		s := exactFloat(v)
		if s == "0" || s == "-0" {
			t.Fatalf("exactFloat(%g) = %q, but the value is not zero; a rounded rendering "+
				"contradicts the function's own doc", v, s)
		}
	}
	if a, b := exactFloat(tiny), exactFloat(two); a == b {
		t.Errorf("exactFloat rendered two different doubles identically:\n  %g -> %s\n  %g -> %s",
			tiny, a, two, b)
	}

	// Exactness is checkable, not just non-zero-ness: 2^-1074 = 5^1074 / 10^1074, so its exact
	// decimal expansion terminates after exactly 1074 fractional digits and ends in 5.
	s := exactFloat(tiny)
	frac, ok := strings.CutPrefix(s, "0.")
	if !ok {
		t.Fatalf("exactFloat(%g) = %q; want a plain 0.xxx expansion", tiny, s)
	}
	if len(frac) != 1074 {
		t.Errorf("exactFloat(2^-1074) has %d fractional digits; the exact expansion has 1074", len(frac))
	}
	if !strings.HasSuffix(frac, "5") {
		t.Errorf("exactFloat(2^-1074) ends in %q; an exact power of 2^-n ends in 5", s[len(s)-1:])
	}
}

// Whatever exactFloat prints must name the double it was given: parsing the rendering back has to
// return the identical bits. This is the property that makes the LOSSY line trustworthy.
func TestExactFloatRoundTripsToTheSameDouble(t *testing.T) {
	for _, v := range []float64{
		0, 1, -1, 0.1, 1.5, 2.675, 1e300, -1e-300,
		math.MaxFloat64, math.SmallestNonzeroFloat64, 9223372036854775808.0, 9007199254740992.0,
	} {
		s := exactFloat(v)
		back, err := strconv.ParseFloat(s, 64)
		if err != nil {
			t.Errorf("exactFloat(%g) = %q, which does not parse as a float: %v", v, s, err)
			continue
		}
		if back != v {
			t.Errorf("exactFloat(%g) = %q, which parses back as %g", v, s, back)
		}
	}
}
