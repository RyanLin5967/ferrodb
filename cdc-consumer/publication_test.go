package main

// B7 — the consumer's half of the publication guard, forced to fire.
//
// The Rust suite proves the producer withholds a denied column. That cannot prove anything about this
// check, because a correct producer never gives it an event to refuse. So every case here hands the
// consumer a feed the producer would not have written — which is exactly the feed a bypassed,
// mis-configured or future producer would write — and each refusal has an anti-vacuity half showing
// the same shape passing when the policy allows it.

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// withPolicy installs a publication for one test and takes it away afterwards, so an ordering change
// in the test binary cannot make one test's policy silently do another's work.
func withPolicy(t *testing.T, decl string) {
	t.Helper()
	p, err := parsePublication(decl)
	if err != nil {
		t.Fatalf("the test's own publication does not parse: %v", err)
	}
	activePublication = p
	t.Cleanup(func() { activePublication = nil })
}

func writePolicyFile(t *testing.T, decl string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "publication.txt")
	if err := os.WriteFile(p, []byte(decl), 0o644); err != nil {
		t.Fatalf("write publication: %v", err)
	}
	return p
}

const policy = "publication analytics\ncustomers: id, name\n"

// A row event carrying a column the policy does not name.
func rowLine(after string) string {
	return `{"table":"customers","op":"INSERT","txn":1,"lsn":10,"commit_lsn":20,` +
		`"commit_end_lsn":21,"before":null,"after":` + after + "}\n"
}

// **The case the producer's own guard would have prevented, which is why it is written by hand.**
//
// Breaking shape: a feed produced with no publication (or by a build predating the guard, or by a
// path that forgot it) landing in a consumer that has one. Without this check the ssn is applied to
// the destination and the run exits 0.
func TestConsumerRefusesADeniedColumnTheProducerLetThrough(t *testing.T) {
	withPolicy(t, policy)
	feed := writeFeed(t, rowLine(`{"id":1,"name":"ada","ssn":"000-11-2222"}`))
	err := validate(feed)
	if err == nil {
		t.Fatal("a feed carrying an unpublished column validated; it has left the database")
	}
	if !strings.Contains(err.Error(), "ssn") || !strings.Contains(err.Error(), "does not publish") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}

	// Anti-vacuity: the same event without the denied column passes, so the refusal is about the
	// column rather than about this consumer refusing everything under a policy.
	if err := validate(writeFeed(t, rowLine(`{"id":1,"name":"ada"}`))); err != nil {
		t.Fatalf("a compliant feed was refused: %v", err)
	}
}

// The BEFORE image is the half a producer projecting only `after` would leak, and a DELETE has
// nothing else.
func TestConsumerChecksTheBeforeImageToo(t *testing.T) {
	withPolicy(t, policy)
	del := `{"table":"customers","op":"DELETE","txn":1,"lsn":10,"commit_lsn":20,` +
		`"commit_end_lsn":21,"before":{"id":1,"ssn":"000-11-2222"},"after":null}` + "\n"
	err := validate(writeFeed(t, del))
	if err == nil {
		t.Fatal("a denied column in a before image validated")
	}
	if !strings.Contains(err.Error(), "before image") || !strings.Contains(err.Error(), "ssn") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}

	ok := `{"table":"customers","op":"DELETE","txn":1,"lsn":10,"commit_lsn":20,` +
		`"commit_end_lsn":21,"before":{"id":1},"after":null}` + "\n"
	if err := validate(writeFeed(t, ok)); err != nil {
		t.Fatalf("a compliant DELETE was refused: %v", err)
	}
}

// **A table the policy has never heard of.** This is the shape an operator produces by creating a
// table after writing the publication, and the reason the rule is an allowlist: a denylist publishes
// the new table in full.
func TestConsumerRefusesAnUndecidedTable(t *testing.T) {
	withPolicy(t, policy)
	line := `{"table":"audit_log","op":"INSERT","txn":1,"lsn":10,"commit_lsn":20,` +
		`"commit_end_lsn":21,"before":null,"after":{"id":1}}` + "\n"
	err := validate(writeFeed(t, line))
	if err == nil {
		t.Fatal("an event for a table outside the publication validated")
	}
	if !strings.Contains(err.Error(), "audit_log") || !strings.Contains(err.Error(), "allowlist") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}

	// Anti-vacuity: the table it does name passes.
	if err := validate(writeFeed(t, rowLine(`{"id":1}`))); err != nil {
		t.Fatalf("a published table was refused: %v", err)
	}
}

// A CREATE_TABLE announcing a denied column is a name and a type leaving the database, and it also
// makes the consumer create a column it will never receive a value for.
func TestConsumerRefusesASchemaEventDeclaringADeniedColumn(t *testing.T) {
	withPolicy(t, policy)
	shape := func(cols string) string {
		return `{"table":"customers","op":"CREATE_TABLE","txn":0,"lsn":10,"commit_lsn":10,` +
			`"commit_end_lsn":11,"before":null,"after":{"columns":[` + cols + `]}}` + "\n"
	}
	denied := `{"name":"id","type":"INTEGER","nullable":false},` +
		`{"name":"ssn","type":"VARCHAR(16)","nullable":true}`
	err := validate(writeFeed(t, shape(denied)))
	if err == nil {
		t.Fatal("a schema event declaring an unpublished column validated")
	}
	if !strings.Contains(err.Error(), "ssn") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}

	// Anti-vacuity: the projected shape passes. Without this the test would also pass against a
	// consumer that refused every CREATE_TABLE — and one did exactly that when CREATE_TABLE was
	// first emitted, so this is not a hypothetical failure mode.
	allowed := `{"name":"id","type":"INTEGER","nullable":false},` +
		`{"name":"name","type":"VARCHAR(32)","nullable":true}`
	if err := validate(writeFeed(t, shape(allowed))); err != nil {
		t.Fatalf("a compliant schema event was refused: %v", err)
	}
}

// **The other direction: a producer running a NARROWER policy than this consumer.**
//
// Breaking shape: the publication amended to publish `name` on the consumer while the producer still
// runs the old one. Nothing leaks, and no other check in this consumer would notice — the destination
// column simply stays null for ever, which reads as missing data rather than as a stale policy. The
// declared shape is the one event that says what the producer believes it may send.
func TestConsumerRefusesAShapeNarrowerThanItsOwnPolicy(t *testing.T) {
	withPolicy(t, policy) // customers: id, name
	narrow := `{"table":"customers","op":"CREATE_TABLE","txn":0,"lsn":10,"commit_lsn":10,` +
		`"commit_end_lsn":11,"before":null,"after":{"columns":[` +
		`{"name":"id","type":"INTEGER","nullable":false}]}}` + "\n"
	err := validate(writeFeed(t, narrow))
	if err == nil {
		t.Fatal("a shape missing a published column validated; that column stays null for ever")
	}
	if !strings.Contains(err.Error(), "name") || !strings.Contains(err.Error(), "narrower") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}

	// Anti-vacuity: the shape that matches the policy passes, so the refusal is about the missing
	// column and not about this consumer refusing every CREATE_TABLE.
	full := `{"table":"customers","op":"CREATE_TABLE","txn":0,"lsn":10,"commit_lsn":10,` +
		`"commit_end_lsn":11,"before":null,"after":{"columns":[` +
		`{"name":"id","type":"INTEGER","nullable":false},` +
		`{"name":"name","type":"VARCHAR(32)","nullable":true}]}}` + "\n"
	if err := validate(writeFeed(t, full)); err != nil {
		t.Fatalf("a shape matching the policy was refused: %v", err)
	}
}

// A DROP carries no shape, so the narrower-producer check must not read one into its null after image
// and refuse the event that tells a consumer to drop its own copy.
func TestADropIsNotJudgedByTheShapeCheck(t *testing.T) {
	withPolicy(t, policy)
	drop := `{"table":"customers","op":"DROP_TABLE","txn":0,"lsn":10,"commit_lsn":10,` +
		`"commit_end_lsn":11,"before":null,"after":null}` + "\n"
	if err := validate(writeFeed(t, drop)); err != nil {
		t.Fatalf("a DROP of a published table was refused: %v", err)
	}
}

// **The blind spot, asserted so it is a decision rather than an oversight.** With no -publication
// flag this consumer enforces nothing, which is what every invocation predating B7 relies on.
func TestWithNoPublicationNothingIsRefused(t *testing.T) {
	if activePublication != nil {
		t.Fatal("a previous test left a policy installed")
	}
	if err := validate(writeFeed(t, rowLine(`{"id":1,"name":"ada","ssn":"000-11-2222"}`))); err != nil {
		t.Fatalf("with no publication the feed must be accepted as it was before B7: %v", err)
	}
}

// usePublication is the only way to turn the check on, so a file it cannot read must be an error
// rather than a quiet run with no policy.
func TestUsePublicationRefusesAFileItCannotRead(t *testing.T) {
	t.Cleanup(func() { activePublication = nil })
	if err := usePublication(filepath.Join(t.TempDir(), "absent.txt")); err == nil {
		t.Fatal("a missing publication file was accepted; the run would enforce nothing")
	}
	if activePublication != nil {
		t.Fatal("a failed load installed a policy anyway")
	}

	// Anti-vacuity: a real file installs it, and an empty path means "no policy" rather than an error.
	if err := usePublication(writePolicyFile(t, policy)); err != nil {
		t.Fatalf("a valid publication file was refused: %v", err)
	}
	if activePublication == nil || activePublication.Name != "analytics" {
		t.Fatalf("the policy was not installed: %+v", activePublication)
	}
	activePublication = nil
	if err := usePublication(""); err != nil || activePublication != nil {
		t.Fatalf("an empty path should mean no policy: err=%v policy=%+v", err, activePublication)
	}
}

// The parser's refusals. Written against the format description; the Rust parser has its own set of
// these, and the point of both is that a disagreement shows up as a refusal rather than as a
// published column.
func TestParsePublicationRefusesEveryMalformedShape(t *testing.T) {
	for _, c := range []struct{ name, decl, want string }{
		// Two ways to have no header, refused by two different guards: a table line reached before
		// any header names the line it is on, and a file with no table lines at all falls through to
		// the check at the end. Both must refuse, or an unrelated file read as a publication becomes
		// an allowlist that publishes nothing and stalls the feed with no clue why.
		{"table before header", "customers: id\n", "before any `publication <name>` header"},
		{"nothing but comments", "# not a publication\n", "no `publication <name>` header"},
		{"no tables", "publication p\n# nothing\n", "publishes no table"},
		{"empty column list", "publication p\ncustomers:\n", "lists no columns"},
		{"table twice", "publication p\nt: id\nt: id, ssn\n", "declared twice"},
		{"column twice", "publication p\nt: id, id\n", "twice"},
		{"unreadable line", "publication p\nt: id\ncustomers id\n", "neither a comment nor"},
		{"empty between commas", "publication p\nt: id,,qty\n", "empty column between commas"},
		{"header with no name", "publication \nt: id\n", "header has no name"},
		{"bare header word", "publication\nt: id\n", "header has no name"},
		// **The widening both parsers had.** `customers: id, ssn#hash` truncated to `customers: id,
		// ssn` and published `ssn`. Refused now, by name, in the consumer as well as the producer -
		// and the point of it being refused HERE too is that a producer running an older parser
		// cannot hand this consumer a feed built from the widened set.
		{"name containing the comment character", "publication p\ncustomers: id, ssn#hash\n", "not an identifier"},
		{"non-identifier column", "publication p\nt: id, qty-2\n", "not an identifier"},
		{"non-identifier table", "publication p\nt able: id\n", "not an identifier"},
		{"two headers", "publication a\npublication b\nt: id\n", "second `publication` header"},
	} {
		_, err := parsePublication(c.decl)
		if err == nil {
			t.Fatalf("%s: accepted", c.name)
		}
		if !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: refused, but not by the expected guard: %v", c.name, err)
		}
	}

	// Anti-vacuity: the well-formed declaration parses, and to exactly what it names.
	p, err := parsePublication("publication analytics\n# c\ncustomers: id, name\n\norders: id\n")
	if err != nil {
		t.Fatalf("a well-formed declaration was refused: %v", err)
	}
	if p.Name != "analytics" || len(p.Tables) != 2 {
		t.Fatalf("parsed to the wrong thing: %+v", p)
	}
	if !p.Tables["customers"]["id"] || !p.Tables["customers"]["name"] || p.Tables["customers"]["ssn"] {
		t.Fatalf("customers parsed to the wrong column set: %+v", p.Tables["customers"])
	}
}

// The sink is the mode that writes to a destination, so the guard has to be in its path too — the
// check lives in decodeLine precisely so no mode can be added without it.
func TestTheSinkRefusesADeniedColumnBeforeLandingIt(t *testing.T) {
	withPolicy(t, policy)
	db := filepath.Join(t.TempDir(), "out.sqlite")
	err := runSink(writeFeed(t, rowLine(`{"id":1,"name":"ada","ssn":"000-11-2222"}`)), db, "id", "sqlite")
	if err == nil {
		t.Fatal("the sink landed a row carrying an unpublished column")
	}
	if !strings.Contains(err.Error(), "ssn") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}

	// Anti-vacuity: the compliant feed lands.
	db2 := filepath.Join(t.TempDir(), "ok.sqlite")
	if err := runSink(writeFeed(t, rowLine(`{"id":1,"name":"ada"}`)), db2, "id", "sqlite"); err != nil {
		t.Fatalf("a compliant feed was refused by the sink: %v", err)
	}
}

// **The mode that prints column names and values, which does not go through decodeLine.**
//
// Breaking shape: `precision leaky.jsonl -publication p.txt`. `precision` decodes each line itself —
// twice, once loosely and once exactly — and prints `FIELD <line> <col> <type> <value>` for every
// column it finds. With the guard wired only into decodeLine it printed `FIELD 3 ssn string
// 000-11-2222` and exited 0: the denied column's name and value, on stdout, from the mode whose whole
// job is to describe columns. Found by an adversarial review of the wiring, not by writing it.
func TestPrecisionRefusesADeniedColumnRatherThanPrintingIt(t *testing.T) {
	withPolicy(t, policy)
	err := precision(writeFeed(t, rowLine(`{"id":1,"name":"ada","ssn":"000-11-2222"}`)))
	if err == nil {
		t.Fatal("precision reported on a column the policy denies; its name and value went to stdout")
	}
	if !strings.Contains(err.Error(), "ssn") || !strings.Contains(err.Error(), "type report") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}

	// Anti-vacuity: the compliant feed still gets a report. Without this the test would pass against a
	// precision that refused everything, which would make the detector useless rather than safe.
	if err := precision(writeFeed(t, rowLine(`{"id":1,"name":"ada"}`))); err != nil {
		t.Fatalf("precision refused a compliant feed: %v", err)
	}

	// And with no policy it reports everything, as it did before B7.
	activePublication = nil
	if err := precision(writeFeed(t, rowLine(`{"id":1,"name":"ada","ssn":"000-11-2222"}`))); err != nil {
		t.Fatalf("with no publication, precision must behave as it did before B7: %v", err)
	}
}

// An undecided table refuses in the type report too, for the same reason it refuses in every other
// mode: the policy has not been told about it.
func TestPrecisionRefusesAnUndecidedTable(t *testing.T) {
	withPolicy(t, policy)
	line := `{"table":"audit_log","op":"INSERT","txn":1,"lsn":10,"commit_lsn":20,` +
		`"commit_end_lsn":21,"before":null,"after":{"id":1}}` + "\n"
	err := precision(writeFeed(t, line))
	if err == nil {
		t.Fatal("precision reported on a table outside the publication")
	}
	if !strings.Contains(err.Error(), "audit_log") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}
}

// **A DROP_TABLE's before image was exempt from the whole rule.**
//
// Breaking shape: `{"op":"DROP_TABLE",...,"before":{"id":1,"ssn":"111-22-3333"},"after":null}`. The
// check skipped the row loop for any schema op and looked only inside `after["columns"]`, which a drop
// does not have — so a whole row of denied columns validated with OK 1 and exit 0. A producer never
// writes a before image on a drop, which is exactly why this is the shape worth policing: this check
// exists for feeds the producer should not have written. Found by an adversarial review.
func TestConsumerChecksADropsBeforeImage(t *testing.T) {
	withPolicy(t, policy)
	leak := `{"table":"customers","op":"DROP_TABLE","txn":1,"lsn":10,"commit_lsn":10,` +
		`"commit_end_lsn":11,"before":{"id":1,"ssn":"111-22-3333"},"after":null}` + "\n"
	err := validate(writeFeed(t, leak))
	if err == nil {
		t.Fatal("a DROP carrying a row of denied columns validated")
	}
	if !strings.Contains(err.Error(), "ssn") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}

	// Anti-vacuity: an ordinary DROP still passes. Refusing every drop would stop a consumer ever
	// learning that a table is gone.
	ok := `{"table":"customers","op":"DROP_TABLE","txn":1,"lsn":10,"commit_lsn":10,` +
		`"commit_end_lsn":11,"before":null,"after":null}` + "\n"
	if err := validate(writeFeed(t, ok)); err != nil {
		t.Fatalf("an ordinary DROP was refused: %v", err)
	}
}

// **Duplicate keys: the leak that survives the JSON parser.**
//
// Breaking shape: `{"id":1,"name":"ada","name":"000-11-2222"}` from a table declared
// `CREATE TABLE t (id, name, name)`, which this database accepts. `encoding/json` keeps the LAST
// duplicate, so the denied position's value is the one that reaches the destination while the published
// one is destroyed — an adversarial review followed it into a SQLite row. Only a token walk over the
// raw bytes can see two keys where the struct has one.
func TestConsumerRefusesDuplicateKeysInOneObject(t *testing.T) {
	withPolicy(t, policy)
	dup := rowLine(`{"id":1,"name":"ada","name":"000-11-2222"}`)
	err := validate(writeFeed(t, dup))
	if err == nil {
		t.Fatal("a line with two keys of one name validated; the surviving value may be the denied one")
	}
	if !strings.Contains(err.Error(), "twice") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}

	// Anti-vacuity: the same key in DIFFERENT objects is not a duplicate.
	fine := `{"table":"customers","op":"UPDATE","txn":1,"lsn":10,"commit_lsn":20,` +
		`"commit_end_lsn":21,"before":{"id":1,"name":"ada"},"after":{"id":1,"name":"grace"}}` + "\n"
	if err := validate(writeFeed(t, fine)); err != nil {
		t.Fatalf("an UPDATE with the same column in both images was refused: %v", err)
	}
}

// A key beside the envelope's own, at the top level or beside a schema payload, is where a denied
// column travels when the checks only walk `before` and `after`.
func TestConsumerRefusesKeysOutsideTheEnvelope(t *testing.T) {
	withPolicy(t, policy)
	for _, c := range []struct{ name, line string }{
		{"top-level", `{"table":"customers","op":"INSERT","txn":1,"lsn":10,"commit_lsn":20,` +
			`"commit_end_lsn":21,"before":null,"after":{"id":1},"ssn":"111-22-3333"}` + "\n"},
		{"beside the schema payload", `{"table":"customers","op":"CREATE_TABLE","txn":0,"lsn":10,` +
			`"commit_lsn":10,"commit_end_lsn":11,"before":null,"after":{"columns":[` +
			`{"name":"id","type":"INTEGER","nullable":false}],"ssn":"111-22-3333"}}` + "\n"},
	} {
		err := validate(writeFeed(t, c.line))
		if err == nil {
			t.Fatalf("%s: a key outside the envelope validated", c.name)
		}
		if !strings.Contains(err.Error(), "ssn") {
			t.Fatalf("%s: refused, but not by this guard: %v", c.name, err)
		}
	}

	// Anti-vacuity: the envelope's own keys, and a real schema payload, still pass.
	shape := `{"table":"customers","op":"CREATE_TABLE","txn":0,"lsn":10,"commit_lsn":10,` +
		`"commit_end_lsn":11,"before":null,"after":{"columns":[` +
		`{"name":"id","type":"INTEGER","nullable":false},` +
		`{"name":"name","type":"VARCHAR(32)","nullable":true}]}}` + "\n"
	if err := validate(writeFeed(t, shape)); err != nil {
		t.Fatalf("a well-formed schema event was refused: %v", err)
	}
}

// A row value that is an object or an array is a place to hide a denied column under a published key.
// Every value a row of this database can carry is a scalar.
func TestConsumerRefusesANonScalarRowValue(t *testing.T) {
	withPolicy(t, policy)
	for _, body := range []string{
		`{"id":1,"name":{"name":"ada","ssn":"111-22-3333"}}`,
		`{"id":1,"name":["ada","111-22-3333"]}`,
	} {
		err := validate(writeFeed(t, rowLine(body)))
		if err == nil {
			t.Fatalf("%s: a nested container under a published key validated", body)
		}
		if !strings.Contains(err.Error(), "scalar") {
			t.Fatalf("%s: refused, but not by this guard: %v", body, err)
		}
	}

	// Anti-vacuity: scalars of every JSON type a feed uses still pass.
	if err := validate(writeFeed(t, rowLine(`{"id":1,"name":null}`))); err != nil {
		t.Fatalf("a null column was refused: %v", err)
	}
	if err := validate(writeFeed(t, rowLine(`{"id":1,"name":"ada"}`))); err != nil {
		t.Fatalf("a string column was refused: %v", err)
	}
}

// `exclude` on the consumer: the producer drops these events, so one arriving here means the two ends
// are running different policies — which is a refusal, not a silent drop.
func TestConsumerRefusesAnEventForAnExcludedTable(t *testing.T) {
	withPolicy(t, "publication analytics\ncustomers: id, name\nexclude audit_log\n")
	line := `{"table":"audit_log","op":"INSERT","txn":1,"lsn":10,"commit_lsn":20,` +
		`"commit_end_lsn":21,"before":null,"after":{"id":1}}` + "\n"
	err := validate(writeFeed(t, line))
	if err == nil {
		t.Fatal("an event for an excluded table validated")
	}
	if !strings.Contains(err.Error(), "EXCLUDES") || !strings.Contains(err.Error(), "audit_log") {
		t.Fatalf("refused, but not by this guard: %v", err)
	}
	// And the message must distinguish it from a table nobody decided about, because the two send an
	// operator to different lines of the file.
	if strings.Contains(err.Error(), "undecided") {
		t.Fatalf("an excluded table was reported as undecided: %v", err)
	}

	// Anti-vacuity: the published table still passes under the same policy.
	if err := validate(writeFeed(t, rowLine(`{"id":1,"name":"ada"}`))); err != nil {
		t.Fatalf("a published table was refused: %v", err)
	}
}

// The parser's own handling of the new form.
func TestParsePublicationHandlesExclusions(t *testing.T) {
	p, err := parsePublication("publication analytics\ncustomers: id\nexclude audit_log\n")
	if err != nil {
		t.Fatalf("a declaration with an exclusion was refused: %v", err)
	}
	if !p.Excluded["audit_log"] || p.Excluded["customers"] {
		t.Fatalf("the exclusion was recorded wrongly: %+v", p)
	}
	for _, c := range []struct{ name, decl, want string }{
		{"both ways round", "publication p\nt: id\nexclude t\n", "both published and excluded"},
		{"reverse order", "publication p\nexclude t\nt: id\n", "both excluded and published"},
		{"twice", "publication p\nt: id\nexclude u\nexclude u\n", "excluded twice"},
		{"no name", "publication p\nt: id\nexclude\n", "no table name"},
		{"exclusions only", "publication p\nexclude t\n", "publishes no table"},
		{"before the header", "exclude t\npublication p\nt: id\n", "before any"},
	} {
		if _, err := parsePublication(c.decl); err == nil {
			t.Fatalf("%s: accepted", c.name)
		} else if !strings.Contains(err.Error(), c.want) {
			t.Fatalf("%s: refused, but not by the expected guard: %v", c.name, err)
		}
	}
}

// **I11 — a writer-bearing event must survive a publication, and it did not.**
//
// Breaking shape: B5 added a top-level `writer` object to the feed. B7 added `envelopeKeys`, an
// allowlist of the feed's top-level keys, and `policeRawLine` refuses an unknown one by name. The two
// lanes were built in parallel against the same base, so neither suite ever saw the other's change:
// once both are merged, EVERY attributed event fails the whole line under ANY publication, and
// `validate`, `sink`, `diff` and `follow` all land nothing.
//
// The lanes did not catch this because B5's tests pass no publication (so `activePublication` is nil
// and nothing is enforced) and B7's tests emit no writer.
func TestAnAttributedEventSurvivesAPublication(t *testing.T) {
	withPolicy(t, policy)
	w, err := json.Marshal(writerJSON(3, "restock-agent", "run-123", "claude-opus", "2026-09"))
	if err != nil {
		t.Fatalf("the test's own writer does not marshal: %v", err)
	}
	line := `{"table":"customers","op":"INSERT","txn":1,"lsn":10,"commit_lsn":20,` +
		`"commit_end_lsn":21,"writer":` + string(w) + `,"before":null,"after":{"id":1,"name":"ada"}}`
	if _, err := decodeLine(line, 1); err != nil {
		t.Fatalf("an attributed event was refused under a publication that publishes every column "+
			"it carries: %v", err)
	}
}

// Anti-vacuity: admitting `writer` must not have opened the envelope to anything else. If this stops
// failing, the allowlist has become a denylist and the guard is gone.
func TestAnUnknownTopLevelKeyIsStillRefused(t *testing.T) {
	withPolicy(t, policy)
	line := `{"table":"customers","op":"INSERT","txn":1,"lsn":10,"commit_lsn":20,` +
		`"commit_end_lsn":21,"smuggled":{"ssn":"111-22-3333"},"before":null,` +
		`"after":{"id":1,"name":"ada"}}`
	if _, err := decodeLine(line, 1); err == nil {
		t.Fatal("an unknown top-level key was accepted; a denied column can travel in it")
	}
}
