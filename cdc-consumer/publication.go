package main

// B7 — the publication allowlist, checked here a second time and on purpose.
//
// The database refuses to render a column its publication does not name, at the one function that
// turns a row into bytes. This file does not trust that. It reads the same declaration and refuses a
// feed that carries anything the policy does not allow, which is worth doing for the same reason
// this whole program exists: a producer validated only by its own author's idea of the rule agrees
// with itself about any misreading of that rule.
//
// It matters more here than anywhere else in this file. Every other check in this consumer catches a
// feed that is missing something — a truncated file, a dropped row, an unordered cursor — and the
// fix for all of those is to read the log again. A column that has already left the database cannot
// be recalled from the warehouse, the webhook and the log aggregator it has reached, so the check
// that would have prevented it is worth writing twice.
//
// **This implementation shares nothing with the Rust one.** Same file format, separately written
// parser and separately written rule, so a defect in either is visible as a disagreement rather than
// as a shared blind spot. Two ends running different policies can disagree in two directions and both
// are refused here, by name:
//
//   - the producer ships a column this policy denies — a leak, caught on every row and every declared
//     shape;
//   - the producer's policy is NARROWER than this one, so a column this policy publishes never
//     arrives. Nothing leaks, and nothing else in this consumer would notice: the destination column
//     stays null for ever and reads as missing data rather than as a stale policy. Caught from the
//     declared shape of a CREATE_TABLE, which is the one event that says what the producer believes
//     the table's publishable columns are.
//
// The second check is opportunistic — it needs a CREATE_TABLE in the feed — and that is acceptable
// because it is the non-security direction. The producer re-emits CREATE_TABLE at every checkpoint, so
// a following consumer sees one.
//
// Both checks are wired into `decodeLine`, which every subcommand that reads events goes through, so
// a mode added later cannot forget them.
//
// # The blind spot, stated rather than left to be discovered
//
// With no `-publication` flag, nothing here is enforced: `activePublication` is nil and every event
// passes. That is the pre-B7 behaviour every existing invocation depends on, and making the flag the
// only way to switch the check on means an unguarded run is visible in the command line rather than
// in a default buried in this file.

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
)

// Publication is what the feed is allowed to carry: a set of columns per table, and nothing for a
// table that is not a key of the map.
type Publication struct {
	Name string
	// table -> column -> published. A table absent from this map is UNDECIDED, which is refused
	// rather than published — the difference between an allowlist and a denylist, and the reason a
	// table created after the policy was written cannot leak.
	Tables map[string]map[string]bool
	// Tables the operator decided NOT to publish. A producer honouring this policy drops their events
	// and keeps its cursor moving; so an event for one of them arriving HERE means the two ends are
	// not running the same policy, and it is refused like any other undecided table — with its own
	// message, because "excluded" and "never heard of" are different mistakes to go and fix.
	Excluded map[string]bool
}

// activePublication is the policy this process enforces, or nil for none. Package-level so the check
// lives in decodeLine — one chokepoint every subcommand already uses — rather than in each of them.
var activePublication *Publication

// usePublication turns the check on. An unreadable or malformed file is an error, never a silent
// fall back to enforcing nothing: the operator asked for a policy, and ignoring the request would
// publish exactly the columns they were trying to hold back.
func usePublication(path string) error {
	if path == "" {
		return nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("reading publication %s: %w", path, err)
	}
	p, err := parsePublication(string(raw))
	if err != nil {
		return err
	}
	activePublication = p
	return nil
}

// parsePublication reads the declaration format:
//
//	publication analytics
//	# a line whose first non-blank character is # is a comment
//	inventory: id, qty
//
// Written from that description rather than from the Rust parser. Every malformed shape is an error at
// load time — a line it cannot read, a missing header, no tables, a table twice, a table with no
// columns, a column twice, a name that is not an identifier. A skipped line would be the worst outcome
// available: the operator reads the file and believes a table is published while this consumer refuses
// every event it carries.
//
// Two rules exist because of a widening both implementations had, found by an adversarial review
// rather than by writing this: `#` starts a comment only at the START of a line, and every name must
// be an identifier ([A-Za-z0-9_], everything the database's SQL scanner can create). With `#` honoured
// mid-line, `customers: id, ssn#hash` truncated to `customers: id, ssn` and published `ssn` — and
// because BOTH parsers truncated identically, reading the file twice could not catch it. Refusing a
// non-identifier name by name, rather than trimming it into a shorter one, closes the class.
func parsePublication(text string) (*Publication, error) {
	name := ""
	tables := map[string]map[string]bool{}
	excluded := map[string]bool{}
	for i, raw := range strings.Split(text, "\n") {
		lineno := i + 1
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		// Matched on the WORD rather than on "publication " with its space. A header whose name was
		// deleted trims to a bare `publication`, and the prefix form misses it and then reports the
		// line as unreadable — sending the reader after a missing colon on a line that plainly says
		// publication.
		if line == "publication" || strings.HasPrefix(line, "publication ") {
			decl := strings.TrimSpace(line[len("publication"):])
			if decl == "" {
				return nil, fmt.Errorf("publication declaration, line %d: the header has no name", lineno)
			}
			if name != "" {
				return nil, fmt.Errorf("publication declaration, line %d: a second `publication` header", lineno)
			}
			name = decl
			continue
		}
		// `exclude <table>`: decided, and decided against.
		if line == "exclude" || strings.HasPrefix(line, "exclude ") {
			table := strings.TrimSpace(line[len("exclude"):])
			if table == "" {
				return nil, fmt.Errorf("publication declaration, line %d: `exclude` with no table name", lineno)
			}
			if !isIdentifier(table) {
				return nil, fmt.Errorf("publication declaration, line %d: %s", lineno, notAnIdentifier("table", table))
			}
			if name == "" {
				return nil, fmt.Errorf("publication declaration, line %d: `exclude %s` appears before any `publication <name>` header", lineno, table)
			}
			if _, dup := tables[table]; dup {
				return nil, fmt.Errorf("publication declaration, line %d: table %q is both published and excluded", lineno, table)
			}
			if excluded[table] {
				return nil, fmt.Errorf("publication declaration, line %d: table %q is excluded twice", lineno, table)
			}
			excluded[table] = true
			continue
		}
		table, cols, found := strings.Cut(line, ":")
		if !found {
			return nil, fmt.Errorf("publication declaration, line %d: %q is neither a comment nor `table: col, col`", lineno, line)
		}
		table = strings.TrimSpace(table)
		if table == "" {
			return nil, fmt.Errorf("publication declaration, line %d: a column list with no table name", lineno)
		}
		if !isIdentifier(table) {
			return nil, fmt.Errorf("publication declaration, line %d: %s", lineno, notAnIdentifier("table", table))
		}
		if name == "" {
			return nil, fmt.Errorf("publication declaration, line %d: table %q appears before any `publication <name>` header", lineno, table)
		}
		if _, dup := tables[table]; dup {
			return nil, fmt.Errorf("publication declaration, line %d: table %q is declared twice", lineno, table)
		}
		if excluded[table] {
			return nil, fmt.Errorf("publication declaration, line %d: table %q is both excluded and published", lineno, table)
		}
		set := map[string]bool{}
		if strings.TrimSpace(cols) == "" {
			return nil, fmt.Errorf("publication declaration, line %d: table %q lists no columns; leave it out of the publication instead", lineno, table)
		}
		for _, col := range strings.Split(cols, ",") {
			col = strings.TrimSpace(col)
			if col == "" {
				return nil, fmt.Errorf("publication declaration, line %d: table %q has an empty column between commas", lineno, table)
			}
			if !isIdentifier(col) {
				return nil, fmt.Errorf("publication declaration, line %d: %s", lineno, notAnIdentifier("column", col))
			}
			if set[col] {
				return nil, fmt.Errorf("publication declaration, line %d: table %q lists column %q twice", lineno, table, col)
			}
			set[col] = true
		}
		tables[table] = set
	}
	if name == "" {
		return nil, fmt.Errorf("publication declaration: no `publication <name>` header; refusing to read this as a publication that publishes nothing")
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("publication declaration: publication %q publishes no table, so nothing could ever be accepted", name)
	}
	return &Publication{Name: name, Tables: tables, Excluded: excluded}, nil
}

// isIdentifier reports whether a name is one the database could have created: [A-Za-z0-9_], which is
// what its SQL scanner accepts, with no quoted-identifier form. Written from that rule rather than
// from the Rust helper, like the rest of this file.
func isIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		ok := (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_'
		if !ok {
			return false
		}
	}
	return true
}

func notAnIdentifier(kind, name string) string {
	return fmt.Sprintf("%s name %q is not an identifier. Names here are [A-Za-z0-9_]; anything else is "+
		"refused rather than trimmed, because a name containing a separator or a # would otherwise be "+
		"silently shortened into a DIFFERENT name that may match a real column and publish it", kind, name)
}

// published reports the column set for a table, and whether the publication has decided about it.
func (p *Publication) published(table string) (map[string]bool, bool) {
	cols, ok := p.Tables[table]
	return cols, ok
}

// check refuses an event that carries anything this publication does not publish, and — the other
// direction, which is just as much a defect — an event that claims to have withheld a column the
// publication does publish.
//
// Both directions are refusals rather than warnings. A consumer that logged "unexpected column" and
// carried on would have already written it to its destination by the time anyone read the log.
func (p *Publication) check(e *Event, n int) error {
	if p == nil {
		return nil
	}
	if p.Excluded[e.Table] {
		return fmt.Errorf("line %d: publication %q EXCLUDES table %q, so no event for it should be in "+
			"this feed at all; a producer honouring this policy drops them. Seeing one means the two "+
			"ends are running different policies", n, p.Name, e.Table)
	}
	cols, decided := p.published(e.Table)
	if !decided {
		return fmt.Errorf("line %d: table %q is not in publication %q, so no event for it may be "+
			"published; this is an allowlist, and a table it has not been told about is undecided "+
			"rather than permitted", n, e.Table, p.Name)
	}

	// **Both images, on every op, with no exemptions.** The first version of this skipped the row loop
	// entirely for a schema op and looked only inside `after["columns"]` — so a DROP_TABLE carrying a
	// `before` image full of denied columns validated with `OK 1` and exit 0. A DROP is the one op the
	// producer never gives a before image, which is exactly why it is the one worth policing here: this
	// check exists for feeds the producer should not have written.
	for _, side := range []struct {
		what string
		row  map[string]any
	}{{"before image", e.Before}, {"after image", e.After}} {
		// Sorted, so a feed with two denied columns fails the same way on every run.
		names := make([]string, 0, len(side.row))
		for col := range side.row {
			names = append(names, col)
		}
		sort.Strings(names)
		for _, col := range names {
			// A schema event's after image is the table's SHAPE, keyed under `columns`, not a row.
			// That one key is the payload and is checked below; everything else in the object is an
			// unexpected key and is refused rather than ignored.
			if isSchema(e.Op) && side.what == "after image" && col == "columns" {
				continue
			}
			if err := p.refuseUnpublished(e.Table, col, "the "+side.what+" of", n); err != nil {
				return err
			}
		}
	}

	if isSchema(e.Op) {
		if list, ok := e.After["columns"].([]any); ok {
			for _, c := range list {
				m, ok := c.(map[string]any)
				if !ok {
					continue // the envelope check has already refused this shape
				}
				col := fmt.Sprint(m["name"])
				if !cols[col] {
					return fmt.Errorf("line %d: %s declares column %q, which publication %q does "+
						"not publish; a consumer told about a column it will never be sent creates "+
						"it and then reports it as permanently null", n, e.Op, col, p.Name)
				}
			}
		}
	}

	return nil
}

// refuseUnpublished is the one wording for "this column left the database", shared by check() and by
// the `precision` report — which reads events without going through decodeLine and prints column
// names and values, so it needs the rule and cannot get it from the envelope path.
//
// `where` names the place the column was found, so the message locates it: "the after image of",
// "the type report for".
func (p *Publication) refuseUnpublished(table, col, where string, n int) error {
	if p == nil {
		return nil
	}
	if p.Excluded[table] {
		return fmt.Errorf("line %d: publication %q EXCLUDES table %q, so no event for it should be in "+
			"this feed at all; a producer honouring this policy drops them. Seeing one means the two "+
			"ends are running different policies", n, p.Name, table)
	}
	cols, decided := p.published(table)
	if !decided {
		return fmt.Errorf("line %d: table %q is not in publication %q, so no event for it may be "+
			"published; this is an allowlist, and a table it has not been told about is undecided "+
			"rather than permitted", n, table, p.Name)
	}
	if !cols[col] {
		return fmt.Errorf("line %d: %s %s carries column %q, which publication %q does not publish; "+
			"it has left the database", n, where, table, col, p.Name)
	}
	return nil
}

// checkShape is the narrower-producer direction: every column this publication publishes must appear
// in the shape the producer declares for the table.
//
// A missing one means the two ends are running different policies, or that the policy names a column
// the source does not have — a typo, or a column since dropped. Both are configuration errors whose
// only other symptom is a destination column that stays null for ever, so they are refused with the
// name rather than logged.
//
// Split from check() so the two directions can be read, and mutated, one at a time.
func (p *Publication) checkShape(e *Event, n int) error {
	if p == nil || e.Op != "CREATE_TABLE" {
		return nil
	}
	cols, decided := p.published(e.Table)
	if !decided {
		return nil // check() has already refused this
	}
	declared := map[string]bool{}
	if list, ok := e.After["columns"].([]any); ok {
		for _, c := range list {
			if m, ok := c.(map[string]any); ok {
				declared[fmt.Sprint(m["name"])] = true
			}
		}
	}
	missing := make([]string, 0, len(cols))
	for col := range cols {
		if !declared[col] {
			missing = append(missing, col)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fmt.Errorf("line %d: publication %q publishes %s.%s, but the producer's %s declares "+
			"no such column; either the producer is running a narrower publication than this one or "+
			"the policy names a column the source does not have. Nothing has leaked - the cost is a "+
			"destination column that stays null and never says why", n, p.Name, e.Table,
			strings.Join(missing, ", "), e.Op)
	}
	return nil
}

// envelopeKeys is the complete set of top-level keys a policed feed line may carry.
//
// An allowlist rather than a denylist, for the reason every guard over what leaves a machine is one:
// a denylist catches only what somebody already thought of. An adversarial review put
// `"ssn":"111-22-3333"` beside `after` at the top level and beside `columns` inside a schema payload,
// and both validated with OK 1 — the checks walked `before` and `after` and nothing else.
//
// **Adding a field to the feed means adding it here.** That is deliberate: a consumer holding a policy
// must not accept a field it cannot reason about, because the field may be where the denied column
// went. The failure is loud and names the key, so the fix is one line.
var envelopeKeys = map[string]bool{
	"table": true, "op": true, "txn": true, "lsn": true,
	"commit_lsn": true, "commit_end_lsn": true, "before": true, "after": true,
}

// policeRawLine enforces the structural rules a policy-checked line must satisfy, reading the raw
// token stream rather than the decoded struct.
//
// Three holes it closes, all found by an adversarial review, none of them visible after `encoding/json`
// has finished with the line:
//
//  1. **Duplicate keys.** `{"id":1,"name":"ada","name":"000-11-2222"}` decodes to ONE `name`, and Go
//     keeps the LAST — so a denied column sharing a published column's name arrives as the published
//     one's value. The producer refuses such a table now, and this is the independent half of that:
//     the duplicate is plainly there in the bytes, and only a token walk can see it.
//  2. **Keys outside the envelope**, at the top level or beside a schema payload's `columns`.
//  3. **Non-scalar values inside a row image.** Every value this database can put in a row is a
//     scalar; an object or array under a published key is a place to hide a denied one.
//
// Only when a policy is in force, so a feed with no `-publication` behaves exactly as it did before.
func (p *Publication) policeRawLine(line string, n int) error {
	if p == nil {
		return nil
	}
	dec := json.NewDecoder(strings.NewReader(line))
	dec.UseNumber()

	type frame struct {
		isObject  bool
		expectKey bool
		seen      map[string]bool
		seenLast  string   // the key whose value is being read, for the path of a nested container
		path      []string // keys from the root to this container
	}
	var stack []frame

	// consumed marks that a value has just been read in the innermost object, so the next token there
	// is a key again.
	consumed := func() {
		if len(stack) > 0 && stack[len(stack)-1].isObject {
			stack[len(stack)-1].expectKey = true
		}
	}
	pathOf := func() []string {
		if len(stack) == 0 {
			return nil
		}
		return stack[len(stack)-1].path
	}
	// forbiddenNesting reports whether a container being entered is nested INSIDE a row image, which is
	// where a denied column could hide. The image objects themselves (`before`, `after`) are the
	// containers a line is made of, and a schema payload's `columns` array and its column objects are
	// the one legitimate nesting; anything else under an image is refused.
	forbiddenNesting := func(path []string) bool {
		if len(path) < 2 || (path[0] != "before" && path[0] != "after") {
			return false
		}
		return !(path[0] == "after" && path[1] == "columns")
	}

	for {
		t, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("line %d: %w", n, err)
		}
		if d, ok := t.(json.Delim); ok {
			switch d {
			case '{', '[':
				parent := pathOf()
				var key string
				if len(stack) > 0 && stack[len(stack)-1].isObject && !stack[len(stack)-1].expectKey {
					key = stack[len(stack)-1].seenLast
				}
				path := append(append([]string{}, parent...), key)
				if key == "" {
					path = parent
				}
				if forbiddenNesting(path) {
					return fmt.Errorf("line %d: the %s image holds a nested %s under key %q; every value "+
						"a row of this database can carry is a scalar, so a container here is a place to "+
						"put a column publication %q does not publish", n, path[0], string(d), key, p.Name)
				}
				stack = append(stack, frame{
					isObject:  d == '{',
					expectKey: d == '{',
					seen:      map[string]bool{},
					path:      path,
				})
			case '}', ']':
				if len(stack) > 0 {
					stack = stack[:len(stack)-1]
				}
				consumed()
			}
			continue
		}
		if len(stack) == 0 {
			continue // a bare scalar document; checkEnvelope has already refused it
		}
		top := &stack[len(stack)-1]
		if top.isObject && top.expectKey {
			key, _ := t.(string)
			if top.seen[key] {
				return fmt.Errorf("line %d: key %q appears twice in the same object; a JSON parser keeps "+
					"one of the two values and this database allows two columns with one name, so the "+
					"value that survives may be the one the publication does not publish", n, key)
			}
			top.seen[key] = true
			top.seenLast = key
			top.expectKey = false
			if len(top.path) == 0 && !envelopeKeys[key] {
				return fmt.Errorf("line %d: unexpected top-level key %q in a feed checked against "+
					"publication %q; this consumer refuses a field it cannot reason about rather than "+
					"ignoring it, because an unknown field is somewhere a denied column can travel. If "+
					"the feed genuinely grew a field, add it to envelopeKeys", n, key, p.Name)
			}
			if len(top.path) == 2 && top.path[0] == "after" && top.path[1] == "columns" {
				continue // a column spec's own keys: name/type/nullable, validated by checkEnvelope
			}
			if len(top.path) == 1 && (top.path[0] == "before" || top.path[0] == "after") {
				continue // a column name, checked against the allowlist by check()
			}
			continue
		}
		// A scalar value.
		consumed()
	}
	return nil
}
