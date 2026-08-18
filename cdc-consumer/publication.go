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
	"fmt"
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
//	# comment
//	inventory: id, qty
//
// Written from that description rather than from the Rust parser. Every malformed shape is an error
// at load time — a line it cannot read, a missing header, no tables, a table twice, a table with no
// columns, a column twice. A skipped line would be the worst outcome available: the operator reads
// the file and believes a table is published while this consumer refuses every event it carries.
func parsePublication(text string) (*Publication, error) {
	name := ""
	tables := map[string]map[string]bool{}
	for i, raw := range strings.Split(text, "\n") {
		lineno := i + 1
		line := raw
		if at := strings.Index(line, "#"); at >= 0 {
			line = line[:at]
		}
		line = strings.TrimSpace(line)
		if line == "" {
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
		table, cols, found := strings.Cut(line, ":")
		if !found {
			return nil, fmt.Errorf("publication declaration, line %d: %q is neither a comment nor `table: col, col`", lineno, line)
		}
		table = strings.TrimSpace(table)
		if table == "" {
			return nil, fmt.Errorf("publication declaration, line %d: a column list with no table name", lineno)
		}
		if name == "" {
			return nil, fmt.Errorf("publication declaration, line %d: table %q appears before any `publication <name>` header", lineno, table)
		}
		if _, dup := tables[table]; dup {
			return nil, fmt.Errorf("publication declaration, line %d: table %q is declared twice", lineno, table)
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
		return nil, fmt.Errorf("publication declaration: publication %q names no table, so it would refuse every event", name)
	}
	return &Publication{Name: name, Tables: tables}, nil
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
	cols, decided := p.published(e.Table)
	if !decided {
		return fmt.Errorf("line %d: table %q is not in publication %q, so no event for it may be "+
			"published; this is an allowlist, and a table it has not been told about is undecided "+
			"rather than permitted", n, e.Table, p.Name)
	}

	// A schema event's after image is the table's SHAPE, keyed under `columns`, not a row — so the
	// names to check are inside that list. Checking it as a row would test the literal key
	// "columns" against the allowlist and refuse every CREATE_TABLE ever emitted.
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
	} else {
		for _, side := range []struct {
			what string
			row  map[string]any
		}{{"before", e.Before}, {"after", e.After}} {
			// Sorted, so a feed with two denied columns fails the same way on every run.
			names := make([]string, 0, len(side.row))
			for col := range side.row {
				names = append(names, col)
			}
			sort.Strings(names)
			for _, col := range names {
				if !cols[col] {
					return fmt.Errorf("line %d: the %s image of %s carries column %q, which "+
						"publication %q does not publish; it has left the database", n, side.what,
						e.Table, col, p.Name)
				}
			}
		}
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
