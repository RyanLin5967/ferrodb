// Command cdc-consumer is an independent consumer of ferrodb's change feed.
//
// It shares no code with the database. That is the point: an encoder validated by its own author's
// idea of the format agrees with itself about any shared misreading, so the feed is checked here by
// a separate implementation in a separate language, reading the documented envelope and Go's
// standard `encoding/json`. It replaces an earlier Python validator and does strictly more.
//
// Go's json package rejects bare NaN and Infinity outright, which is a stronger guarantee than the
// Python version had — that one accepted them by default and needed an explicit override to refuse.
//
// Two subcommands:
//
//	validate <feed.jsonl>          check a feed file's format, exit non-zero on any violation
//	precision <feed.jsonl>         report the JSON type of every column, and which numbers a
//	                               default float64 decode would silently corrupt
//	follow <addr> [-key id]        stream a live feed, materialise it, print the resulting table
//	sink <feed.jsonl> -db f.sqlite land the feed in SQLite with idempotent, order-guarded upserts
//
// `follow` is the interesting one. It maintains the table the feed describes — applying READ,
// INSERT, UPDATE and DELETE to a local map — and prints the result. A caller can then compare that
// against the source database directly, which judges the feed by whether a consumer arrives at the
// right data rather than by whether the producer thinks it emitted the right events.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
)

// Event is the documented change-feed envelope.
type Event struct {
	Table        string         `json:"table"`
	Op           string         `json:"op"`
	Txn          uint64         `json:"txn"`
	LSN          uint64         `json:"lsn"`
	CommitLSN    uint64         `json:"commit_lsn"`
	CommitEndLSN uint64         `json:"commit_end_lsn"`
	Before       map[string]any `json:"before"`
	After        map[string]any `json:"after"`
}

var validOps = map[string]bool{
	"READ": true, "INSERT": true, "UPDATE": true, "DELETE": true,
	// Schema events. Adding an op to the feed is a breaking change for every consumer, and this
	// program proved it: it rejected CREATE_TABLE with "unknown op" the moment the producer
	// started emitting one. That is the independent implementation doing its job rather than a
	// nuisance — a consumer that silently ignored ops it did not recognise would drop schema
	// changes and never say so.
	"CREATE_TABLE": true, "DROP_TABLE": true,
}

// isSchema reports whether an op describes the table's shape rather than a row.
func isSchema(op string) bool { return op == "CREATE_TABLE" || op == "DROP_TABLE" }

// checkEnvelope enforces the invariants the feed documents, independently of the producer.
func checkEnvelope(e *Event, raw string, n int) error {
	if e.Table == "" {
		return fmt.Errorf("line %d: empty table name", n)
	}
	if !validOps[e.Op] {
		return fmt.Errorf("line %d: unknown op %q", n, e.Op)
	}
	// before/after presence must follow from op alone, or a consumer cannot branch on op.
	switch e.Op {
	case "CREATE_TABLE":
		if e.Before != nil {
			return fmt.Errorf("line %d: CREATE_TABLE carries a before image", n)
		}
		// Its payload is the table's shape, keyed under `columns` so it can never be mistaken for
		// a row of data.
		if e.After == nil {
			return fmt.Errorf("line %d: CREATE_TABLE has no schema payload", n)
		}
		cols, ok := e.After["columns"]
		if !ok {
			return fmt.Errorf("line %d: CREATE_TABLE payload has no columns", n)
		}
		list, ok := cols.([]any)
		if !ok || len(list) == 0 {
			return fmt.Errorf("line %d: CREATE_TABLE columns is not a non-empty list", n)
		}
		for _, c := range list {
			m, ok := c.(map[string]any)
			if !ok {
				return fmt.Errorf("line %d: a column is not an object", n)
			}
			for _, want := range []string{"name", "type", "nullable"} {
				if _, ok := m[want]; !ok {
					return fmt.Errorf("line %d: a column has no %q", n, want)
				}
			}
		}
	case "DROP_TABLE":
		if e.After != nil {
			return fmt.Errorf("line %d: DROP_TABLE carries an after image", n)
		}
	case "READ", "INSERT":
		if e.Before != nil {
			return fmt.Errorf("line %d: %s carries a before image", n, e.Op)
		}
		if e.After == nil {
			return fmt.Errorf("line %d: %s has no after image", n, e.Op)
		}
	case "DELETE":
		if e.After != nil {
			return fmt.Errorf("line %d: DELETE carries an after image", n)
		}
		if e.Before == nil {
			return fmt.Errorf("line %d: DELETE has no before image", n)
		}
	case "UPDATE":
		if e.Before == nil || e.After == nil {
			return fmt.Errorf("line %d: UPDATE is missing one of its images", n)
		}
	}
	if e.CommitEndLSN <= e.CommitLSN && e.Op != "READ" {
		// A snapshot READ legitimately stamps all three LSNs the same: it is not a log record and
		// has no commit of its own. A streamed change must have a resume point past its commit.
		return fmt.Errorf("line %d: commit_end_lsn %d is not past commit_lsn %d",
			n, e.CommitEndLSN, e.CommitLSN)
	}
	// The raw line must be one object on one line: a newline inside would split one record into two.
	if strings.Contains(raw, "\n") {
		return fmt.Errorf("line %d: record contains a newline", n)
	}
	return nil
}

// decodeLine parses one line strictly. `encoding/json` rejects NaN/Infinity, trailing garbage and
// duplicate top-level values on its own.
func decodeLine(line string, n int) (*Event, error) {
	dec := json.NewDecoder(strings.NewReader(line))
	dec.UseNumber() // keep numbers exact rather than routing every integer through float64
	var e Event
	if err := dec.Decode(&e); err != nil {
		return nil, fmt.Errorf("line %d is not valid JSON: %w", n, err)
	}
	// Anything after the object means the line held more than one record.
	if _, err := dec.Token(); err != io.EOF {
		return nil, fmt.Errorf("line %d has trailing content after the object", n)
	}
	if err := checkEnvelope(&e, line, n); err != nil {
		return nil, err
	}
	return &e, nil
}

// Table is the materialised view a consumer builds from the feed.
type Table struct {
	key string
	// Column names as last declared by a schema event, so a consumer knows the destination shape.
	columns []string
	rows    map[string]map[string]any
}

func newTable(key string) *Table {
	return &Table{key: key, rows: map[string]map[string]any{}}
}

func (t *Table) keyOf(row map[string]any) (string, error) {
	v, ok := row[t.key]
	if !ok {
		return "", fmt.Errorf("row has no key column %q: %v", t.key, row)
	}
	return fmt.Sprint(v), nil
}

// apply folds one event into the table. This is where a CDC consumer earns its keep, and where
// getting DELETE wrong shows up as a row that never goes away.
func (t *Table) apply(e *Event) error {
	switch e.Op {
	case "CREATE_TABLE":
		// Schema evolution: adopt the declared shape. A real sink would issue CREATE/ALTER against
		// its destination here; the point is that it learns the shape IN BAND and in log order,
		// rather than being told out of band and having to guess which rows it applies to.
		t.columns = t.columns[:0]
		if list, ok := e.After["columns"].([]any); ok {
			for _, c := range list {
				if m, ok := c.(map[string]any); ok {
					t.columns = append(t.columns, fmt.Sprint(m["name"]))
				}
			}
		}
		return nil
	case "DROP_TABLE":
		t.rows = map[string]map[string]any{}
		t.columns = nil
		return nil
	case "READ", "INSERT", "UPDATE":
		k, err := t.keyOf(e.After)
		if err != nil {
			return err
		}
		t.rows[k] = e.After
	case "DELETE":
		k, err := t.keyOf(e.Before)
		if err != nil {
			return err
		}
		delete(t.rows, k)
	}
	return nil
}

// dump prints the table as sorted JSON so a caller can compare it byte for byte.
func (t *Table) dump(w io.Writer) error {
	keys := make([]string, 0, len(t.rows))
	for k := range t.rows {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, t.rows[k])
	}
	enc := json.NewEncoder(w)
	return enc.Encode(out)
}

// exactFloat renders a float64's exact value, not its shortest round-trip form.
//
// `strconv.FormatFloat(v, 'f', -1, 64)` prints the fewest digits that parse back to the same
// double, so `i64::MAX` decoded as a double prints as 9223372036854776000 — which is neither the
// value that was sent nor the value actually held. Every finite binary float is a terminating
// decimal, so a `big.Rat` holds it exactly and prints the true value: 9223372036854775808.
// Understating the corruption would make this report less useful than saying nothing.
func exactFloat(v float64) string {
	r := new(big.Rat).SetFloat64(v)
	if r == nil { // NaN or Inf, which the feed never carries as a bare number
		return strconv.FormatFloat(v, 'f', -1, 64)
	}
	if r.IsInt() {
		return r.Num().String()
	}
	// Enough fractional digits to distinguish any two doubles; trailing zeros are trimmed below.
	s := strings.TrimRight(r.FloatString(40), "0")
	return strings.TrimSuffix(s, ".")
}

// sameNumber reports whether the digits on the wire denote exactly the value the double holds.
//
// The comparison is numeric rather than textual, via exact rationals: `1.50` on the wire and the
// double 1.5 are the same number, and calling that a precision loss would be false. What it does
// catch is the real thing — wire digits that no double can represent, so the value a consumer ends
// up holding is a different number from the one that was sent.
func sameNumber(wire string, held float64) bool {
	w, ok := new(big.Rat).SetString(wire)
	if !ok {
		return false
	}
	h := new(big.Rat).SetFloat64(held)
	if h == nil {
		return false
	}
	return w.Cmp(h) == 0
}

// precision reports, for every row column in the feed, the JSON type it arrived as and what a
// consumer using Go's DEFAULT decoding would end up holding.
//
// This is the independent half of the producer's claim that BIGINT, DECIMAL and TIMESTAMP ship as
// JSON strings. The Rust unit tests assert on bytes the Rust encoder produced, which cannot detect
// a shared misreading of JSON. This decodes with `encoding/json` into `map[string]any` — the
// single most common consumer shape there is, and the one where every JSON number becomes a
// float64 — and then compares what that yields against the exact digits on the wire.
//
// Each column prints one line:
//
//	FIELD <line> <col> string <exact text>
//	FIELD <line> <col> number <what a float64 consumer holds> [LOSSY <exact digits on the wire>]
//
// LOSSY marks a column whose digits did not survive the float64, which is the corruption the
// string encoding exists to prevent. Note that Go raised no error on any of these: the parse
// succeeded and the number is simply wrong, which is exactly the failure mode being demonstrated.
func precision(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		return errors.New("feed is empty; a feed that collected nothing has not passed")
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	strings_, numbers, lossy := 0, 0, 0
	for i, line := range lines {
		// Two decodes of the same line: the default one a consumer writes, and an exact one used
		// only as the reference for what was actually on the wire.
		var loose map[string]any
		if err := json.Unmarshal([]byte(line), &loose); err != nil {
			return fmt.Errorf("line %d is not valid JSON: %w", i+1, err)
		}
		exactDec := json.NewDecoder(strings.NewReader(line))
		exactDec.UseNumber()
		var exact map[string]any
		if err := exactDec.Decode(&exact); err != nil {
			return fmt.Errorf("line %d is not valid JSON: %w", i+1, err)
		}

		for _, side := range []string{"after", "before"} {
			looseRow, ok := loose[side].(map[string]any)
			if !ok {
				continue
			}
			exactRow, _ := exact[side].(map[string]any)
			cols := make([]string, 0, len(looseRow))
			for k := range looseRow {
				cols = append(cols, k)
			}
			sort.Strings(cols)
			for _, col := range cols {
				switch v := looseRow[col].(type) {
				case string:
					strings_++
					fmt.Printf("FIELD %d %s string %s\n", i+1, col, v)
				case float64:
					numbers++
					held := exactFloat(v)
					wire := ""
					if n, ok := exactRow[col].(json.Number); ok {
						wire = n.String()
					}
					if wire != "" && !sameNumber(wire, v) {
						lossy++
						fmt.Printf("FIELD %d %s number %s LOSSY %s\n", i+1, col, held, wire)
					} else {
						fmt.Printf("FIELD %d %s number %s\n", i+1, col, held)
					}
				}
			}
		}
	}
	fmt.Printf("SUMMARY strings=%d numbers=%d lossy=%d\n", strings_, numbers, lossy)
	return nil
}

func validate(path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		return errors.New("feed is empty; a feed that collected nothing has not passed")
	}
	if raw[len(raw)-1] != '\n' {
		return errors.New("feed does not end with a newline; the last record may be truncated")
	}
	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	var last uint64
	for i, line := range lines {
		e, err := decodeLine(line, i+1)
		if err != nil {
			return err
		}
		if e.CommitLSN < last {
			return fmt.Errorf("line %d: commit_lsn went backwards", i+1)
		}
		last = e.CommitLSN
	}
	fmt.Printf("OK %d\n", len(lines))
	return nil
}

func follow(addr, key string, cursor uint64, limit int) error {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := fmt.Fprintf(conn, "%d\n", cursor); err != nil {
		return err
	}

	table := newTable(key)
	scanner := bufio.NewScanner(conn)
	scanner.Buffer(make([]byte, 0, 64*1024), 8*1024*1024)
	n := 0
	var lastCursor uint64
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		n++
		e, err := decodeLine(line, n)
		if err != nil {
			return err
		}
		if err := table.apply(e); err != nil {
			return err
		}
		lastCursor = e.CommitEndLSN
		if limit > 0 && n >= limit {
			break
		}
	}
	if err := scanner.Err(); err != nil && !errors.Is(err, io.EOF) {
		return fmt.Errorf("reading feed: %w", err)
	}
	if n == 0 {
		return errors.New("consumed no events; a consumer that received nothing has not converged")
	}

	fmt.Fprintf(os.Stderr, "consumed %d event(s), cursor %d\n", n, lastCursor)
	fmt.Printf("CURSOR %d\n", lastCursor)
	if len(table.columns) > 0 {
		fmt.Printf("COLUMNS %s\n", strings.Join(table.columns, ","))
	}
	fmt.Print("TABLE ")
	return table.dump(os.Stdout)
}

// runSink lands a feed file into SQLite.
//
// Deliberately not transactional across the whole file. A sink that only becomes visible at the end
// of a batch is a sink that loses everything when it dies mid-batch, and the per-row guard already
// makes re-applying safe — so crashing part-way and being restarted is a normal, correct thing to
// do here rather than a recovery problem.
func runSink(feedPath, dbPath, key string) error {
	raw, err := os.ReadFile(feedPath)
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		return errors.New("feed is empty; a sink that landed nothing has not succeeded")
	}
	sink, err := openSink(dbPath, key)
	if err != nil {
		return err
	}
	defer sink.Close()

	lines := strings.Split(strings.TrimSuffix(string(raw), "\n"), "\n")
	applied, skipped := 0, 0
	var lastTable string
	var cursor uint64
	for i, line := range lines {
		if line == "" {
			continue
		}
		e, err := decodeLine(line, i+1)
		if err != nil {
			return err
		}
		// Events at or below what this table has already absorbed are re-deliveries. Counted rather
		// than hidden: "skipped 40" is how an operator sees a replay happening at all.
		if e.CommitLSN <= sink.cursor(e.Table) && !isSchema(e.Op) {
			skipped++
			continue
		}
		if err := sink.apply(e); err != nil {
			return err
		}
		applied++
		lastTable = e.Table
		if e.CommitEndLSN > cursor {
			cursor = e.CommitEndLSN
		}
		if !isSchema(e.Op) {
			if err := sink.saveCursor(e.Table, e.CommitLSN); err != nil {
				return err
			}
		}
	}
	fmt.Fprintf(os.Stderr, "applied %d, skipped %d re-delivered\n", applied, skipped)
	fmt.Printf("APPLIED %d SKIPPED %d CURSOR %d TABLE %s\n", applied, skipped, cursor, lastTable)
	return nil
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: cdc-consumer validate <feed.jsonl> | follow <addr> [flags] | sink <feed.jsonl> -db <file>")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "validate":
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr, "usage: cdc-consumer validate <feed.jsonl>")
			os.Exit(2)
		}
		if err := validate(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "precision":
		if len(os.Args) != 3 {
			fmt.Fprintln(os.Stderr, "usage: cdc-consumer precision <feed.jsonl>")
			os.Exit(2)
		}
		if err := precision(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "sink":
		fs := flag.NewFlagSet("sink", flag.ExitOnError)
		dbPath := fs.String("db", "cdc.sqlite", "destination SQLite file")
		key := fs.String("key", "id", "primary key column")
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: cdc-consumer sink <feed.jsonl> -db <file>")
			os.Exit(2)
		}
		feed := os.Args[2]
		_ = fs.Parse(os.Args[3:])
		if err := runSink(feed, *dbPath, *key); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "follow":
		fs := flag.NewFlagSet("follow", flag.ExitOnError)
		key := fs.String("key", "id", "column to key the materialised table by")
		cursor := fs.Uint64("cursor", 0, "resume from this cursor; 0 means the start of the log")
		limit := fs.Int("limit", 0, "stop after this many events; 0 means until the server closes")
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: cdc-consumer follow <addr> [flags]")
			os.Exit(2)
		}
		addr := os.Args[2]
		_ = fs.Parse(os.Args[3:])
		if err := follow(addr, *key, *cursor, *limit); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		os.Exit(2)
	}
}
