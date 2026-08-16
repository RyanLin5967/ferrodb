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
//	follow <addr> [-key id]        stream a live feed, materialise it, print the resulting table
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
	"net"
	"os"
	"sort"
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

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: cdc-consumer validate <feed.jsonl> | follow <addr> [flags]")
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
