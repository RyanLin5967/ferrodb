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
// Subcommands:
//
//	validate <feed.jsonl> [-publication f]
//	                                 check a feed file's format, exit non-zero on any violation; with
//	                                 -publication, also refuse anything the policy does not publish
//	precision <feed.jsonl> [-publication f]
//	                                 report the JSON type of every column, and which numbers a
//	                                 default float64 decode would silently corrupt; refuses a column
//	                                 the publication does not publish rather than printing it
//	follow <addr> [-key id]          stream a live feed, materialise it, print the resulting table
//	sink <feed.jsonl> -db f [-engine] land the feed with idempotent, order-guarded upserts
//	retract <db> -table t -model-version v [-engine]  withdraw everything one model version wrote
//	scan <db> -table t [-key id] [-engine]           print every row's writer and retraction flag
//	duckdb-sql <file> <sql>          run one statement against a DuckDB destination, separate process
//
// `sink` speaks to two destinations, chosen with `-engine`: `sqlite` (the default, and what the
// existing tests exercise) and `duckdb`. They are not two spellings of one thing — SQLite is where
// an operational replica goes and DuckDB is where the analysts' copy goes — but they carry the same
// four guarantees, and both put the ordering guard in the SQL statement rather than in Go.
//
// `follow` is the interesting one. It maintains the table the feed describes — applying READ,
// INSERT, UPDATE and DELETE to a local map — and prints the result. A caller can then compare that
// against the source database directly, which judges the feed by whether a consumer arrives at the
// right data rather than by whether the producer thinks it emitted the right events.
package main

import (
	"bufio"
	"bytes"
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
	Writer       *Writer        `json:"writer"`
	Before       map[string]any `json:"before"`
	After        map[string]any `json:"after"`
}

// Writer is the agent run that produced a change.
//
// Nil for a change no agent run produced, for the snapshot READ rows that existed before the feed
// began, and for schema declarations. Nil is a legitimate value and not an error — but a consumer
// that cannot count how often it happens cannot tell "this database has no agents" from "this feed
// lost its attribution", so `validate` reports the number.
//
// `PromptSHA256` is a digest and never the prompt. That is enforced rather than trusted: see
// `writerKeys`.
type Writer struct {
	ProvID       uint64 `json:"prov_id"`
	Agent        string `json:"agent"`
	Run          string `json:"run"`
	Model        string `json:"model"`
	ModelVersion string `json:"model_version"`
	PromptSHA256 string `json:"prompt_sha256"`
	StartedAt    string `json:"started_at"`
	Branch       string `json:"branch"`
}

// writerKeys is an ALLOWLIST of the keys a writer object may carry, and it is an allowlist on
// purpose.
//
// `prompt_hash` exists precisely so that a prompt containing customer data does not become a
// durable copy of it in every consumer's destination table. A denylist of forbidden key names only
// catches the spellings somebody already thought of — `prompt`, `prompt_text`, `instructions`,
// `system_prompt` — and a producer that adds a ninth field for any reason would land it in the sink
// unnoticed. A closed set cannot be talked around.
var writerKeys = map[string]bool{
	"prov_id": true, "agent": true, "run": true, "model": true, "model_version": true,
	"prompt_sha256": true, "started_at": true, "branch": true,
}

// isHex64 reports whether s is exactly 64 lowercase-or-uppercase hex digits: a SHA-256 digest.
func isHex64(s string) bool {
	if len(s) != 64 {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F') {
			return false
		}
	}
	return true
}

// checkWriter enforces the writer contract against the RAW object, not the decoded struct.
//
// The struct is the wrong instrument for this: `encoding/json` silently drops keys it has no field
// for, so a producer leaking the prompt text would decode cleanly into a Writer that looks perfect.
func checkWriter(raw map[string]json.RawMessage, w *Writer, n int) error {
	for k := range raw {
		if !writerKeys[k] {
			return fmt.Errorf("line %d: the writer object carries an unknown key %q. The writer "+
				"contract is a closed set, and `prompt_hash` exists so a prompt never becomes a "+
				"durable copy of itself downstream", n, k)
		}
	}
	if w.ProvID == 0 {
		return fmt.Errorf("line %d: the writer names prov_id 0, which is the slot meaning "+
			"'unattributed'; an event claiming a writer must name one", n)
	}
	for name, v := range map[string]string{
		"agent": w.Agent, "run": w.Run, "model": w.Model, "model_version": w.ModelVersion,
	} {
		if v == "" {
			return fmt.Errorf("line %d: the writer has an empty %s", n, name)
		}
	}
	if !isHex64(w.PromptSHA256) {
		return fmt.Errorf("line %d: prompt_sha256 %q is not 64 hex digits; a prompt digest that is "+
			"not a digest is either missing or is the prompt itself", n, w.PromptSHA256)
	}
	return nil
}

var validOps = map[string]bool{
	"READ": true, "INSERT": true, "UPDATE": true, "DELETE": true,
	// Schema events. Adding an op to the feed is a breaking change for every consumer, and this
	// program proved it: it rejected CREATE_TABLE with "unknown op" the moment the producer
	// started emitting one. That is the independent implementation doing its job rather than a
	// nuisance — a consumer that silently ignored ops it did not recognise would drop schema
	// changes and never say so.
	"CREATE_TABLE": true, "DROP_TABLE": true,
	// B11's column-level three. Same lesson, applied on purpose this time rather than discovered:
	// the producer and this program were changed together, because a consumer that has not been
	// taught an op refuses the whole feed and one that silently ignores it drops schema changes.
	"ADD_COLUMN": true, "RENAME_COLUMN": true, "ALTER_COLUMN_TYPE": true,
}

// isSchema reports whether an op describes the table's shape rather than a row.
func isSchema(op string) bool {
	switch op {
	case "CREATE_TABLE", "DROP_TABLE", "ADD_COLUMN", "RENAME_COLUMN", "ALTER_COLUMN_TYPE":
		return true
	}
	return false
}

// isDeclaration reports the schema ops that are **re-emitted** rather than delivered once.
//
// This is the distinction that matters to a consumer, and it is NOT the same as isSchema. A
// CREATE_TABLE is a declaration — "this table has this shape" — re-sent at every checkpoint of the
// source, because a checkpoint truncates the log and has to re-establish the schema at the new
// base. A DROP_TABLE likewise leaves the source's retained set. The column-level three are
// **news**: each is delivered exactly once, at the position the DDL occupied, and a consumer that
// applies one twice has renamed a column that no longer has the old name.
//
// The source draws the same line in the same place (`SchemaChange::is_declaration`), and the
// mechanism behind it is that an ALTER updates the source's retained CREATE_TABLE declaration
// rather than being retained itself.
func isDeclaration(op string) bool { return op == "CREATE_TABLE" || op == "DROP_TABLE" }

// bypassesCursor reports ops whose idempotence CANNOT come from the resume cursor.
//
// Schema events are re-emitted at every checkpoint by design, so they were always exempt. READ joins
// them for a sharper reason: a snapshot is ONE logical batch taken at a single LSN, and
// `replication::snapshot` gives every row of it the same lsn, commit_lsn AND commit_end_lsn. No
// positional key can order rows that share every position — so the composite (commit_lsn, lsn) key
// that fixed multi-row COMMITS cannot fix multi-row SNAPSHOTS, and the first row of a backfill was
// still advancing the cursor past all its siblings.
//
// Measured on the shipped binary before this: a 3-row snapshot landed ONE row, printed
// `APPLIED 2 SKIPPED 2`, and exited 0 — a full-table backfill of any size landed one row.
//
// Their idempotence comes from the upsert instead, which is strictly stronger here: a re-delivered
// snapshot row compares EQUAL on (commit_lsn, lsn) and the ON CONFLICT guard requires strictly greater,
// so a replay is a no-op; and a snapshot row arriving AFTER a newer stream event for the same key has a
// lower commit_lsn and is rejected — which is the cutover's whole hazard, since the snapshot boundary
// is taken before the scan.
// B11: `isDeclaration`, not `isSchema`. A column-level change is an ordinary positioned log
// record delivered once, so the cursor is exactly the right idempotence mechanism for it — and
// exempting it would be worse than useless: a re-run of the same feed would re-apply the rename.
func bypassesCursor(op string) bool { return isDeclaration(op) || op == "READ" }

// schemaColumns validates the `after.columns` payload every shape-carrying event has, and returns
// it as name -> declared type.
//
// One function, two callers: `CREATE_TABLE` and B11's column-level three all carry the table's full
// shape under the same key with the same rules, and validating it twice is two chances to check
// different things. The map it returns is what lets the column-level cases cross-check the
// alteration against the shape it claims to have produced.
func schemaColumns(e *Event, n int) (map[string]string, error) {
	// Its payload is the table's shape, keyed under `columns` so it can never be mistaken for a row
	// of data.
	if e.After == nil {
		return nil, fmt.Errorf("line %d: %s has no schema payload", n, e.Op)
	}
	cols, ok := e.After["columns"]
	if !ok {
		return nil, fmt.Errorf("line %d: %s payload has no columns", n, e.Op)
	}
	list, ok := cols.([]any)
	if !ok || len(list) == 0 {
		return nil, fmt.Errorf("line %d: %s columns is not a non-empty list", n, e.Op)
	}
	out := make(map[string]string, len(list))
	for _, c := range list {
		m, ok := c.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("line %d: a column is not an object", n)
		}
		for _, want := range []string{"name", "type", "nullable"} {
			if _, ok := m[want]; !ok {
				return nil, fmt.Errorf("line %d: a column has no %q", n, want)
			}
		}
		out[fmt.Sprint(m["name"])] = fmt.Sprint(m["type"])
	}
	return out, nil
}

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
		if _, err := schemaColumns(e, n); err != nil {
			return err
		}
	case "DROP_TABLE":
		if e.After != nil {
			return fmt.Errorf("line %d: DROP_TABLE carries an after image", n)
		}
	case "ADD_COLUMN", "RENAME_COLUMN", "ALTER_COLUMN_TYPE":
		// A column-level change carries BOTH halves and each is checked here, independently of the
		// producer, because either half alone is unusable:
		//
		//   - `columns` is the table's full shape afterwards, which is what the sinks reconcile
		//     their destination against, positionally, exactly as they do for a CREATE_TABLE;
		//   - `alter` says which change produced that shape, which the shape cannot say. A rename
		//     and a drop-plus-add leave identical column lists, and only one of them keeps the
		//     column's data.
		//
		// The cross-checks below are the point of an independent implementation: they verify the
		// two halves agree with each other, which a producer validated by its own idea of the
		// format cannot do for itself.
		if e.Before != nil {
			return fmt.Errorf("line %d: %s carries a before image", n, e.Op)
		}
		cols, err := schemaColumns(e, n)
		if err != nil {
			return err
		}
		alter, ok := e.After["alter"].(map[string]any)
		if !ok {
			return fmt.Errorf("line %d: %s has no alter payload; the new shape alone cannot say "+
				"which change produced it", n, e.Op)
		}
		named := func(k string) (string, error) {
			v, ok := alter[k].(string)
			if !ok || v == "" {
				return "", fmt.Errorf("line %d: %s alter payload has no %q", n, e.Op, k)
			}
			return v, nil
		}
		switch e.Op {
		case "ADD_COLUMN":
			c, err := named("column")
			if err != nil {
				return err
			}
			if _, ok := cols[c]; !ok {
				return fmt.Errorf("line %d: ADD_COLUMN adds %q but the new shape does not contain "+
					"it", n, c)
			}
		case "RENAME_COLUMN":
			from, err := named("from")
			if err != nil {
				return err
			}
			to, err := named("to")
			if err != nil {
				return err
			}
			if from == to {
				return fmt.Errorf("line %d: RENAME_COLUMN renames %q to itself", n, from)
			}
			if _, ok := cols[to]; !ok {
				return fmt.Errorf("line %d: RENAME_COLUMN renames to %q but the new shape does not "+
					"contain it", n, to)
			}
			if _, ok := cols[from]; ok {
				return fmt.Errorf("line %d: RENAME_COLUMN renames away from %q but the new shape "+
					"still contains it", n, from)
			}
		case "ALTER_COLUMN_TYPE":
			c, err := named("column")
			if err != nil {
				return err
			}
			was, err := named("from")
			if err != nil {
				return err
			}
			now, err := named("to")
			if err != nil {
				return err
			}
			if was == now {
				return fmt.Errorf("line %d: ALTER_COLUMN_TYPE changes %q from %s to the same type",
					n, c, was)
			}
			t, ok := cols[c]
			if !ok {
				return fmt.Errorf("line %d: ALTER_COLUMN_TYPE retypes %q but the new shape does "+
					"not contain it", n, c)
			}
			if t != now {
				return fmt.Errorf("line %d: ALTER_COLUMN_TYPE says %q became %s but the new shape "+
					"declares it %s", n, c, now, t)
			}
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
	// **The publication, re-checked here rather than trusted from the producer.** In decodeLine
	// because every mode that reads events goes through it, so a subcommand added later cannot be
	// written without the check. See publication.go for what each direction catches, and for the
	// blind spot: with no -publication flag, activePublication is nil and nothing is enforced.
	// The structural rules first: they read the raw bytes, and two of the three things they catch are
	// invisible once `encoding/json` has collapsed the line into a struct.
	if err := activePublication.policeRawLine(line, n); err != nil {
		return nil, err
	}
	if err := activePublication.check(&e, n); err != nil {
		return nil, err
	}
	if err := activePublication.checkShape(&e, n); err != nil {
		return nil, err
	}
	if e.Writer != nil {
		// Re-read the writer as raw keys. The struct above cannot answer "what else was in there",
		// and that is exactly the question the prompt-leak guard has to ask.
		var probe struct {
			Writer map[string]json.RawMessage `json:"writer"`
		}
		if err := json.Unmarshal([]byte(line), &probe); err != nil {
			return nil, fmt.Errorf("line %d: re-reading the writer object: %w", n, err)
		}
		if err := checkWriter(probe.Writer, e.Writer, n); err != nil {
			return nil, err
		}
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
		t.adoptColumns(e)
		return nil
	case "ADD_COLUMN":
		// The rows already held keep the values they were delivered with; the new column is absent
		// from them, which is exactly right — it did not exist when they were written, and the
		// source did not re-image them.
		t.adoptColumns(e)
		return nil
	case "RENAME_COLUMN":
		// The rows already held are keyed by the OLD name. Renaming the shape and leaving them
		// alone would make every one of them look like a row missing the new column and carrying a
		// stray one, and `diff` against the source — which reports the new name — would show every
		// row as different. This is the case that makes the alteration's `from` load-bearing: the
		// shape alone cannot say which key to move.
		from, _ := alterField(e, "from")
		to, _ := alterField(e, "to")
		t.adoptColumns(e)
		if from == "" || to == "" {
			return nil
		}
		if t.key == from {
			t.key = to
		}
		for _, row := range t.rows {
			if v, ok := row[from]; ok {
				row[to] = v
				delete(row, from)
			}
		}
		return nil
	case "ALTER_COLUMN_TYPE":
		// The rows already held carry the column in its OLD encoding. The feed encodes BIGINT,
		// DECIMAL and TIMESTAMP as JSON **strings** and INTEGER as a JSON number (see the producer's
		// `jsonl` module: a double cannot hold an i64 exactly, so the digits ship as text), and the
		// source does not re-image existing rows for a retype — the values did not change, only the
		// column's declared type did.
		//
		// So the fold has to re-encode what it is already holding, or a table materialised across a
		// retype has two encodings for one column and `diff` reports every older row as different.
		// Every retype the source performs widens toward a string-encoded type, which makes this a
		// single rule rather than a conversion table.
		to, _ := alterField(e, "to")
		col, _ := alterField(e, "column")
		t.adoptColumns(e)
		if col == "" || !stringEncoded(to) {
			return nil
		}
		for _, row := range t.rows {
			if v, ok := row[col]; ok {
				row[col] = asFeedString(v)
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

// adoptColumns replaces the table's column list with the shape the event declares.
func (t *Table) adoptColumns(e *Event) {
	t.columns = t.columns[:0]
	if list, ok := e.After["columns"].([]any); ok {
		for _, c := range list {
			if m, ok := c.(map[string]any); ok {
				t.columns = append(t.columns, fmt.Sprint(m["name"]))
			}
		}
	}
}

// alterField reads one string out of a schema event's `alter` payload. `checkEnvelope` has already
// established that the required fields are present and non-empty for the op, so an empty return
// here means the caller asked for a field this op does not have.
func alterField(e *Event, key string) (string, bool) {
	alter, ok := e.After["alter"].(map[string]any)
	if !ok {
		return "", false
	}
	v, ok := alter[key].(string)
	return v, ok
}

// stringEncoded reports the feed types whose values ship as JSON strings rather than numbers.
//
// Mirrors the producer's rule in `replication::jsonl`, and the reason is the same: an i64 past 2^53
// and an unbounded decimal are both corrupted by a default float64 decode, so their digits travel
// as text. INTEGER is i32 and is deliberately NOT in this set — three orders of magnitude inside
// what a double holds exactly, and turning it into a string would break every consumer reading that
// column today.
func stringEncoded(feedType string) bool {
	switch feedType {
	case "BIGINT", "DECIMAL", "TIMESTAMP":
		return true
	}
	return false
}

// asFeedString renders a decoded JSON value the way the feed would render it as a string.
//
// `json.Number` keeps the digits verbatim, which is what makes this exact: the value is re-encoded
// rather than round-tripped through a float.
func asFeedString(v any) any {
	switch n := v.(type) {
	case nil:
		return nil
	case json.Number:
		return n.String()
	case string:
		return n
	default:
		return fmt.Sprint(v)
	}
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
	// A finite non-integer double is m/2^k in lowest terms with m odd, so m/2^k = m*5^k/10^k: its
	// exact decimal expansion TERMINATES after exactly k fractional digits. Asking `FloatString`
	// for exactly k is therefore lossless.
	//
	// A fixed 40 was not. `FloatString` ROUNDS to the requested places, so every double smaller
	// than 1e-40 came out as "0" — including every subnormal, and including pairs that are
	// provably different numbers. Printing two distinct corrupted values identically, as zero, is
	// the same understatement this function's doc exists to forbid.
	prec := r.Denom().BitLen() - 1
	s := strings.TrimRight(r.FloatString(prec), "0")
	return strings.TrimSuffix(s, ".")
}

// sameNumber reports whether a consumer decoding this JSON number as a float64 ends up holding the
// number that was actually sent.
//
// Two ways that can be true, and BOTH are needed:
//
//  1. The wire digits denote exactly the value the double holds. `1.50` on the wire and the double
//     1.5 are one number, and so is `-9223372036854775808`, which is exactly -2^63. Calling either
//     a precision loss would be false.
//
//  2. The wire digits are the double's own SHORTEST ROUND-TRIP form. Rust prints an `f64` with
//     `{}`, which emits the fewest digits that parse back to the identical double — so `0.1` is
//     not the wire being sloppy about one tenth, it is the exact NAME of the double that was sent,
//     and the consumer recovers it bit for bit. Nothing was lost end to end.
//
// Testing only (1) is what this function used to do, and it made a FLOAT column unreportable: no
// finite decimal fraction except a dyadic one equals its double exactly, so `"f":0.1` — an
// ordinary, perfectly faithful value — was reported LOSSY. A detector that fires on the common
// case teaches its reader to ignore it, which is worse than not shipping one.
//
// What survives both tests is the real thing: wire digits that no double can represent AND that
// are not any double's canonical name, so the value a consumer holds is a different number from
// the one that was sent. `9223372036854775807` is that: it is not -2^63, and the double it lands
// on is named `9.223372036854776e+18`, a third number again.
func sameNumber(wire string, held float64) bool {
	w, ok := new(big.Rat).SetString(wire)
	if !ok {
		return false
	}
	h := new(big.Rat).SetFloat64(held)
	if h == nil {
		return false
	}
	if w.Cmp(h) == 0 {
		return true
	}
	canon, ok := new(big.Rat).SetString(strconv.FormatFloat(held, 'g', -1, 64))
	return ok && canon.Cmp(w) == 0
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

		// The table this line belongs to, for the publication check below. Absent or non-string means
		// the line is not an event envelope at all, and the check refuses an unknown table anyway.
		table, _ := loose["table"].(string)

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
				// **This subcommand does not go through decodeLine**, so it does not get the
				// publication check from the envelope path - and it is the one mode that prints a
				// column NAME and its VALUE to stdout. An adversarial review found it: with the guard
				// wired only into decodeLine, `precision leaky.jsonl -publication p.txt` printed
				// `FIELD 3 ssn string 000-11-2222` and exited 0. Refused here, per field, before
				// anything is printed for it.
				if err := activePublication.refuseUnpublished(table, col, "the type report for", i+1); err != nil {
					return err
				}
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
	attributed, unattributed := 0, 0
	for i, line := range lines {
		e, err := decodeLine(line, i+1)
		if err != nil {
			return err
		}
		if e.CommitLSN < last {
			return fmt.Errorf("line %d: commit_lsn went backwards", i+1)
		}
		last = e.CommitLSN
		// Row WRITES only. A snapshot READ describes a row that existed before the feed began and a
		// schema event describes a table, so counting either would drown the number that matters.
		if isRowWrite(e.Op) {
			if e.Writer != nil {
				attributed++
			} else {
				unattributed++
			}
		}
	}
	// `OK <n>` stays the first line and stays exactly parseable: it is read by
	// `tests/integration_cdc_feed.rs`. The attribution census is a second line, because a feed that
	// ships rows attributed to nobody looks identical, line by line, to one written by no agent —
	// every record simply carries `"writer":null`, and only a count tells them apart.
	fmt.Printf("OK %d\n", len(lines))
	fmt.Printf("WRITERS attributed=%d unattributed=%d\n", attributed, unattributed)
	return nil
}

// isRowWrite reports whether an op is a change some run performed, as opposed to a snapshot
// observation or a declaration about a table's shape.
func isRowWrite(op string) bool {
	return op == "INSERT" || op == "UPDATE" || op == "DELETE"
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

// changeSink is what landing a feed needs of a destination, and nothing more.
//
// The interface exists so `runSink` below has exactly one copy of the replay bookkeeping. Two
// destinations with two hand-written loops is two places for the cursor logic to drift, and drift
// there is invisible until a replay corrupts one of them.
type changeSink interface {
	// apply writes one event, ignoring it if the destination already holds a newer one. The
	// ordering guard belongs in the implementation's SQL, not in any caller.
	apply(e *Event) error
	// The resume point is the COMPOSITE (commit_lsn, record lsn), not commit_lsn alone.
	//
	// A commit_lsn identifies a transaction, and a transaction can carry many rows — every row of a
	// multi-row statement, and every row of a backfill snapshot, shares one. Keying idempotence on it
	// alone made the first row of a commit advance the cursor to that commit, so every sibling then
	// compared `<=` and was discarded as a re-delivery. Measured on a 3-row commit: one row landed,
	// the run printed APPLIED 2 SKIPPED 2 and exited 0.
	saveCursor(table string, commitLSN, recordLSN uint64) error
	cursor(table string) (uint64, uint64)
	Close() error
}

// openDestination picks a sink by engine name, and refuses an engine it does not know.
//
// No fallback to a default: a typo in `-engine` that quietly landed the feed somewhere other than
// where the operator asked is worse than an error, because the destination they were watching stays
// empty and the one they were not fills up.
func openDestination(engine, dbPath, key string) (changeSink, error) {
	switch engine {
	case "sqlite":
		return openSink(dbPath, key)
	case "duckdb":
		return openDuckSink(dbPath, key)
	default:
		return nil, fmt.Errorf("unknown -engine %q; known engines are sqlite and duckdb", engine)
	}
}

// runSink lands a feed file into the chosen destination.
//
// Deliberately not transactional across the whole file. A sink that only becomes visible at the end
// of a batch is a sink that loses everything when it dies mid-batch, and the per-row guard already
// makes re-applying safe — so crashing part-way and being restarted is a normal, correct thing to
// do here rather than a recovery problem.
func runSink(feedPath, dbPath, key, engine string) error {
	raw, err := os.ReadFile(feedPath)
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		return errors.New("feed is empty; a sink that landed nothing has not succeeded")
	}
	sink, err := openDestination(engine, dbPath, key)
	if err != nil {
		return err
	}

	applied, skipped, cursor, lastTable, err := applyFeed(sink, string(raw))
	// Closing is part of landing the feed, not cleanup after it: the DuckDB sink checkpoints on
	// close, and a checkpoint that failed leaves a database an outside reader cannot open. Reporting
	// APPLIED over the top of that would be a green that is not one, so the close error is folded in
	// before anything is printed.
	if cerr := sink.Close(); err == nil && cerr != nil {
		err = fmt.Errorf("closing the destination: %w", cerr)
	}
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "applied %d, skipped %d re-delivered\n", applied, skipped)
	fmt.Printf("APPLIED %d SKIPPED %d CURSOR %d TABLE %s\n", applied, skipped, cursor, lastTable)
	return nil
}

// applyFeed folds every line of a feed into a sink, counting what landed and what was a
// re-delivery. Engine-independent by construction: everything engine-specific is behind changeSink.
func applyFeed(sink changeSink, raw string) (applied, skipped int, cursor uint64, lastTable string, err error) {
	lines := strings.Split(strings.TrimSuffix(raw, "\n"), "\n")
	for i, line := range lines {
		if line == "" {
			continue
		}
		e, derr := decodeLine(line, i+1)
		if derr != nil {
			return applied, skipped, cursor, lastTable, derr
		}
		// Events at or below what this table has already absorbed are re-deliveries. Counted rather
		// than hidden: "skipped 40" is how an operator sees a replay happening at all.
		//
		// Compared LEXICOGRAPHICALLY on (commit_lsn, lsn). commit_lsn alone cannot order two rows of
		// the same commit, and treating them as equal meant discarding all but the first.
		cc, cl := sink.cursor(e.Table)
		if !bypassesCursor(e.Op) && (e.CommitLSN < cc || (e.CommitLSN == cc && e.LSN <= cl)) {
			skipped++
			continue
		}
		if aerr := sink.apply(e); aerr != nil {
			return applied, skipped, cursor, lastTable, aerr
		}
		applied++
		lastTable = e.Table
		if e.CommitEndLSN > cursor {
			cursor = e.CommitEndLSN
		}
		if !bypassesCursor(e.Op) {
			if serr := sink.saveCursor(e.Table, e.CommitLSN, e.LSN); serr != nil {
				return applied, skipped, cursor, lastTable, serr
			}
		}
	}
	return applied, skipped, cursor, lastTable, nil
}

// diffAgainstSource re-materializes a table from the change events and compares it, row by row and
// column by column, against a dump of the source table.
//
// # Why this is a subcommand and not a test
//
// The consumer could already re-materialize a table and print it; the comparison was done by a Rust
// integration test holding the expected rows as a literal. That verifies the pipeline against what
// somebody typed, not against the database - and if the workload changes, the literal is what breaks.
// Here the expected side is produced by `table_dump`, which asks the source database the same
// `SELECT * FROM <table>` a user would.
//
// # Compared semantically, not byte for byte
//
// Both sides are decoded with `UseNumber()`, so numeric text is preserved exactly - which is the whole
// point for an int64 past 2^53 - and then compared per column. Comparing the two JSON documents as
// bytes would fail on key order and on any formatting difference between a Rust writer and a Go
// writer, neither of which is a data problem. What is compared is the values.
//
// The `_deleted` and `_commit_lsn` bookkeeping columns a sink adds are not part of the source table, so
// they are ignored on the re-materialized side rather than reported as extra columns.
func diffAgainstSource(feedPath, sourcePath, key string) error {
	raw, err := os.ReadFile(feedPath)
	if err != nil {
		return err
	}
	// Re-materialize with the SAME Table.apply every other mode uses. A second fold written for the
	// diff could disagree with the sink about what a DELETE means and the diff would certify it.
	table := newTable(key)
	n := 0
	for _, line := range strings.Split(string(raw), "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		n++
		e, derr := decodeLine(line, n)
		if derr != nil {
			return derr
		}
		if aerr := table.apply(e); aerr != nil {
			return aerr
		}
	}
	if n == 0 {
		return fmt.Errorf("%s carried no events; a diff against an empty feed proves nothing", feedPath)
	}

	srcBytes, err := os.ReadFile(sourcePath)
	if err != nil {
		return err
	}
	dec := json.NewDecoder(bytes.NewReader(srcBytes))
	dec.UseNumber()
	var srcRows []map[string]any
	if err := dec.Decode(&srcRows); err != nil {
		return fmt.Errorf("%s is not a JSON array of rows: %w", sourcePath, err)
	}

	// **Both sides empty is not agreement.** Two empty tables match trivially, and a pipeline that
	// delivered nothing would pass. Refuse instead, naming which side is empty.
	if len(srcRows) == 0 && len(table.rows) == 0 {
		return fmt.Errorf("both the source dump and the re-materialized table are empty; " +
			"there is nothing to agree about")
	}

	src := make(map[string]map[string]any, len(srcRows))
	for i, r := range srcRows {
		v, ok := r[key]
		if !ok {
			return fmt.Errorf("source row %d has no key column %q: %v", i, key, r)
		}
		k := fmt.Sprint(v)
		if _, dup := src[k]; dup {
			return fmt.Errorf("source dump has two rows with key %s; it is not a table state", k)
		}
		src[k] = r
	}

	// Sorted so a failure reads the same way twice.
	keys := map[string]bool{}
	for k := range src {
		keys[k] = true
	}
	for k := range table.rows {
		keys[k] = true
	}
	ordered := make([]string, 0, len(keys))
	for k := range keys {
		ordered = append(ordered, k)
	}
	sort.Strings(ordered)

	var problems []string
	for _, k := range ordered {
		s, inSource := src[k]
		d, inFeed := table.rows[k]
		switch {
		case inSource && !inFeed:
			problems = append(problems, fmt.Sprintf("%s=%s: in the source, missing from the feed", key, k))
		case !inSource && inFeed:
			problems = append(problems, fmt.Sprintf("%s=%s: rebuilt from the feed, absent from the source", key, k))
		default:
			for col, want := range s {
				got, present := d[col]
				if !present {
					problems = append(problems, fmt.Sprintf("%s=%s column %q: source has %v, the feed never carried it", key, k, col, want))
					continue
				}
				if fmt.Sprint(want) != fmt.Sprint(got) {
					problems = append(problems, fmt.Sprintf("%s=%s column %q: source %v, feed %v", key, k, col, want, got))
				}
			}
			for col := range d {
				// A sink's own bookkeeping is not a column of the source table.
				if col == "_deleted" || col == "_commit_lsn" {
					continue
				}
				if _, present := s[col]; !present {
					problems = append(problems, fmt.Sprintf("%s=%s column %q: rebuilt from the feed, not in the source", key, k, col))
				}
			}
		}
	}

	if len(problems) > 0 {
		sort.Strings(problems)
		return fmt.Errorf("the table rebuilt from %d event(s) does not match %s:\n  %s",
			n, sourcePath, strings.Join(problems, "\n  "))
	}
	fmt.Printf("MATCH %d row(s) from %d event(s)\n", len(src), n)
	return nil
}

// publicationFlagHelp is one wording for the flag, so four subcommands cannot describe it four ways.
const publicationFlagHelp = "publication declaration file; the feed is refused if it carries any " +
	"column this does not publish, or omits one it does"

// refuseTrailingArgs exits non-zero when a positional argument follows the flags.
//
// Go's flag package stops at the first non-flag argument and reports no error, so
// `validate feed.jsonl junk -publication p.txt` parsed to publication="" - the policy was never read,
// nothing was enforced, and the run printed OK and exited 0. That is the exact failure the design
// claims to prevent by making the flag the only way to switch the check on: here the flag WAS on the
// command line and not in force. Found by an adversarial review of this file.
//
// Refusing leftovers rather than warning about them, because the run that would be warned about is a
// run whose guard is off.
func refuseTrailingArgs(fs *flag.FlagSet, usage string) {
	if fs.NArg() != 0 {
		fmt.Fprintf(os.Stderr, "unexpected argument %q after the flags; a positional argument here "+
			"stops flag parsing, so -publication would be silently ignored and the feed checked "+
			"against no policy at all\nusage: cdc-consumer %s\n", fs.Arg(0), usage)
		os.Exit(2)
	}
}

// mustUsePublication installs the policy or exits. Not a warning: a run asked to enforce a policy and
// unable to read it must not land the feed anyway, because the columns it would land are exactly the
// ones somebody was trying to hold back.
func mustUsePublication(path string) {
	if err := usePublication(path); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: cdc-consumer validate <feed.jsonl> [-publication f] | follow <addr> [flags] | "+
			"sink <feed.jsonl> -db <file> [-engine sqlite|duckdb] | "+
			"retract <db> -table <t> -model-version <v> [-mode quarantine|delete] [-engine sqlite|duckdb] | "+
			"scan <db> -table <t> [-key col] [-engine sqlite|duckdb] | "+
			"diff <feed.jsonl> <source.json> [-key col] | duckdb-sql <file.duckdb> <sql>")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "validate":
		fs := flag.NewFlagSet("validate", flag.ExitOnError)
		pub := fs.String("publication", "", publicationFlagHelp)
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: cdc-consumer validate <feed.jsonl> [-publication <file>]")
			os.Exit(2)
		}
		feed := os.Args[2]
		if err := fs.Parse(os.Args[3:]); err != nil {
			os.Exit(2)
		}
		refuseTrailingArgs(fs, "validate <feed.jsonl> [-publication <file>]")
		mustUsePublication(*pub)
		if err := validate(feed); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "diff":
		fs := flag.NewFlagSet("diff", flag.ExitOnError)
		key := fs.String("key", "id", "primary key column shared by the feed and the source dump")
		pub := fs.String("publication", "", publicationFlagHelp)
		if len(os.Args) < 4 {
			fmt.Fprintln(os.Stderr, "usage: cdc-consumer diff <feed.jsonl> <source.json> [-key col]")
			os.Exit(2)
		}
		if err := fs.Parse(os.Args[4:]); err != nil {
			os.Exit(2)
		}
		refuseTrailingArgs(fs, "diff <feed.jsonl> <source.json> [-key col] [-publication <file>]")
		mustUsePublication(*pub)
		if err := diffAgainstSource(os.Args[2], os.Args[3], *key); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "precision":
		fs := flag.NewFlagSet("precision", flag.ExitOnError)
		pub := fs.String("publication", "", publicationFlagHelp)
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: cdc-consumer precision <feed.jsonl> [-publication <file>]")
			os.Exit(2)
		}
		feed := os.Args[2]
		if err := fs.Parse(os.Args[3:]); err != nil {
			os.Exit(2)
		}
		refuseTrailingArgs(fs, "precision <feed.jsonl> [-publication <file>]")
		mustUsePublication(*pub)
		if err := precision(feed); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "sink":
		fs := flag.NewFlagSet("sink", flag.ExitOnError)
		dbPath := fs.String("db", "cdc.sqlite", "destination database file")
		key := fs.String("key", "id", "primary key column")
		engine := fs.String("engine", "sqlite", "destination engine: sqlite or duckdb")
		pub := fs.String("publication", "", publicationFlagHelp)
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: cdc-consumer sink <feed.jsonl> -db <file> [-engine sqlite|duckdb]")
			os.Exit(2)
		}
		feed := os.Args[2]
		_ = fs.Parse(os.Args[3:])
		refuseTrailingArgs(fs, "sink <feed.jsonl> -db <file> [-engine sqlite|duckdb] [-publication <file>]")
		mustUsePublication(*pub)
		if err := runSink(feed, *dbPath, *key, *engine); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "duckdb-sql":
		// Running one statement against a DuckDB destination from a separate process, for machines
		// with no `duckdb` CLI. Weaker than the CLI and never a substitute for it in a check that
		// claims independence: it is the same driver and the same linked DuckDB that did the
		// writing, so it shares any misreading either has.
		if len(os.Args) != 4 {
			fmt.Fprintln(os.Stderr, "usage: cdc-consumer duckdb-sql <file.duckdb> <sql>")
			os.Exit(2)
		}
		out, err := duckSQL(os.Args[2], os.Args[3])
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println(out)
	case "follow":
		fs := flag.NewFlagSet("follow", flag.ExitOnError)
		key := fs.String("key", "id", "column to key the materialised table by")
		cursor := fs.Uint64("cursor", 0, "resume from this cursor; 0 means the start of the log")
		limit := fs.Int("limit", 0, "stop after this many events; 0 means until the server closes")
		pub := fs.String("publication", "", publicationFlagHelp)
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: cdc-consumer follow <addr> [flags]")
			os.Exit(2)
		}
		addr := os.Args[2]
		_ = fs.Parse(os.Args[3:])
		refuseTrailingArgs(fs, "follow <addr> [-key col] [-cursor n] [-limit n] [-publication <file>]")
		mustUsePublication(*pub)
		if err := follow(addr, *key, *cursor, *limit); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "retract":
		fs := flag.NewFlagSet("retract", flag.ExitOnError)
		table := fs.String("table", "", "destination table to retract from")
		version := fs.String("model-version", "", "the model_version whose rows to withdraw")
		mode := fs.String("mode", string(quarantine), "quarantine (mark only) or delete (mark and tombstone)")
		engine := fs.String("engine", "sqlite", "destination engine: sqlite or duckdb")
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr, "usage: cdc-consumer retract <db> -table <t> "+
				"-model-version <v> [-mode quarantine|delete] [-engine sqlite|duckdb]")
			os.Exit(2)
		}
		dbPath := os.Args[2]
		_ = fs.Parse(os.Args[3:])
		if *table == "" {
			fmt.Fprintln(os.Stderr, "retract needs -table")
			os.Exit(2)
		}
		if err := runRetract(dbPath, *table, *version, retractMode(*mode), *engine); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "scan":
		fs := flag.NewFlagSet("scan", flag.ExitOnError)
		table := fs.String("table", "", "destination table to scan")
		key := fs.String("key", "id", "primary key column")
		engine := fs.String("engine", "sqlite", "destination engine: sqlite or duckdb")
		if len(os.Args) < 3 {
			fmt.Fprintln(os.Stderr,
				"usage: cdc-consumer scan <db> -table <t> [-key id] [-engine sqlite|duckdb]")
			os.Exit(2)
		}
		dbPath := os.Args[2]
		_ = fs.Parse(os.Args[3:])
		if *table == "" {
			fmt.Fprintln(os.Stderr, "scan needs -table")
			os.Exit(2)
		}
		if err := runScan(dbPath, *table, *key, *engine); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown subcommand %q\n", os.Args[1])
		os.Exit(2)
	}
}
