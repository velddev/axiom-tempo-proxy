package axiom

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Result is the tabular response of an APL query.
type Result struct {
	// APL is the query that produced this result. Set by the client.
	APL string `json:"-"`

	Format       string                    `json:"format"`
	DatasetNames []string                  `json:"datasetNames"`
	FieldsMeta   map[string][]DatasetField `json:"fieldsMetaMap"`
	Tables       []Table                   `json:"tables"`
	Status       Status                    `json:"status"`
}

// Status carries query execution metadata.
type Status struct {
	ElapsedTime    int64           `json:"elapsedTime"`
	BlocksExamined int64           `json:"blocksExamined"`
	RowsExamined   int64           `json:"rowsExamined"`
	RowsMatched    int64           `json:"rowsMatched"`
	IsPartial      bool            `json:"isPartial"`
	IsEstimate     bool            `json:"isEstimate"`
	Messages       []StatusMessage `json:"messages"`
}

// StatusMessage is a warning or notice attached to a query result.
type StatusMessage struct {
	Code     string `json:"code"`
	Msg      string `json:"msg"`
	Priority string `json:"priority"`
	Count    int    `json:"count"`
}

// Table is one result table. Columns are column-major: Columns[i][row]
// holds the value of Fields[i] for that row.
type Table struct {
	Name    string              `json:"name"`
	Fields  []Field             `json:"fields"`
	Order   []Order             `json:"order"`
	Groups  []Group             `json:"groups"`
	Range   *Range              `json:"range"`
	Buckets *Buckets            `json:"buckets"`
	Columns [][]json.RawMessage `json:"columns"`

	index map[string]int
}

// Field describes a result column.
type Field struct {
	Name   string `json:"name"`
	Type   string `json:"type"`
	Hidden bool   `json:"hidden"`
}

// Order is a sort key on the table.
type Order struct {
	Field string `json:"field"`
	Desc  bool   `json:"desc"`
}

// Group is a grouping key on the table.
type Group struct {
	Name string `json:"name"`
}

// Range is the effective time range of the query.
type Range struct {
	Field string `json:"field"`
	Start string `json:"start"`
	End   string `json:"end"`
}

// Buckets describes time bucketing applied to the table.
type Buckets struct {
	Field string `json:"field"`
	Size  int64  `json:"size"`
}

// FirstTable returns the first table or nil.
func (r *Result) FirstTable() *Table {
	if r == nil || len(r.Tables) == 0 {
		return nil
	}
	return &r.Tables[0]
}

// NumRows returns the number of rows in the table.
func (t *Table) NumRows() int {
	if t == nil || len(t.Columns) == 0 {
		return 0
	}
	return len(t.Columns[0])
}

// ColumnIndex returns the index of the named field, or -1.
func (t *Table) ColumnIndex(name string) int {
	if t == nil {
		return -1
	}
	if t.index == nil {
		t.index = make(map[string]int, len(t.Fields))
		for i, f := range t.Fields {
			t.index[f.Name] = i
		}
	}
	if i, ok := t.index[name]; ok {
		return i
	}
	return -1
}

// Row returns a view over row i.
func (t *Table) Row(i int) Row {
	return Row{t: t, i: i}
}

// Rows returns all rows.
func (t *Table) Rows() []Row {
	n := t.NumRows()
	rows := make([]Row, n)
	for i := range n {
		rows[i] = Row{t: t, i: i}
	}
	return rows
}

// Row is a view over one row of a Table.
type Row struct {
	t *Table
	i int
}

// Raw returns the raw JSON value of the named column, or nil when the
// column is missing or the value is JSON null.
func (r Row) Raw(name string) json.RawMessage {
	idx := r.t.ColumnIndex(name)
	if idx < 0 || idx >= len(r.t.Columns) || r.i >= len(r.t.Columns[idx]) {
		return nil
	}
	v := r.t.Columns[idx][r.i]
	if isNull(v) {
		return nil
	}
	return v
}

// Has reports whether the named column exists and is non-null.
func (r Row) Has(name string) bool {
	return r.Raw(name) != nil
}

// String returns the column as a string. Non-string scalars are
// formatted; objects and arrays are returned as compact JSON.
func (r Row) String(name string) string {
	v := r.Raw(name)
	if v == nil {
		return ""
	}
	if v[0] == '"' {
		var s string
		if err := json.Unmarshal(v, &s); err == nil {
			return s
		}
	}
	return string(v)
}

// Int64 returns the column as an int64. Floats are truncated, numeric
// strings are parsed. Missing or unparsable values yield 0.
func (r Row) Int64(name string) int64 {
	v, _ := r.TryInt64(name)
	return v
}

// TryInt64 is like Int64 but reports success.
func (r Row) TryInt64(name string) (int64, bool) {
	v := r.Raw(name)
	if v == nil {
		return 0, false
	}
	s := strings.Trim(string(v), `"`)
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, true
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(f), true
	}
	return 0, false
}

// Float64 returns the column as a float64, 0 when absent.
func (r Row) Float64(name string) float64 {
	v, _ := r.TryFloat64(name)
	return v
}

// TryFloat64 is like Float64 but reports success.
func (r Row) TryFloat64(name string) (float64, bool) {
	v := r.Raw(name)
	if v == nil {
		return 0, false
	}
	s := strings.Trim(string(v), `"`)
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, false
	}
	return f, true
}

// Bool returns the column as a bool.
func (r Row) Bool(name string) bool {
	v := r.Raw(name)
	if v == nil {
		return false
	}
	switch strings.Trim(string(v), `"`) {
	case "true", "1":
		return true
	}
	return false
}

// Time parses the column as an RFC3339 datetime. The zero time is
// returned when absent or unparsable.
func (r Row) Time(name string) time.Time {
	v := r.Raw(name)
	if v == nil {
		return time.Time{}
	}
	var s string
	if err := json.Unmarshal(v, &s); err != nil {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return time.Time{}
	}
	return t
}

// Duration parses the column as an Axiom timespan. Axiom encodes
// timespans as Go-style duration strings; bare numbers are treated as
// nanoseconds.
func (r Row) Duration(name string) time.Duration {
	v := r.Raw(name)
	if v == nil {
		return 0
	}
	if v[0] == '"' {
		var s string
		if err := json.Unmarshal(v, &s); err != nil {
			return 0
		}
		if d, err := time.ParseDuration(s); err == nil {
			return d
		}
		if n, err := strconv.ParseInt(s, 10, 64); err == nil {
			return time.Duration(n)
		}
		return 0
	}
	if n, err := strconv.ParseInt(string(v), 10, 64); err == nil {
		return time.Duration(n)
	}
	if f, err := strconv.ParseFloat(string(v), 64); err == nil {
		return time.Duration(f)
	}
	return 0
}

// Unmarshal decodes the column into v.
func (r Row) Unmarshal(name string, v any) error {
	raw := r.Raw(name)
	if raw == nil {
		return fmt.Errorf("axiom: column %q is null or missing", name)
	}
	return json.Unmarshal(raw, v)
}

// Object decodes the column as a JSON object. A nil map is returned when
// the column is absent or not an object.
func (r Row) Object(name string) map[string]json.RawMessage {
	raw := r.Raw(name)
	if raw == nil || raw[0] != '{' {
		return nil
	}
	var m map[string]json.RawMessage
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil
	}
	return m
}

func isNull(v json.RawMessage) bool {
	return len(v) == 0 || (len(v) == 4 && string(v) == "null")
}
