// Package apl contains helpers for building Axiom Processing Language
// queries safely: identifier quoting, literal formatting, and a small
// pipeline builder.
package apl

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Ident renders a field or dataset name so it is always valid, using the
// bracket form for anything that isn't a plain identifier.
func Ident(name string) string {
	if isPlainIdent(name) {
		return name
	}
	return "['" + escapeSingle(name) + "']"
}

// Dataset renders a dataset reference.
func Dataset(name string) string {
	return "['" + escapeSingle(name) + "']"
}

// MapKey renders access to a key inside a map field, e.g.
// ['attributes.custom']['http.host'].
func MapKey(field, key string) string {
	return Ident(field) + "['" + escapeSingle(key) + "']"
}

// String renders a string literal.
func String(s string) string {
	return `"` + escapeDouble(s) + `"`
}

// RawString renders a verbatim (regex-safe) string literal.
func RawString(s string) string {
	// Raw strings cannot contain the quote character at all, so fall back
	// to a normal escaped string when needed.
	if strings.ContainsRune(s, '"') {
		return String(s)
	}
	return `@"` + s + `"`
}

// Int renders an integer literal.
func Int(i int64) string {
	return strconv.FormatInt(i, 10)
}

// Float renders a float literal. Non-finite values become null.
func Float(f float64) string {
	if math.IsNaN(f) || math.IsInf(f, 0) {
		return "real(null)"
	}
	s := strconv.FormatFloat(f, 'f', -1, 64)
	if !strings.ContainsAny(s, ".eE") {
		s += ".0"
	}
	return s
}

// Bool renders a boolean literal.
func Bool(b bool) string {
	if b {
		return "true"
	}
	return "false"
}

// Timespan renders a duration as an APL timespan literal. It uses the
// coarsest unit that represents the value exactly, so 1500ms becomes
// 1500ms rather than 1.5s.
func Timespan(d time.Duration) string {
	if d == 0 {
		return "0s"
	}
	neg := d < 0
	if neg {
		d = -d
	}
	var s string
	switch {
	case d%(24*time.Hour) == 0:
		s = strconv.FormatInt(int64(d/(24*time.Hour)), 10) + "d"
	case d%time.Hour == 0:
		s = strconv.FormatInt(int64(d/time.Hour), 10) + "h"
	case d%time.Minute == 0:
		s = strconv.FormatInt(int64(d/time.Minute), 10) + "m"
	case d%time.Second == 0:
		s = strconv.FormatInt(int64(d/time.Second), 10) + "s"
	case d%time.Millisecond == 0:
		s = strconv.FormatInt(int64(d/time.Millisecond), 10) + "ms"
	case d%time.Microsecond == 0:
		s = strconv.FormatInt(int64(d/time.Microsecond), 10) + "microsecond"
	default:
		s = "totimespan(" + strconv.FormatInt(int64(d), 10) + ")"
	}
	if neg {
		return "-" + s
	}
	return s
}

// Datetime renders a datetime literal.
func Datetime(t time.Time) string {
	return "datetime(" + t.UTC().Format(time.RFC3339Nano) + ")"
}

// Call renders a function call.
func Call(fn string, args ...string) string {
	return fn + "(" + strings.Join(args, ", ") + ")"
}

// And joins non-empty expressions with "and", parenthesising each.
func And(exprs ...string) string {
	return join(" and ", exprs)
}

// Or joins non-empty expressions with "or", parenthesising each.
func Or(exprs ...string) string {
	return join(" or ", exprs)
}

// Not negates an expression.
func Not(expr string) string {
	return "not(" + expr + ")"
}

func join(sep string, exprs []string) string {
	parts := make([]string, 0, len(exprs))
	for _, e := range exprs {
		if e == "" {
			continue
		}
		parts = append(parts, e)
	}
	switch len(parts) {
	case 0:
		return ""
	case 1:
		return parts[0]
	}
	for i, p := range parts {
		parts[i] = "(" + p + ")"
	}
	return strings.Join(parts, sep)
}

// Query is a simple pipeline builder.
type Query struct {
	dataset string
	stages  []string
}

// NewQuery starts a query against a dataset.
func NewQuery(dataset string) *Query {
	return &Query{dataset: dataset}
}

// Where appends a where stage. Empty expressions are skipped.
func (q *Query) Where(expr string) *Query {
	if expr != "" {
		q.stages = append(q.stages, "where "+expr)
	}
	return q
}

// Extend appends an extend stage.
func (q *Query) Extend(assignments ...string) *Query {
	if len(assignments) > 0 {
		q.stages = append(q.stages, "extend "+strings.Join(assignments, ", "))
	}
	return q
}

// Project appends a project stage.
func (q *Query) Project(cols ...string) *Query {
	if len(cols) > 0 {
		q.stages = append(q.stages, "project "+strings.Join(cols, ", "))
	}
	return q
}

// Summarize appends a summarize stage.
func (q *Query) Summarize(aggs []string, by []string) *Query {
	s := "summarize"
	if len(aggs) > 0 {
		s += " " + strings.Join(aggs, ", ")
	}
	if len(by) > 0 {
		s += " by " + strings.Join(by, ", ")
	}
	q.stages = append(q.stages, s)
	return q
}

// Sort appends a sort stage. Each key is "expr asc|desc".
func (q *Query) Sort(keys ...string) *Query {
	if len(keys) > 0 {
		q.stages = append(q.stages, "sort by "+strings.Join(keys, ", "))
	}
	return q
}

// Top appends a top stage.
func (q *Query) Top(n int, by string) *Query {
	q.stages = append(q.stages, fmt.Sprintf("top %d by %s", n, by))
	return q
}

// Limit appends a limit stage.
func (q *Query) Limit(n int) *Query {
	q.stages = append(q.stages, "limit "+strconv.Itoa(n))
	return q
}

// Raw appends a verbatim stage.
func (q *Query) Raw(stage string) *Query {
	if stage != "" {
		q.stages = append(q.stages, stage)
	}
	return q
}

// String renders the query with one stage per line.
func (q *Query) String() string {
	var b strings.Builder
	b.WriteString(Dataset(q.dataset))
	for _, s := range q.stages {
		b.WriteString("\n| ")
		b.WriteString(s)
	}
	return b.String()
}

func isPlainIdent(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
			if i == 0 {
				return false
			}
		default:
			return false
		}
	}
	return !reserved[strings.ToLower(s)]
}

var reserved = map[string]bool{
	"and": true, "or": true, "not": true, "by": true, "where": true,
	"project": true, "extend": true, "summarize": true, "sort": true,
	"order": true, "limit": true, "take": true, "top": true, "count": true,
	"distinct": true, "union": true, "join": true, "let": true, "true": true,
	"false": true, "null": true, "in": true, "contains": true, "has": true,
	"startswith": true, "endswith": true, "matches": true, "regex": true,
	"asc": true, "desc": true, "on": true, "kind": true, "parse": true,
	"with": true, "between": true,
}

func escapeSingle(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `'`, `\'`)
}

func escapeDouble(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch r {
		case '\\':
			b.WriteString(`\\`)
		case '"':
			b.WriteString(`\"`)
		case '\n':
			b.WriteString(`\n`)
		case '\r':
			b.WriteString(`\r`)
		case '\t':
			b.WriteString(`\t`)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}
