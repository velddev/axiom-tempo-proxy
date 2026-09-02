package translate

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/tempo/pkg/traceql"
)

// Tempo's traceql package exposes the parsed metrics stage only through
// unexported fields, so this file parses the metrics tail of a query
// itself. The full query is always validated by traceql.Parse first, so
// the parser here can assume well-formed input.

// MetricsFunc is a TraceQL metrics aggregation.
type MetricsFunc string

const (
	FuncRate              MetricsFunc = "rate"
	FuncCountOverTime     MetricsFunc = "count_over_time"
	FuncMinOverTime       MetricsFunc = "min_over_time"
	FuncMaxOverTime       MetricsFunc = "max_over_time"
	FuncAvgOverTime       MetricsFunc = "avg_over_time"
	FuncSumOverTime       MetricsFunc = "sum_over_time"
	FuncQuantileOverTime  MetricsFunc = "quantile_over_time"
	FuncHistogramOverTime MetricsFunc = "histogram_over_time"
	FuncCompare           MetricsFunc = "compare"
)

var metricsFuncs = map[string]MetricsFunc{
	"rate":                FuncRate,
	"count_over_time":     FuncCountOverTime,
	"min_over_time":       FuncMinOverTime,
	"max_over_time":       FuncMaxOverTime,
	"avg_over_time":       FuncAvgOverTime,
	"sum_over_time":       FuncSumOverTime,
	"quantile_over_time":  FuncQuantileOverTime,
	"histogram_over_time": FuncHistogramOverTime,
	"compare":             FuncCompare,
}

// IsMetricsQuery reports whether the query contains a metrics stage.
func IsMetricsQuery(q string) bool {
	parts := splitTopLevel(stripHints(q), '|')
	for _, p := range parts[1:] {
		name, _, ok := callParts(strings.TrimSpace(p))
		if ok {
			if _, isMetric := metricsFuncs[name]; isMetric {
				return true
			}
		}
	}
	// Math expressions start with a parenthesised sub-query.
	trimmed := strings.TrimSpace(parts[0])
	if strings.HasPrefix(trimmed, "(") {
		inner, _ := matchGroup(trimmed, 0)
		return IsMetricsQuery(inner)
	}
	return false
}

// SecondStage is a topk/bottomk or value filter applied to the series.
type SecondStage struct {
	// Op is "topk", "bottomk", or a comparison operator.
	Op string
	// N is the k for topk/bottomk.
	N int
	// Value is the threshold for comparisons.
	Value float64
}

// CompareArgs are the arguments of compare().
type CompareArgs struct {
	Selection traceql.FieldExpression
	TopN      int
	StartNs   int64
	EndNs     int64
}

// MetricsQuery is a parsed TraceQL metrics query.
type MetricsQuery struct {
	// Raw is the original query text.
	Raw string
	// Math is set for arithmetic between two metrics queries. The other
	// fields are then unused.
	Math *MathExpr

	// Pipeline is the spanset pipeline preceding the metrics function.
	Pipeline traceql.Pipeline
	// Filter is the combined spanset filter expression of Pipeline, when
	// the pipeline is a plain filter (see SpansetFilters).
	Filter traceql.FieldExpression
	// FilterExact is false when Pipeline contains stages beyond filters.
	FilterExact bool

	Func      MetricsFunc
	Attr      traceql.Attribute
	HasAttr   bool
	Quantiles []float64
	By        []traceql.Attribute
	Compare   *CompareArgs

	SecondStage []SecondStage
	Hints       map[string]string
}

// MathExpr is `(A) op (B)` or `(A) op scalar`.
type MathExpr struct {
	Op     string
	LHS    *MetricsQuery
	RHS    *MetricsQuery
	LHSNum *float64
	RHSNum *float64
}

// Sample reports whether the with(sample=...) hint was given.
func (m *MetricsQuery) Sample() bool {
	v, ok := m.Hints["sample"]
	return ok && v != "false"
}

// ParseMetrics parses a TraceQL metrics query. The query is validated by
// Tempo's parser first so syntax errors surface with Tempo's messages.
func ParseMetrics(q string) (*MetricsQuery, error) {
	if _, err := traceql.Parse(q); err != nil {
		return nil, err
	}
	return parseMetrics(q)
}

func parseMetrics(q string) (*MetricsQuery, error) {
	body, hints := splitHints(q)
	body = strings.TrimSpace(body)
	mq := &MetricsQuery{Raw: q, Hints: hints}

	// Math expression: top-level arithmetic between groups.
	if strings.HasPrefix(body, "(") {
		if me, ok, err := parseMath(body); err != nil {
			return nil, err
		} else if ok {
			mq.Math = me
			return mq, nil
		}
		// A single parenthesised query without arithmetic.
		inner, end := matchGroup(body, 0)
		if strings.TrimSpace(body[end+1:]) == "" {
			sub, err := parseMetrics(inner)
			if err != nil {
				return nil, err
			}
			for k, v := range hints {
				sub.Hints[k] = v
			}
			sub.Raw = q
			return sub, nil
		}
	}

	parts := splitTopLevel(body, '|')
	aggIdx := -1
	for i := 1; i < len(parts); i++ {
		name, _, ok := callParts(strings.TrimSpace(parts[i]))
		if ok {
			if _, isMetric := metricsFuncs[name]; isMetric {
				aggIdx = i
				break
			}
		}
	}
	if aggIdx < 0 {
		return nil, fmt.Errorf("%w: no metrics aggregation in query", ErrUnsupported)
	}

	pipelineText := strings.TrimSpace(strings.Join(parts[:aggIdx], "|"))
	root, err := traceql.Parse(pipelineText)
	if err != nil {
		return nil, fmt.Errorf("parse spanset pipeline: %w", err)
	}
	p, ok := root.SinglePipeline()
	if !ok {
		return nil, fmt.Errorf("%w: multiple pipelines", ErrUnsupported)
	}
	mq.Pipeline = p
	filters, exact := SpansetFilters(p)
	mq.FilterExact = exact
	mq.Filter = andAll(filters)

	if err := mq.parseAggregate(strings.TrimSpace(parts[aggIdx])); err != nil {
		return nil, err
	}
	for _, rest := range parts[aggIdx+1:] {
		if err := mq.parseSecondStage(strings.TrimSpace(rest)); err != nil {
			return nil, err
		}
	}
	return mq, nil
}

func andAll(filters []traceql.FieldExpression) traceql.FieldExpression {
	if len(filters) == 0 {
		return traceql.NewStaticBool(true)
	}
	expr := filters[0]
	for _, f := range filters[1:] {
		expr = &traceql.BinaryOperation{Op: traceql.OpAnd, LHS: expr, RHS: f}
	}
	return expr
}

// parseAggregate parses `func(args) [by(a, b)] [cmp value]`.
func (mq *MetricsQuery) parseAggregate(s string) error {
	name, args, ok := callParts(s)
	if !ok {
		return fmt.Errorf("%w: bad aggregation %q", ErrUnsupported, s)
	}
	mq.Func = metricsFuncs[name]
	_, end := matchGroup(s, strings.Index(s, "("))
	rest := strings.TrimSpace(s[end+1:])

	switch mq.Func {
	case FuncRate, FuncCountOverTime:
		if strings.TrimSpace(args) != "" {
			return fmt.Errorf("%s takes no arguments", name)
		}
	case FuncMinOverTime, FuncMaxOverTime, FuncAvgOverTime, FuncSumOverTime, FuncHistogramOverTime:
		attr, err := parseAttribute(strings.TrimSpace(args))
		if err != nil {
			return err
		}
		mq.Attr, mq.HasAttr = attr, true
	case FuncQuantileOverTime:
		list := splitTopLevel(args, ',')
		if len(list) < 2 {
			return fmt.Errorf("quantile_over_time needs an attribute and at least one quantile")
		}
		attr, err := parseAttribute(strings.TrimSpace(list[0]))
		if err != nil {
			return err
		}
		mq.Attr, mq.HasAttr = attr, true
		for _, q := range list[1:] {
			f, err := strconv.ParseFloat(strings.TrimSpace(q), 64)
			if err != nil {
				return fmt.Errorf("bad quantile %q: %w", q, err)
			}
			mq.Quantiles = append(mq.Quantiles, f)
		}
	case FuncCompare:
		list := splitTopLevel(args, ',')
		if len(list) == 0 {
			return fmt.Errorf("compare needs a spanset filter")
		}
		sel, err := ParseFilter(strings.TrimSpace(list[0]))
		if err != nil {
			return fmt.Errorf("compare selection: %w", err)
		}
		c := &CompareArgs{Selection: sel, TopN: 10}
		if len(list) > 1 {
			n, err := strconv.Atoi(strings.TrimSpace(list[1]))
			if err != nil {
				return fmt.Errorf("compare topN: %w", err)
			}
			c.TopN = n
		}
		if len(list) > 3 {
			st, err := strconv.ParseInt(strings.TrimSpace(list[2]), 10, 64)
			if err != nil {
				return fmt.Errorf("compare start: %w", err)
			}
			en, err := strconv.ParseInt(strings.TrimSpace(list[3]), 10, 64)
			if err != nil {
				return fmt.Errorf("compare end: %w", err)
			}
			c.StartNs, c.EndNs = st, en
		}
		mq.Compare = c
	}

	// Optional by(...)
	if strings.HasPrefix(rest, "by") {
		open := strings.Index(rest, "(")
		if open < 0 {
			return fmt.Errorf("bad by clause %q", rest)
		}
		inner, end := matchGroup(rest, open)
		for _, a := range splitTopLevel(inner, ',') {
			a = strings.TrimSpace(a)
			if a == "" {
				continue
			}
			attr, err := parseAttribute(a)
			if err != nil {
				return err
			}
			mq.By = append(mq.By, attr)
		}
		rest = strings.TrimSpace(rest[end+1:])
	}

	// Optional trailing comparison: rate() > 5
	if rest != "" {
		return mq.parseSecondStage(rest)
	}
	return nil
}

func (mq *MetricsQuery) parseSecondStage(s string) error {
	if s == "" {
		return nil
	}
	if name, args, ok := callParts(s); ok && (name == "topk" || name == "bottomk") {
		n, err := strconv.Atoi(strings.TrimSpace(args))
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		mq.SecondStage = append(mq.SecondStage, SecondStage{Op: name, N: n})
		_, end := matchGroup(s, strings.Index(s, "("))
		return mq.parseSecondStage(strings.TrimSpace(s[end+1:]))
	}
	for _, op := range []string{">=", "<=", "!=", ">", "<", "="} {
		if strings.HasPrefix(s, op) {
			valText := strings.TrimSpace(s[len(op):])
			// The value may be followed by another stage.
			endIdx := strings.IndexAny(valText, " |")
			numText := valText
			rest := ""
			if endIdx >= 0 {
				numText = valText[:endIdx]
				rest = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(valText[endIdx:]), "|"))
			}
			v, err := parseScalar(numText)
			if err != nil {
				return err
			}
			if op == "=" {
				op = "=="
			}
			mq.SecondStage = append(mq.SecondStage, SecondStage{Op: op, Value: v})
			return mq.parseSecondStage(rest)
		}
	}
	return fmt.Errorf("%w: unrecognised metrics stage %q", ErrUnsupported, s)
}

func parseScalar(s string) (float64, error) {
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		return f, nil
	}
	ns, err := ParseDurationLiteral(s)
	if err != nil {
		return 0, fmt.Errorf("bad scalar %q", s)
	}
	return float64(ns) / float64(time.Second), nil
}

func parseMath(body string) (*MathExpr, bool, error) {
	_, end := matchGroup(body, 0)
	if end < 0 {
		return nil, false, fmt.Errorf("unbalanced parentheses in %q", body)
	}
	rest := strings.TrimSpace(body[end+1:])
	if rest == "" {
		return nil, false, nil
	}
	op := rest[:1]
	if !strings.ContainsAny(op, "+-*/") {
		return nil, false, nil
	}
	rhsText := strings.TrimSpace(rest[1:])
	me := &MathExpr{Op: op}
	lhs, err := parseMetrics(body[:end+1])
	if err != nil {
		return nil, false, err
	}
	me.LHS = lhs
	if strings.HasPrefix(rhsText, "(") {
		rhs, err := parseMetrics(rhsText)
		if err != nil {
			return nil, false, err
		}
		me.RHS = rhs
	} else {
		f, err := strconv.ParseFloat(rhsText, 64)
		if err != nil {
			return nil, false, fmt.Errorf("bad scalar %q in math expression", rhsText)
		}
		me.RHSNum = &f
	}
	return me, true, nil
}

func parseAttribute(s string) (traceql.Attribute, error) {
	root, err := traceql.Parse("{ " + s + " != nil }")
	if err != nil {
		return traceql.Attribute{}, fmt.Errorf("bad attribute %q: %w", s, err)
	}
	p, _ := root.SinglePipeline()
	filters, _ := SpansetFilters(p)
	if len(filters) != 1 {
		return traceql.Attribute{}, fmt.Errorf("bad attribute %q", s)
	}
	var lhs traceql.FieldExpression
	switch v := filters[0].(type) {
	case *traceql.BinaryOperation:
		lhs = v.LHS
	case *traceql.UnaryOperation:
		lhs = v.Expression
	case traceql.UnaryOperation:
		lhs = v.Expression
	default:
		return traceql.Attribute{}, fmt.Errorf("bad attribute %q", s)
	}
	attr, ok := lhs.(traceql.Attribute)
	if !ok {
		return traceql.Attribute{}, fmt.Errorf("bad attribute %q", s)
	}
	return attr, nil
}

// --- text scanning helpers ---

// splitHints separates a trailing with(...) hints clause.
func splitHints(q string) (string, map[string]string) {
	hints := map[string]string{}
	body := strings.TrimSpace(q)
	for {
		idx := lastTopLevelWith(body)
		if idx < 0 {
			return body, hints
		}
		open := strings.Index(body[idx:], "(")
		if open < 0 {
			return body, hints
		}
		inner, end := matchGroup(body, idx+open)
		if end < 0 || strings.TrimSpace(body[end+1:]) != "" {
			return body, hints
		}
		for _, kv := range splitTopLevel(inner, ',') {
			k, v, ok := strings.Cut(kv, "=")
			if ok {
				hints[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"`)
			}
		}
		body = strings.TrimSpace(body[:idx])
	}
}

func stripHints(q string) string {
	body, _ := splitHints(q)
	return body
}

// lastTopLevelWith finds the start of a trailing `with(` at depth 0.
func lastTopLevelWith(s string) int {
	depth := 0
	inStr := false
	last := -1
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inStr:
			if c == '\\' {
				i++
			} else if c == '"' {
				inStr = false
			}
		case c == '"':
			inStr = true
		case c == '(' || c == '{' || c == '[':
			depth++
		case c == ')' || c == '}' || c == ']':
			depth--
		case depth == 0 && strings.HasPrefix(s[i:], "with") && (i == 0 || !isIdentByte(s[i-1])):
			j := i + 4
			for j < len(s) && s[j] == ' ' {
				j++
			}
			if j < len(s) && s[j] == '(' {
				last = i
			}
		}
	}
	return last
}

// splitTopLevel splits on sep outside of brackets and strings.
func splitTopLevel(s string, sep byte) []string {
	var parts []string
	depth := 0
	inStr := false
	start := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case inStr:
			if c == '\\' {
				i++
			} else if c == '"' {
				inStr = false
			}
		case c == '"':
			inStr = true
		case c == '(' || c == '{' || c == '[':
			depth++
		case c == ')' || c == '}' || c == ']':
			depth--
		case c == sep && depth == 0:
			// `||` is an operator, not a pipe.
			if sep == '|' && ((i+1 < len(s) && s[i+1] == '|') || (i > 0 && s[i-1] == '|')) {
				continue
			}
			parts = append(parts, s[start:i])
			start = i + 1
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// matchGroup returns the text inside the bracket opening at index open and
// the index of the matching close bracket.
func matchGroup(s string, open int) (string, int) {
	if open < 0 || open >= len(s) {
		return "", -1
	}
	depth := 0
	inStr := false
	for i := open; i < len(s); i++ {
		c := s[i]
		switch {
		case inStr:
			if c == '\\' {
				i++
			} else if c == '"' {
				inStr = false
			}
		case c == '"':
			inStr = true
		case c == '(' || c == '{' || c == '[':
			depth++
		case c == ')' || c == '}' || c == ']':
			depth--
			if depth == 0 {
				return s[open+1 : i], i
			}
		}
	}
	return "", -1
}

// callParts splits `name(args)` returning ok=false when s does not start
// with a call.
func callParts(s string) (name, args string, ok bool) {
	i := 0
	for i < len(s) && isIdentByte(s[i]) {
		i++
	}
	if i == 0 {
		return "", "", false
	}
	name = s[:i]
	rest := strings.TrimLeft(s[i:], " ")
	if !strings.HasPrefix(rest, "(") {
		return "", "", false
	}
	args, end := matchGroup(rest, 0)
	if end < 0 {
		return "", "", false
	}
	return name, args, true
}

func isIdentByte(c byte) bool {
	return c == '_' || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9')
}
