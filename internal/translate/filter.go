// Package translate converts TraceQL expressions into APL.
package translate

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/grafana/tempo/pkg/traceql"

	"github.com/velddev/axiom-tempo-proxy/internal/apl"
	"github.com/velddev/axiom-tempo-proxy/internal/schema"
)

// ErrUnsupported is returned when an expression cannot be expressed in APL
// at all.
var ErrUnsupported = errors.New("unsupported in APL")

// Filter is the APL rendering of a TraceQL spanset filter.
type Filter struct {
	// Where is the APL predicate. Empty means "match everything".
	Where string
	// Exact is true when Where matches exactly the spans the TraceQL
	// filter matches. When false, Where is a superset (a prefilter) and
	// the caller must re-evaluate the TraceQL expression on the rows.
	Exact bool
	// Unsupported lists the sub-expressions that were relaxed.
	Unsupported []string
}

// Translator renders TraceQL field expressions against a schema mapping.
type Translator struct {
	m *schema.Mapping
}

// New creates a Translator.
func New(m *schema.Mapping) *Translator {
	return &Translator{m: m}
}

// Mapping returns the schema mapping used by the translator.
func (t *Translator) Mapping() *schema.Mapping { return t.m }

// Filter translates a spanset filter expression into a where predicate.
// Parts that cannot be pushed down are relaxed (treated as true) so the
// result is always a valid prefilter; Exact reports whether nothing was
// relaxed.
func (t *Translator) Filter(e traceql.FieldExpression) Filter {
	f := Filter{Exact: true}
	expr, ok := t.pred(e, &f)
	if !ok {
		f.Exact = false
		expr = ""
	}
	if expr == "true" {
		expr = ""
	}
	f.Where = expr
	return f
}

// pred renders a boolean expression. ok=false means the whole expression
// had to be relaxed to true.
func (t *Translator) pred(e traceql.FieldExpression, f *Filter) (string, bool) {
	switch v := e.(type) {
	case traceql.Static:
		if b, ok := v.Bool(); ok {
			return apl.Bool(b), true
		}
		return t.relax(f, v.String())
	case *traceql.BinaryOperation:
		return t.binary(v, f)
	case *traceql.UnaryOperation:
		return t.unary(v, f)
	case traceql.UnaryOperation:
		return t.unary(&v, f)
	case traceql.Attribute:
		// A bare boolean attribute: { span.flag }
		col, ok := t.m.Resolve(v)
		if !ok {
			return t.relax(f, v.String())
		}
		if col.Missing {
			return "false", true
		}
		return cast(col, schema.TypeBool) + " == true", true
	}
	return t.relax(f, fmt.Sprintf("%v", e))
}

func (t *Translator) relax(f *Filter, what string) (string, bool) {
	f.Exact = false
	f.Unsupported = append(f.Unsupported, what)
	return "", false
}

func (t *Translator) unary(u *traceql.UnaryOperation, f *Filter) (string, bool) {
	switch u.Op {
	case traceql.OpExists, traceql.OpNotExists:
		return t.exists(&traceql.BinaryOperation{Op: u.Op, LHS: u.Expression, RHS: traceql.NewStaticNil()}, f)
	case traceql.OpNot:
		inner, ok := t.pred(u.Expression, f)
		if !ok {
			// not(unknown) is unknown; relax to true.
			return "", false
		}
		return apl.Not(inner), true
	case traceql.OpSub:
		val, _, ok := t.value(u.Expression, f)
		if !ok {
			return "", false
		}
		return "-(" + val + ")", true
	}
	return t.relax(f, u.String())
}

func (t *Translator) binary(b *traceql.BinaryOperation, f *Filter) (string, bool) {
	switch b.Op {
	case traceql.OpAnd:
		// Unknown conjuncts are dropped: the result is a superset.
		var parts []string
		for _, e := range flatten(b, traceql.OpAnd) {
			p, ok := t.pred(e, f)
			if !ok || p == "true" {
				continue
			}
			parts = append(parts, p)
		}
		if len(parts) == 0 {
			return "", false
		}
		return apl.And(parts...), true
	case traceql.OpOr:
		// Any unknown disjunct makes the whole disjunction unknown.
		var parts []string
		for _, e := range flatten(b, traceql.OpOr) {
			p, ok := t.pred(e, f)
			if !ok {
				return "", false
			}
			parts = append(parts, p)
		}
		return apl.Or(parts...), true
	case traceql.OpEqual, traceql.OpNotEqual, traceql.OpGreater, traceql.OpGreaterEqual,
		traceql.OpLess, traceql.OpLessEqual, traceql.OpRegex, traceql.OpNotRegex:
		return t.comparison(b, f)
	case traceql.OpExists, traceql.OpNotExists:
		return t.exists(b, f)
	}
	// Arithmetic at the boolean level is a type error in TraceQL; relax.
	return t.relax(f, b.String())
}

// flatten gathers the operands of nested same-operator binary nodes.
func flatten(b *traceql.BinaryOperation, op traceql.Operator) []traceql.FieldExpression {
	var out []traceql.FieldExpression
	var walk func(e traceql.FieldExpression)
	walk = func(e traceql.FieldExpression) {
		if bin, ok := e.(*traceql.BinaryOperation); ok && bin.Op == op {
			walk(bin.LHS)
			walk(bin.RHS)
			return
		}
		out = append(out, e)
	}
	walk(b)
	return out
}

// exists handles attr != nil / attr = nil.
func (t *Translator) exists(b *traceql.BinaryOperation, f *Filter) (string, bool) {
	attr, ok := b.LHS.(traceql.Attribute)
	if !ok {
		return t.relax(f, b.String())
	}
	if special, handled, ok := t.specialIntrinsic(attr, b.Op, traceql.NewStaticNil()); handled {
		if !ok {
			return t.relax(f, b.String())
		}
		return special, true
	}
	col, ok := t.m.Resolve(attr)
	if !ok {
		return t.relax(f, b.String())
	}
	if col.Missing {
		// The attribute never exists in this dataset.
		return apl.Bool(b.Op == traceql.OpNotExists), true
	}
	if b.Op == traceql.OpExists {
		return apl.Call("isnotnull", col.Expr), true
	}
	return apl.Call("isnull", col.Expr), true
}

// missingAttribute reports whether an operand is an attribute known to be
// absent from the dataset. Comparisons involving it can never match.
func (t *Translator) missingAttribute(e traceql.FieldExpression) bool {
	attr, ok := e.(traceql.Attribute)
	if !ok {
		return false
	}
	col, ok := t.m.Resolve(attr)
	return ok && col.Missing
}

func (t *Translator) comparison(b *traceql.BinaryOperation, f *Filter) (string, bool) {
	// Normalise so a static, if any, is on the right.
	lhs, rhs, op := b.LHS, b.RHS, b.Op
	if _, isStatic := lhs.(traceql.Static); isStatic {
		if _, alsoStatic := rhs.(traceql.Static); !alsoStatic {
			lhs, rhs = rhs, lhs
			op = flip(op)
		}
	}

	if attr, ok := lhs.(traceql.Attribute); ok {
		if st, ok := rhs.(traceql.Static); ok {
			if special, handled, ok := t.specialIntrinsic(attr, op, st); handled {
				if !ok {
					return t.relax(f, b.String())
				}
				return special, true
			}
		}
	}
	if t.missingAttribute(lhs) || t.missingAttribute(rhs) {
		return "false", true
	}

	if st, ok := rhs.(traceql.Static); ok {
		lv, lt, ok := t.value(lhs, f)
		if !ok {
			return "", false
		}
		return t.compareStatic(lv, lt, op, st, f)
	}

	lv, lt, lok := t.value(lhs, f)
	rv, rt, rok := t.value(rhs, f)
	if !lok || !rok {
		return "", false
	}
	if lt == schema.TypeUnknown && rt != schema.TypeUnknown {
		lv = cast(schema.Column{Expr: lv}, rt)
	} else if rt == schema.TypeUnknown && lt != schema.TypeUnknown {
		rv = cast(schema.Column{Expr: rv}, lt)
	}
	switch op {
	case traceql.OpRegex, traceql.OpNotRegex:
		return t.relax(f, b.String())
	}
	return lv + " " + cmpOp(op) + " " + rv, true
}

// compareStatic renders <expr> <op> <literal>.
func (t *Translator) compareStatic(expr string, et schema.Type, op traceql.Operator, st traceql.Static, f *Filter) (string, bool) {
	switch st.Type {
	case traceql.TypeString:
		s := st.EncodeToString(false)
		switch op {
		case traceql.OpRegex:
			return castTo(expr, et, schema.TypeString) + " matches regex " + apl.RawString(anchor(s)), true
		case traceql.OpNotRegex:
			return apl.Not(castTo(expr, et, schema.TypeString) + " matches regex " + apl.RawString(anchor(s))), true
		case traceql.OpEqual, traceql.OpNotEqual:
			return castTo(expr, et, schema.TypeString) + " " + cmpOp(op) + " " + apl.String(s), true
		default:
			return castTo(expr, et, schema.TypeString) + " " + cmpOp(op) + " " + apl.String(s), true
		}
	case traceql.TypeInt:
		i, _ := st.Int()
		if et == schema.TypeDuration {
			// TraceQL compares durations with ints as nanoseconds.
			return expr + " " + cmpOp(op) + " totimespan(" + apl.Int(int64(i)) + ")", true
		}
		return castTo(expr, et, schema.TypeInt) + " " + cmpOp(op) + " " + apl.Int(int64(i)), true
	case traceql.TypeFloat:
		if et == schema.TypeDuration {
			return expr + " " + cmpOp(op) + " totimespan(" + apl.Float(st.Float()) + ")", true
		}
		return castTo(expr, et, schema.TypeFloat) + " " + cmpOp(op) + " " + apl.Float(st.Float()), true
	case traceql.TypeBoolean:
		b, _ := st.Bool()
		return castTo(expr, et, schema.TypeBool) + " " + cmpOp(op) + " " + apl.Bool(b), true
	case traceql.TypeDuration:
		d, _ := st.Duration()
		return castTo(expr, et, schema.TypeDuration) + " " + cmpOp(op) + " " + apl.Timespan(d), true
	case traceql.TypeStatus:
		s, _ := st.Status()
		return t.statusPredicate(expr, op, s, f)
	case traceql.TypeKind:
		k, _ := st.Kind()
		return t.kindPredicate(expr, op, k, f)
	case traceql.TypeNil:
		switch op {
		case traceql.OpEqual:
			return apl.Call("isnull", expr), true
		case traceql.OpNotEqual:
			return apl.Call("isnotnull", expr), true
		}
	}
	return t.relax(f, st.String())
}

// value renders a non-boolean expression (attribute, literal, arithmetic).
func (t *Translator) value(e traceql.FieldExpression, f *Filter) (string, schema.Type, bool) {
	switch v := e.(type) {
	case traceql.Attribute:
		col, ok := t.m.Resolve(v)
		if !ok {
			t.relax(f, v.String())
			return "", schema.TypeUnknown, false
		}
		return col.Expr, col.Type, true
	case traceql.Static:
		s, typ, ok := literal(v)
		if !ok {
			t.relax(f, v.String())
			return "", schema.TypeUnknown, false
		}
		return s, typ, true
	case *traceql.BinaryOperation:
		return t.arith(v, f)
	case *traceql.UnaryOperation:
		if v.Op == traceql.OpSub {
			s, typ, ok := t.value(v.Expression, f)
			if !ok {
				return "", schema.TypeUnknown, false
			}
			return "-(" + s + ")", typ, true
		}
	case traceql.UnaryOperation:
		if v.Op == traceql.OpSub {
			s, typ, ok := t.value(v.Expression, f)
			if !ok {
				return "", schema.TypeUnknown, false
			}
			return "-(" + s + ")", typ, true
		}
	}
	t.relax(f, fmt.Sprintf("%v", e))
	return "", schema.TypeUnknown, false
}

func (t *Translator) arith(b *traceql.BinaryOperation, f *Filter) (string, schema.Type, bool) {
	var op string
	switch b.Op {
	case traceql.OpAdd:
		op = "+"
	case traceql.OpSub:
		op = "-"
	case traceql.OpMult:
		op = "*"
	case traceql.OpDiv:
		op = "/"
	case traceql.OpMod:
		op = "%"
	case traceql.OpPower:
		l, lt, lok := t.value(b.LHS, f)
		r, _, rok := t.value(b.RHS, f)
		if !lok || !rok {
			return "", schema.TypeUnknown, false
		}
		return apl.Call("pow", l, r), promote(lt, schema.TypeFloat), true
	default:
		t.relax(f, b.String())
		return "", schema.TypeUnknown, false
	}
	l, lt, lok := t.value(b.LHS, f)
	r, rt, rok := t.value(b.RHS, f)
	if !lok || !rok {
		return "", schema.TypeUnknown, false
	}
	typ := promote(lt, rt)
	if lt == schema.TypeUnknown {
		l = cast(schema.Column{Expr: l}, numericOr(rt))
	}
	if rt == schema.TypeUnknown {
		r = cast(schema.Column{Expr: r}, numericOr(lt))
	}
	return "(" + l + " " + op + " " + r + ")", typ, true
}

// specialIntrinsic handles intrinsics that do not map to a plain column.
// handled=false means the attribute is ordinary.
func (t *Translator) specialIntrinsic(a traceql.Attribute, op traceql.Operator, st traceql.Static) (expr string, handled, ok bool) {
	switch a.Intrinsic {
	case traceql.IntrinsicNestedSetParent:
		// Root spans have nestedSetParent < 0 (Tempo assigns -1).
		i, isInt := st.Int()
		if !isInt {
			return "", true, false
		}
		root := t.m.IsRoot()
		switch {
		case op == traceql.OpLess && i <= 0, op == traceql.OpLessEqual && i < 0,
			op == traceql.OpEqual && i < 0:
			return root, true, true
		case op == traceql.OpGreaterEqual && i <= 0, op == traceql.OpGreater && i < 0,
			op == traceql.OpNotEqual && i < 0:
			return apl.Not(root), true, true
		}
		return "", true, false
	case traceql.IntrinsicEventName:
		expr, ok := t.eventPredicate("", op, st)
		return expr, true, ok
	case traceql.IntrinsicNestedSetLeft, traceql.IntrinsicNestedSetRight, traceql.IntrinsicChildCount,
		traceql.IntrinsicTraceRootService, traceql.IntrinsicTraceRootSpan, traceql.IntrinsicTraceDuration,
		traceql.ScopedIntrinsicTraceRootName, traceql.ScopedIntrinsicTraceRootService, traceql.ScopedIntrinsicTraceDuration,
		traceql.IntrinsicEventTimeSinceStart,
		traceql.IntrinsicLinkSpanID, traceql.IntrinsicLinkTraceID, traceql.IntrinsicParent:
		return "", true, false
	}
	if a.Scope == traceql.AttributeScopeEvent && !a.Parent {
		expr, ok := t.eventPredicate(a.Name, op, st)
		return expr, true, ok
	}
	if a.Parent || a.Scope == traceql.AttributeScopeLink {
		return "", true, false
	}
	return "", false, false
}

// eventPredicate renders a comparison against an event attribute. Events
// are an array of {name, attributes} objects, and APL cannot express
// "any element matches" per row, so the comparison is repeated over the
// first MaxEventsPerSpan slots and or-ed: a span matches when any event
// does, which is TraceQL's semantics for event attributes. Negated
// comparisons match when no event does.
func (t *Translator) eventPredicate(name string, op traceql.Operator, st traceql.Static) (string, bool) {
	slots, ok := t.m.EventSlots(name)
	if !ok {
		return "", false
	}
	var parts []string
	f := &Filter{}
	switch {
	case st.Type == traceql.TypeNil && op == traceql.OpExists, st.Type == traceql.TypeNil && op == traceql.OpEqual:
		for _, s := range slots {
			parts = append(parts, apl.Call("isnotnull", s))
		}
		if op == traceql.OpEqual {
			return apl.Not(apl.Or(parts...)), true
		}
		return apl.Or(parts...), true
	case st.Type == traceql.TypeNil && op == traceql.OpNotExists, st.Type == traceql.TypeNil && op == traceql.OpNotEqual:
		for _, s := range slots {
			parts = append(parts, apl.Call("isnotnull", s))
		}
		if op == traceql.OpNotEqual {
			return apl.Or(parts...), true
		}
		return apl.Not(apl.Or(parts...)), true
	}
	positive := op
	negated := false
	switch op {
	case traceql.OpNotEqual:
		positive, negated = traceql.OpEqual, true
	case traceql.OpNotRegex:
		positive, negated = traceql.OpRegex, true
	}
	for _, s := range slots {
		p, ok := t.compareStatic(s, schema.TypeUnknown, positive, st, f)
		if !ok {
			return "", false
		}
		parts = append(parts, p)
	}
	if negated {
		return apl.Not(apl.Or(parts...)), true
	}
	return apl.Or(parts...), true
}

func (t *Translator) statusPredicate(expr string, op traceql.Operator, s traceql.Status, f *Filter) (string, bool) {
	// Datasets may carry the status as a code column, an error flag, or
	// both; use whichever exists.
	codeCol := t.m.StatusCode()
	errCol := t.m.Error()
	hasCode, hasErr := !codeCol.Missing, !errCol.Missing
	if !hasCode && !hasErr {
		return t.relax(f, "status "+s.String())
	}
	var eq string
	switch s {
	case traceql.StatusError:
		var parts []string
		if hasCode {
			parts = append(parts, expr+` =~ "error"`, expr+` =~ "STATUS_CODE_ERROR"`)
		}
		if hasErr {
			parts = append(parts, errCol.Expr+" == true")
		}
		eq = apl.Or(parts...)
	case traceql.StatusOk:
		if !hasCode {
			return t.relax(f, "status ok")
		}
		eq = apl.Or(expr+` =~ "ok"`, expr+` =~ "STATUS_CODE_OK"`)
	case traceql.StatusUnset:
		if !hasCode {
			return t.relax(f, "status unset")
		}
		eq = apl.Or(apl.Call("isempty", expr), expr+` =~ "unset"`, expr+` =~ "STATUS_CODE_UNSET"`)
	default:
		return t.relax(f, "status "+s.String())
	}
	switch op {
	case traceql.OpEqual:
		return eq, true
	case traceql.OpNotEqual:
		return apl.Not(eq), true
	}
	// Ordering comparisons on status are meaningless here.
	return t.relax(f, "status "+cmpOp(op)+" "+s.String())
}

func (t *Translator) kindPredicate(expr string, op traceql.Operator, k traceql.Kind, f *Filter) (string, bool) {
	name := k.String()
	eq := apl.Or(expr+" =~ "+apl.String(name), expr+" =~ "+apl.String("SPAN_KIND_"+strings.ToUpper(name)))
	switch op {
	case traceql.OpEqual:
		return eq, true
	case traceql.OpNotEqual:
		return apl.Not(eq), true
	}
	return t.relax(f, "kind "+cmpOp(op)+" "+name)
}

// literal renders a static as an APL literal.
func literal(st traceql.Static) (string, schema.Type, bool) {
	switch st.Type {
	case traceql.TypeString:
		return apl.String(st.EncodeToString(false)), schema.TypeString, true
	case traceql.TypeInt:
		i, _ := st.Int()
		return apl.Int(int64(i)), schema.TypeInt, true
	case traceql.TypeFloat:
		return apl.Float(st.Float()), schema.TypeFloat, true
	case traceql.TypeBoolean:
		b, _ := st.Bool()
		return apl.Bool(b), schema.TypeBool, true
	case traceql.TypeDuration:
		d, _ := st.Duration()
		return apl.Timespan(d), schema.TypeDuration, true
	case traceql.TypeStatus:
		s, _ := st.Status()
		return apl.String(s.String()), schema.TypeString, true
	case traceql.TypeKind:
		k, _ := st.Kind()
		return apl.String(k.String()), schema.TypeString, true
	}
	return "", schema.TypeUnknown, false
}

// cast wraps a column so it has the wanted type when its type is unknown
// or different.
func cast(c schema.Column, want schema.Type) string {
	return castTo(c.Expr, c.Type, want)
}

func castTo(expr string, have, want schema.Type) string {
	if have == want || want == schema.TypeUnknown {
		return expr
	}
	switch want {
	case schema.TypeString:
		return apl.Call("tostring", expr)
	case schema.TypeInt:
		if have == schema.TypeFloat {
			return expr
		}
		return apl.Call("tolong", expr)
	case schema.TypeFloat:
		if have == schema.TypeInt {
			return expr
		}
		return apl.Call("toreal", expr)
	case schema.TypeBool:
		return apl.Call("tobool", expr)
	case schema.TypeDuration:
		return apl.Call("totimespan", expr)
	}
	return expr
}

func numericOr(t schema.Type) schema.Type {
	switch t {
	case schema.TypeInt, schema.TypeFloat, schema.TypeDuration:
		return t
	}
	return schema.TypeFloat
}

func promote(a, b schema.Type) schema.Type {
	if a == schema.TypeFloat || b == schema.TypeFloat {
		return schema.TypeFloat
	}
	if a == schema.TypeDuration || b == schema.TypeDuration {
		return schema.TypeDuration
	}
	if a == schema.TypeInt || b == schema.TypeInt {
		return schema.TypeInt
	}
	return schema.TypeUnknown
}

func cmpOp(op traceql.Operator) string {
	switch op {
	case traceql.OpEqual:
		return "=="
	case traceql.OpNotEqual:
		return "!="
	case traceql.OpGreater:
		return ">"
	case traceql.OpGreaterEqual:
		return ">="
	case traceql.OpLess:
		return "<"
	case traceql.OpLessEqual:
		return "<="
	}
	return "=="
}

func flip(op traceql.Operator) traceql.Operator {
	switch op {
	case traceql.OpGreater:
		return traceql.OpLess
	case traceql.OpGreaterEqual:
		return traceql.OpLessEqual
	case traceql.OpLess:
		return traceql.OpGreater
	case traceql.OpLessEqual:
		return traceql.OpGreaterEqual
	}
	return op
}

// anchor makes a regex fully anchored, as TraceQL regexes are.
func anchor(re string) string {
	if strings.HasPrefix(re, "^") && strings.HasSuffix(re, "$") {
		return re
	}
	return "^(?:" + re + ")$"
}

// SpansetFilters returns the spanset filter expressions of a parsed
// pipeline. It only understands pipelines made of a single spanset
// filter optionally followed by select/by/coalesce, which is what can be
// pushed down exactly; anything else returns ok=false.
func SpansetFilters(p traceql.Pipeline) (filters []traceql.FieldExpression, exact bool) {
	exact = true
	for _, el := range p.Elements {
		switch v := el.(type) {
		case *traceql.SpansetFilter:
			filters = append(filters, v.Expression)
		case traceql.SelectOperation, *traceql.SelectOperation:
			// select() does not change which spans match.
		default:
			exact = false
		}
	}
	return filters, exact
}

// ParseFilter parses a standalone TraceQL spanset filter such as
// `{ span.foo = "bar" }` and returns its expression.
func ParseFilter(q string) (traceql.FieldExpression, error) {
	q = strings.TrimSpace(q)
	if q == "" || q == "{}" || q == "{ }" {
		return traceql.NewStaticBool(true), nil
	}
	root, err := traceql.Parse(q)
	if err != nil {
		return nil, err
	}
	if IsMetricsQuery(q) {
		return nil, fmt.Errorf("%w: metrics query given where a spanset filter was expected", ErrUnsupported)
	}
	p, ok := root.SinglePipeline()
	if !ok {
		return nil, fmt.Errorf("%w: expected a single spanset filter", ErrUnsupported)
	}
	filters, exact := SpansetFilters(p)
	if !exact || len(filters) == 0 {
		return nil, fmt.Errorf("%w: expected a single spanset filter", ErrUnsupported)
	}
	if len(filters) == 1 {
		return filters[0], nil
	}
	// Chained filters are an intersection.
	var expr traceql.FieldExpression = filters[0]
	for _, f := range filters[1:] {
		expr = &traceql.BinaryOperation{Op: traceql.OpAnd, LHS: expr, RHS: f}
	}
	return expr, nil
}

// TraceIDFilter renders a where clause matching one trace by hex id.
func TraceIDFilter(m *schema.Mapping, hexID string) string {
	return m.TraceID().Expr + " == " + apl.String(strings.ToLower(hexID))
}

// TraceIDsFilter renders a where clause matching any of the given trace
// ids.
func TraceIDsFilter(m *schema.Mapping, ids []string) string {
	if len(ids) == 0 {
		return "false"
	}
	quoted := make([]string, len(ids))
	for i, id := range ids {
		quoted[i] = apl.String(id)
	}
	return m.TraceID().Expr + " in (" + strings.Join(quoted, ", ") + ")"
}

// ParseDurationLiteral parses a TraceQL duration literal like 100ms.
func ParseDurationLiteral(s string) (int64, error) {
	st, err := traceql.Parse("{ duration > " + s + " }")
	if err != nil {
		return 0, err
	}
	p, _ := st.SinglePipeline()
	filters, _ := SpansetFilters(p)
	if len(filters) != 1 {
		return 0, fmt.Errorf("bad duration %q", s)
	}
	b, ok := filters[0].(*traceql.BinaryOperation)
	if !ok {
		return 0, fmt.Errorf("bad duration %q", s)
	}
	static, ok := b.RHS.(traceql.Static)
	if !ok {
		return 0, fmt.Errorf("bad duration %q", s)
	}
	d, ok := static.Duration()
	if !ok {
		i, ok := static.Int()
		if !ok {
			return 0, fmt.Errorf("bad duration %q", s)
		}
		return int64(i), nil
	}
	return int64(d), nil
}

var _ = strconv.Itoa
