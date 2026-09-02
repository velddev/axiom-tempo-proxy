package translate

import (
	"time"

	"github.com/grafana/tempo/pkg/traceql"

	"github.com/velddev/axiom-tempo-proxy/internal/apl"
	"github.com/velddev/axiom-tempo-proxy/internal/schema"
)

// Column aliases the per-trace query computes. They are short and
// underscore-prefixed so they cannot collide with dataset fields.
const (
	// ColTraceDuration holds max(end) - min(start) over a trace's spans.
	ColTraceDuration = "_td"
	// ColRootName holds the name of the trace's root span.
	ColRootName = "_rn"
	// ColRootService holds the service name of the trace's root span.
	ColRootService = "_rs"
	// ColRootKey is the arg_min sort key that prefers parentless spans.
	ColRootKey = "_rk"
)

// rootEpoch sorts parentless spans ahead of every real span time, so
// arg_min picks the root span when the trace has one and the earliest
// span otherwise. It matches spans.Trace.finish().
var rootEpoch = time.Unix(0, 0).UTC()

// TraceIntrinsics records which trace-level intrinsics an expression uses.
type TraceIntrinsics struct {
	Duration    bool
	RootName    bool
	RootService bool
}

// Any reports whether any trace-level intrinsic is used.
func (u TraceIntrinsics) Any() bool { return u.Duration || u.RootName || u.RootService }

// RootOnly reports whether only root-span intrinsics are used, which can
// be narrowed down to a per-span predicate on root spans.
func (u TraceIntrinsics) RootOnly() bool { return u.Any() && !u.Duration }

func (u *TraceIntrinsics) add(i traceql.Intrinsic) bool {
	switch i {
	case traceql.IntrinsicTraceDuration, traceql.ScopedIntrinsicTraceDuration:
		u.Duration = true
	case traceql.IntrinsicTraceRootSpan, traceql.ScopedIntrinsicTraceRootName:
		u.RootName = true
	case traceql.IntrinsicTraceRootService, traceql.ScopedIntrinsicTraceRootService:
		u.RootService = true
	default:
		return false
	}
	return true
}

// TraceSplit is a spanset filter separated into the part that must be
// evaluated per trace and the part that can be evaluated per span.
type TraceSplit struct {
	// Trace is the conjunction of trace-level conjuncts, nil when none.
	Trace traceql.FieldExpression
	// Span is the conjunction of span-level conjuncts, nil when none.
	Span traceql.FieldExpression
	// Uses lists the trace-level intrinsics referenced by Trace.
	Uses TraceIntrinsics
	// Mixed is true when a single conjunct combines trace-level and
	// span-level terms (`{ traceDuration > 1s || status = error }`).
	// Such a filter cannot be split and is not supported.
	Mixed bool
}

// SplitTrace separates the trace-level conjuncts of a spanset filter from
// the span-level ones. Trace-level intrinsics are constant across a
// trace, so a top-level conjunction can be evaluated in two steps: pick
// the traces whose aggregate values match, then filter their spans.
func SplitTrace(e traceql.FieldExpression) TraceSplit {
	var s TraceSplit
	var trace, span []traceql.FieldExpression
	for _, c := range conjuncts(e) {
		uses, hasSpan := classify(c)
		switch {
		case uses.Any() && hasSpan:
			s.Mixed = true
			return s
		case uses.Any():
			s.Uses.Duration = s.Uses.Duration || uses.Duration
			s.Uses.RootName = s.Uses.RootName || uses.RootName
			s.Uses.RootService = s.Uses.RootService || uses.RootService
			trace = append(trace, c)
		default:
			span = append(span, c)
		}
	}
	s.Trace = conjoin(trace)
	s.Span = conjoin(span)
	return s
}

// conjuncts flattens a top-level && chain.
func conjuncts(e traceql.FieldExpression) []traceql.FieldExpression {
	if b, ok := e.(*traceql.BinaryOperation); ok && b.Op == traceql.OpAnd {
		return flatten(b, traceql.OpAnd)
	}
	if e == nil {
		return nil
	}
	return []traceql.FieldExpression{e}
}

func conjoin(parts []traceql.FieldExpression) traceql.FieldExpression {
	if len(parts) == 0 {
		return nil
	}
	out := parts[0]
	for _, p := range parts[1:] {
		out = &traceql.BinaryOperation{Op: traceql.OpAnd, LHS: out, RHS: p}
	}
	return out
}

// classify reports the trace-level intrinsics an expression uses and
// whether it also references anything that is per-span.
func classify(e traceql.FieldExpression) (uses TraceIntrinsics, hasSpan bool) {
	walkAttributes(e, func(a traceql.Attribute) {
		if !a.Parent && uses.add(a.Intrinsic) {
			return
		}
		hasSpan = true
	})
	return uses, hasSpan
}

// walkAttributes visits every attribute of a field expression.
func walkAttributes(e traceql.FieldExpression, fn func(traceql.Attribute)) {
	switch v := e.(type) {
	case traceql.Attribute:
		fn(v)
	case *traceql.Attribute:
		fn(*v)
	case *traceql.BinaryOperation:
		walkAttributes(v.LHS, fn)
		walkAttributes(v.RHS, fn)
	case *traceql.UnaryOperation:
		walkAttributes(v.Expression, fn)
	case traceql.UnaryOperation:
		walkAttributes(v.Expression, fn)
	}
}

// traceColumns tells the translator where the trace-level intrinsics live
// while rendering a trace-level predicate.
type traceColumns struct {
	duration    schema.Column
	rootName    schema.Column
	rootService schema.Column
}

func (t *Translator) with(tc *traceColumns) *Translator {
	c := *t
	c.trace = tc
	return &c
}

// traceCol resolves a trace-level intrinsic in trace mode.
func (t *Translator) traceCol(i traceql.Intrinsic) (schema.Column, bool) {
	if t.trace == nil {
		return schema.Column{}, false
	}
	var c schema.Column
	switch i {
	case traceql.IntrinsicTraceDuration, traceql.ScopedIntrinsicTraceDuration:
		c = t.trace.duration
	case traceql.IntrinsicTraceRootSpan, traceql.ScopedIntrinsicTraceRootName:
		c = t.trace.rootName
	case traceql.IntrinsicTraceRootService, traceql.ScopedIntrinsicTraceRootService:
		c = t.trace.rootService
	default:
		return schema.Column{}, false
	}
	if c.Expr == "" || c.Missing {
		return schema.Column{}, false
	}
	return c, true
}

// traceIntrinsicPredicate renders a comparison against a trace-level
// value that has already been reduced to a column.
func (t *Translator) traceIntrinsicPredicate(col schema.Column, op traceql.Operator, st traceql.Static) (string, bool) {
	switch op {
	case traceql.OpExists:
		return apl.Call("isnotnull", col.Expr), true
	case traceql.OpNotExists:
		return apl.Call("isnull", col.Expr), true
	}
	var f Filter
	return t.compareStatic(col.Expr, col.Type, op, st, &f)
}

// TraceFilter renders a trace-level predicate against the aggregate
// columns produced by TraceAggregates. Exact is false when part of the
// expression could not be translated, in which case the caller must
// refuse the query rather than return wrong numbers.
func (t *Translator) TraceFilter(e traceql.FieldExpression) Filter {
	return t.with(&traceColumns{
		duration:    schema.Column{Expr: ColTraceDuration, Type: schema.TypeDuration},
		rootName:    schema.Column{Expr: ColRootName, Type: schema.TypeString},
		rootService: schema.Column{Expr: ColRootService, Type: schema.TypeString},
	}).Filter(e)
}

// RootSpanFilter renders a root-only trace predicate as a per-span
// predicate over root spans. It is used to narrow the set of traces the
// aggregate query has to look at, and only ever selects traces that
// actually carry a parentless span.
func (t *Translator) RootSpanFilter(e traceql.FieldExpression) (string, bool) {
	m := t.m
	f := t.with(&traceColumns{rootName: m.Name(), rootService: m.ServiceName()}).Filter(e)
	if !f.Exact || f.Where == "" {
		return "", false
	}
	if m.ParentSpanID().Missing {
		return "", false
	}
	return apl.And(m.IsRoot(), f.Where), true
}

// TraceAggregates renders the stages that compute per-trace values:
// extend assignments feeding the aggregation, and summarize aggregations
// producing ColTraceDuration / ColRootName / ColRootService. It fails
// when the dataset lacks a column the values are derived from, since
// referencing an unknown field is a hard error in APL.
func (t *Translator) TraceAggregates(u TraceIntrinsics) (extend, aggs []string, ok bool) {
	m := t.m
	if u.Duration {
		if m.Duration().Missing || m.Time().Missing {
			return nil, nil, false
		}
		span := "(" + m.Time().Expr + " + " + m.Duration().Expr + ")"
		aggs = append(aggs, ColTraceDuration+" = "+
			apl.Call("todatetime", apl.Call("max", span))+" - "+
			apl.Call("todatetime", apl.Call("min", m.Time().Expr)))
	}
	if u.RootName || u.RootService {
		// arg_min names its outputs after the expressions it is given, so
		// the values are aliased first and picked out by that alias.
		key := m.Time().Expr
		if !m.ParentSpanID().Missing {
			key = apl.Call("iff", m.IsRoot(), "datetime("+rootEpoch.Format(time.RFC3339)+")", m.Time().Expr)
		}
		extend = append(extend, ColRootKey+" = "+key)
		var picks []string
		if u.RootName {
			if m.Name().Missing {
				return nil, nil, false
			}
			extend = append(extend, ColRootName+" = "+m.Name().Expr)
			picks = append(picks, ColRootName)
		}
		if u.RootService {
			if m.ServiceName().Missing {
				return nil, nil, false
			}
			extend = append(extend, ColRootService+" = "+m.ServiceName().Expr)
			picks = append(picks, ColRootService)
		}
		aggs = append(aggs, "_r = "+apl.Call("arg_min", append([]string{ColRootKey}, picks...)...))
	}
	return extend, aggs, len(aggs) > 0
}
