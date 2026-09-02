package metrics

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/grafana/tempo/pkg/traceql"

	"github.com/velddev/axiom-tempo-proxy/internal/apl"
	"github.com/velddev/axiom-tempo-proxy/internal/axiom"
	"github.com/velddev/axiom-tempo-proxy/internal/translate"
)

// Axiom truncates a `summarize ... by <high cardinality>` silently once
// the group count grows past a few thousand and flags the result as an
// estimate. Per-trace aggregation is exactly that shape, so every
// trace-level query checks for the flag and refuses rather than
// returning numbers computed from a fraction of the traces.
const truncationWarning = "max_limit_warning"

// truncated reports whether Axiom capped the groups of a summarize. The
// queries built here never carry an explicit `limit`, which raises the
// same flags, so this only fires on real truncation.
func truncated(res *axiom.Result) bool {
	if res == nil {
		return false
	}
	if res.Status.IsEstimate {
		return true
	}
	for _, m := range res.Status.Messages {
		if m.Code == truncationWarning {
			return true
		}
	}
	return false
}

// Aliases the per-trace queries return: the exact number of qualifying
// traces and a bounded list of their ids. Reading both means an
// overflowing set is detected instead of being silently clipped.
const (
	countAlias = "_n"
	idsAlias   = "_ids"
)

// traceFilter resolves the trace-level part of a spanset filter into an
// APL predicate restricting spans to the qualifying traces.
//
// Trace-level intrinsics (traceDuration, rootName, rootServiceName) are
// properties of a whole trace, so they cannot be tested on a single row.
// They are resolved in a separate pass: the qualifying trace ids are
// computed with a per-trace aggregation and then inlined into the metrics
// query as `trace_id in (...)`. APL's `join` was measured to truncate its
// left side at 50,000 rows without any warning, and `in (<subquery>)` is
// rejected outright, so neither can carry this safely.
func (e *Evaluator) traceFilter(ctx context.Context, split translate.TraceSplit, spanWhere string, start, end time.Time) (string, error) {
	m := e.tr.Mapping()
	pred := e.tr.TraceFilter(split.Trace)
	if !pred.Exact || pred.Where == "" {
		return "", &UnsupportedError{Reason: "trace-level filter cannot be evaluated in APL: " + split.Trace.String()}
	}
	extend, aggs, ok := e.tr.TraceAggregates(split.Uses)
	if !ok {
		return "", &UnsupportedError{Reason: "the dataset has no columns to derive trace-level values from"}
	}

	// Narrow the traces the aggregation has to look at where that is
	// possible without changing the result: only traces holding a span
	// that matches the span-level filter can contribute to the metric.
	// Failing that, a filter that only looks at the root span can be
	// pushed onto root spans directly.
	var candidates []string
	var narrowed bool
	switch {
	case spanWhere != "":
		ids, err := e.traceIDsWhere(ctx, spanWhere, start, end, "the span-level filter")
		if err != nil {
			return "", err
		}
		candidates, narrowed = ids, true
	case split.Uses.RootOnly():
		if rootWhere, ok := e.tr.RootSpanFilter(split.Trace); ok {
			ids, err := e.traceIDsWhere(ctx, rootWhere, start, end, "the root-span filter")
			if err != nil {
				return "", err
			}
			candidates, narrowed = ids, true
		}
	}
	if narrowed && len(candidates) == 0 {
		return "false", nil
	}

	q := apl.NewQuery(e.opts.Dataset).Where(m.SpansOnly())
	if narrowed {
		q.Where(translate.TraceIDsFilter(m, candidates))
	}
	q.Extend(extend...).
		Summarize(aggs, []string{m.TraceID().Expr}).
		Where(pred.Where).
		Summarize([]string{
			countAlias + " = count()",
			fmt.Sprintf("%s = make_list(%s, %d)", idsAlias, m.TraceID().Expr, e.maxTraceIDs()),
		}, nil)

	ids, err := e.readTraceIDs(ctx, q.String(), start, end, "the trace-level filter")
	if err != nil {
		return "", err
	}
	return translate.TraceIDsFilter(m, ids), nil
}

// traceIDsWhere lists the ids of the traces holding a span that matches a
// per-span predicate.
func (e *Evaluator) traceIDsWhere(ctx context.Context, where string, start, end time.Time, what string) ([]string, error) {
	m := e.tr.Mapping()
	q := apl.NewQuery(e.opts.Dataset).
		Where(m.SpansOnly()).
		Where(where).
		Summarize(nil, []string{m.TraceID().Expr}).
		Summarize([]string{
			countAlias + " = count()",
			fmt.Sprintf("%s = make_list(%s, %d)", idsAlias, m.TraceID().Expr, e.maxTraceIDs()),
		}, nil)
	return e.readTraceIDs(ctx, q.String(), start, end, what)
}

// readTraceIDs runs a query shaped as `summarize _n = count(), _ids =
// make_list(trace_id, cap)` and returns the ids, refusing the query when
// the aggregation was truncated or the set is larger than the cap.
func (e *Evaluator) readTraceIDs(ctx context.Context, query string, start, end time.Time, what string) ([]string, error) {
	res, err := e.run(ctx, query, start, end)
	if err != nil {
		return nil, err
	}
	if truncated(res) {
		return nil, &UnsupportedError{Reason: fmt.Sprintf(
			"too many traces in the query window to evaluate %s exactly; shorten the time range or add a more selective span filter", what)}
	}
	t := res.FirstTable()
	if t == nil || t.NumRows() == 0 {
		return nil, nil
	}
	row := t.Row(0)
	n := row.Int64(countAlias)
	if max := int64(e.maxTraceIDs()); n > max {
		return nil, &UnsupportedError{Reason: fmt.Sprintf(
			"%s matches %d traces, more than the limit of %d; shorten the time range or narrow the query", what, n, max)}
	}
	raw := row.Raw(idsAlias)
	if raw == nil {
		return nil, nil
	}
	var ids []string
	if err := json.Unmarshal(raw, &ids); err != nil {
		return nil, fmt.Errorf("axiom returned an unreadable trace id list: %w", err)
	}
	out := ids[:0]
	for _, id := range ids {
		if id != "" {
			out = append(out, id)
		}
	}
	return out, nil
}

func (e *Evaluator) maxTraceIDs() int {
	if e.opts.MaxTraceIntrinsicTraces > 0 {
		return e.opts.MaxTraceIntrinsicTraces
	}
	return defaultMaxTraceIntrinsicTraces
}

// unsupportedMixed builds the error for a filter that mixes trace-level
// and span-level terms in a way that cannot be split.
func unsupportedMixed(e traceql.FieldExpression) error {
	return &UnsupportedError{Reason: "trace-level intrinsics can only be combined with span-level conditions using &&: " + e.String()}
}
