// Package fetch implements traceql.SpansetFetcher on top of Axiom so the
// Tempo engine can evaluate TraceQL search queries with exact semantics
// while Axiom does the heavy filtering.
//
// A search runs two APL queries. The first finds candidate trace ids:
// spans are filtered by the union of the query's spanset filters, counted
// per trace and per filter, and traces are kept when the counts satisfy
// the query's spanset structure (both sides present for && and structural
// operators, either side for ||). The second pulls every span of those
// traces. Spans are grouped into spansets and handed to the engine's
// second pass, which applies the full TraceQL semantics.
package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/util"

	"github.com/velddev/axiom-tempo-proxy/internal/apl"
	"github.com/velddev/axiom-tempo-proxy/internal/axiom"
	"github.com/velddev/axiom-tempo-proxy/internal/schema"
	"github.com/velddev/axiom-tempo-proxy/internal/spans"
	"github.com/velddev/axiom-tempo-proxy/internal/translate"
)

// Options configure a Fetcher.
type Options struct {
	Dataset string
	// MaxTraces caps candidate traces pulled for one search.
	MaxTraces int
	// MaxSpans caps spans pulled in the second query.
	MaxSpans int
	// DefaultLookback is used when the request has no time range.
	DefaultLookback time.Duration
	// TracePadding widens the span pull window so spans of a candidate
	// trace that fall outside the search window are still returned.
	TracePadding time.Duration
	// NoPreferSelected disables ranking candidate traces that carry the
	// attributes the query select()s ahead of those that do not.
	NoPreferSelected bool
	Log              *slog.Logger
	LogQueries       bool
}

func (o Options) withDefaults() Options {
	if o.MaxTraces <= 0 {
		o.MaxTraces = 200
	}
	if o.MaxSpans <= 0 {
		o.MaxSpans = 50000
	}
	if o.DefaultLookback <= 0 {
		o.DefaultLookback = time.Hour
	}
	if o.TracePadding <= 0 {
		o.TracePadding = time.Hour
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	return o
}

// Stats are counters reported back to the engine.
type Stats struct {
	CandidateTraces int
	SpansFetched    int
	Queries         int
}

// Fetcher runs the two-phase fetch for one query.
type Fetcher struct {
	client *axiom.Client
	tr     *translate.Translator
	parser *spans.Parser
	opts   Options
	plan   *plan
	stats  Stats
}

// NewSearchFetcher prepares a fetcher for a TraceQL search query. The
// query must already parse; the fetcher derives its APL prefilter from it.
func NewSearchFetcher(client *axiom.Client, tr *translate.Translator, query string, opts Options) (*Fetcher, error) {
	p, err := buildPlan(tr, query)
	if err != nil {
		return nil, err
	}
	return &Fetcher{
		client: client,
		tr:     tr,
		parser: spans.NewParser(tr.Mapping()),
		opts:   opts.withDefaults(),
		plan:   p,
	}, nil
}

// Stats returns fetch counters.
func (f *Fetcher) Stats() Stats { return f.stats }

// Plan returns a description of the prefilter for logging.
func (f *Fetcher) Plan() string { return f.plan.String() }

// Exact reports whether the prefilter selects exactly the traces the
// query matches (no relaxed sub-expressions).
func (f *Fetcher) Exact() bool { return f.plan.exact && f.plan.traceWhere != "" }

// SetMaxTraces overrides the candidate trace cap.
func (f *Fetcher) SetMaxTraces(n int) {
	if n > 0 {
		f.opts.MaxTraces = n
	}
}

// Fetch implements traceql.SpansetFetcher.
func (f *Fetcher) Fetch(ctx context.Context, req traceql.FetchSpansRequest) (traceql.FetchSpansResponse, error) {
	start, end := f.window(req.StartTimeUnixNanos, req.EndTimeUnixNanos)

	ids, err := f.candidates(ctx, start, end)
	if err != nil {
		return traceql.FetchSpansResponse{}, err
	}
	f.stats.CandidateTraces = len(ids)
	if len(ids) == 0 {
		return traceql.FetchSpansResponse{Results: &iterator{}, Stats: f.fetchStats}, nil
	}

	traces, err := f.pullTraces(ctx, ids, start, end)
	if err != nil {
		return traceql.FetchSpansResponse{}, err
	}
	// Only report the attributes the query asked for, as Tempo does.
	exposed := spans.NewAttrSet(req.Conditions, req.SecondPassConditions)
	if req.SecondPassSelectAll {
		exposed = nil
	}
	for _, t := range traces {
		for _, s := range t.Spans {
			s.SetExposed(exposed)
		}
	}
	return traceql.FetchSpansResponse{Results: &iterator{traces: traces, secondPass: req.SecondPass}, Stats: f.fetchStats}, nil
}

// FetchSpans implements traceql.SpansetFetcher. Span-only fetching is
// used by Tempo's metrics engine, which this proxy does not use.
func (f *Fetcher) FetchSpans(context.Context, traceql.FetchSpansRequest) (traceql.FetchSpansOnlyResponse, error) {
	return traceql.FetchSpansOnlyResponse{}, util.ErrUnsupported
}

func (f *Fetcher) fetchStats() traceql.FetchSpansStats {
	return traceql.FetchSpansStats{ValuesMatched: uint64(f.stats.SpansFetched)}
}

func (f *Fetcher) window(startNs, endNs uint64) (time.Time, time.Time) {
	now := time.Now()
	end := now
	if endNs > 0 {
		end = time.Unix(0, int64(endNs))
	}
	start := end.Add(-f.opts.DefaultLookback)
	if startNs > 0 {
		start = time.Unix(0, int64(startNs))
	}
	return start, end
}

// candidates runs the trace-level prefilter query.
func (f *Fetcher) candidates(ctx context.Context, start, end time.Time) ([]string, error) {
	m := f.tr.Mapping()
	q := apl.NewQuery(f.opts.Dataset).Where(m.SpansOnly())

	if any := f.plan.anySpanWhere(); any != "" {
		q.Where(any)
	}
	aggs := make([]string, 0, len(f.plan.filters)+1)
	for i, fl := range f.plan.filters {
		if fl.Where == "" {
			aggs = append(aggs, fmt.Sprintf("m%d = count()", i))
		} else {
			aggs = append(aggs, fmt.Sprintf("m%d = countif(%s)", i, fl.Where))
		}
	}
	aggs = append(aggs, "start = min("+m.Time().Expr+")")
	prefer := f.plan.prefer
	if f.opts.NoPreferSelected {
		prefer = nil
	}
	if len(prefer) > 0 {
		present := make([]string, len(prefer))
		for i, c := range prefer {
			present[i] = apl.Call("isnotnull", c)
		}
		aggs = append(aggs, "sel = countif("+apl.Or(present...)+")")
	}
	q.Summarize(aggs, []string{m.TraceID().Expr})
	q.Where(f.plan.traceWhere)
	if len(prefer) > 0 {
		q.Extend("_pref = iff(sel > 0, 1, 0)")
		q.Sort("_pref desc", "start desc")
	} else {
		q.Sort("start desc")
	}
	q.Limit(f.opts.MaxTraces)

	res, err := f.run(ctx, q.String(), start, end)
	if err != nil {
		return nil, err
	}
	t := res.FirstTable()
	if t == nil {
		return nil, nil
	}
	idCol := m.Config().TraceID
	ids := make([]string, 0, t.NumRows())
	for _, row := range t.Rows() {
		if id := row.String(idCol); id != "" {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// pullTraces fetches all spans of the candidate traces and groups them,
// preserving the candidate order.
func (f *Fetcher) pullTraces(ctx context.Context, ids []string, start, end time.Time) ([]*spans.Trace, error) {
	m := f.tr.Mapping()
	q := apl.NewQuery(f.opts.Dataset).
		Where(translate.TraceIDsFilter(m, ids)).
		Limit(f.opts.MaxSpans)

	res, err := f.run(ctx, q.String(), start.Add(-f.opts.TracePadding), end.Add(f.opts.TracePadding))
	if err != nil {
		return nil, err
	}
	all := f.parser.Parse(res.FirstTable())
	f.stats.SpansFetched += len(all)
	traces := spans.GroupTraces(all)

	byID := make(map[string]*spans.Trace, len(traces))
	for _, t := range traces {
		byID[t.HexTraceID()] = t
	}
	ordered := make([]*spans.Trace, 0, len(traces))
	seen := map[string]bool{}
	for _, id := range ids {
		key := normaliseTraceID(id)
		if t, ok := byID[key]; ok && !seen[key] {
			ordered = append(ordered, t)
			seen[key] = true
		}
	}
	for _, t := range traces {
		if !seen[t.HexTraceID()] {
			ordered = append(ordered, t)
		}
	}
	return ordered, nil
}

func normaliseTraceID(id string) string {
	b, err := util.HexStringToTraceID(id)
	if err != nil {
		return id
	}
	return util.TraceIDToHexString(b)
}

func (f *Fetcher) run(ctx context.Context, query string, start, end time.Time) (*axiom.Result, error) {
	f.stats.Queries++
	if f.opts.LogQueries {
		f.opts.Log.Info("axiom query", "apl", query, "start", start, "end", end)
	}
	res, err := f.client.Query(ctx, query, axiom.QueryOptions{Start: start, End: end})
	if err != nil {
		return nil, fmt.Errorf("axiom query failed: %w", err)
	}
	return res, nil
}

// iterator yields spansets, running the engine's second pass on each.
type iterator struct {
	traces     []*spans.Trace
	secondPass traceql.SecondPassFn
	pending    []*traceql.Spanset
	pos        int
}

func (it *iterator) Next(ctx context.Context) (*traceql.Spanset, error) {
	for {
		if len(it.pending) > 0 {
			ss := it.pending[0]
			it.pending = it.pending[1:]
			return ss, nil
		}
		if it.pos >= len(it.traces) {
			return nil, io.EOF
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ss := it.traces[it.pos].Spanset()
		it.pos++
		if it.secondPass == nil {
			return ss, nil
		}
		out, err := it.secondPass(ss)
		if err != nil {
			return nil, err
		}
		it.pending = out
	}
}

func (it *iterator) Close() {}

// --- prefilter plan ---

type plan struct {
	// filters are the translated spanset filters, indexed by position in
	// the traceWhere expression.
	filters []translate.Filter
	// traceWhere is the APL predicate over m<i> counts selecting traces.
	traceWhere string
	exact      bool
	// prefer lists column expressions for attributes the query select()s.
	// Candidate traces that have any of them are ranked before traces
	// that do not, so a bounded result favours traces carrying the data
	// the caller asked to see (Drilldown's Exceptions tab selects
	// event.exception.* over the 400 most recent errored traces, which
	// would otherwise be dominated by exception-free errors).
	prefer []string
}

// selectedColumns maps the attributes a query select()s onto dataset
// columns whose presence can be tested cheaply.
func selectedColumns(tr *translate.Translator, query string) []string {
	req, err := traceql.ExtractFetchSpansRequest(query)
	if err != nil {
		return nil
	}
	m := tr.Mapping()
	seen := map[string]bool{}
	var cols []string
	add := func(c schemaColumn) {
		if c.Missing || c.Expr == "" || seen[c.Expr] {
			return
		}
		seen[c.Expr] = true
		cols = append(cols, c.Expr)
	}
	for _, c := range req.SecondPassConditions {
		if c.Op != traceql.OpNone {
			continue
		}
		a := c.Attribute
		switch {
		case a.Scope == traceql.AttributeScopeEvent,
			a.Intrinsic == traceql.IntrinsicEventName, a.Intrinsic == traceql.IntrinsicEventTimeSinceStart:
			add(m.Events())
		case a.Scope == traceql.AttributeScopeLink,
			a.Intrinsic == traceql.IntrinsicLinkSpanID, a.Intrinsic == traceql.IntrinsicLinkTraceID:
			add(m.Links())
		case a.Intrinsic != traceql.IntrinsicNone:
			// Intrinsics exist on every span; nothing to prefer.
		default:
			if col, ok := m.Resolve(a); ok {
				add(col)
			}
		}
	}
	return cols
}

type schemaColumn = schema.Column

func (p *plan) String() string {
	var b strings.Builder
	for i, f := range p.filters {
		fmt.Fprintf(&b, "m%d: %q exact=%v; ", i, f.Where, f.Exact)
	}
	fmt.Fprintf(&b, "trace: %q", p.traceWhere)
	return b.String()
}

// anySpanWhere is the span-level prefilter: a span must match at least
// one filter to count. Empty when any filter matches everything.
func (p *plan) anySpanWhere() string {
	parts := make([]string, 0, len(p.filters))
	for _, f := range p.filters {
		if f.Where == "" {
			return ""
		}
		parts = append(parts, f.Where)
	}
	return apl.Or(parts...)
}

func buildPlan(tr *translate.Translator, query string) (*plan, error) {
	root, err := traceql.Parse(query)
	if err != nil {
		return nil, err
	}
	if translate.IsMetricsQuery(query) {
		return nil, errors.New("metrics query passed to search")
	}
	pipeline, ok := root.SinglePipeline()
	if !ok {
		return nil, errors.New("unsupported query structure")
	}
	p := &plan{exact: true}
	where, ok := p.pipelineWhere(tr, pipeline)
	if !ok {
		where = ""
	}
	p.traceWhere = where
	if len(p.filters) == 0 {
		// No spanset filter at all: match every trace.
		p.filters = append(p.filters, translate.Filter{Exact: true})
	}
	p.prefer = selectedColumns(tr, query)
	return p, nil
}

// pipelineWhere renders the trace-level predicate for a pipeline: all
// spanset stages must produce spans, so they are and-ed.
func (p *plan) pipelineWhere(tr *translate.Translator, pipe traceql.Pipeline) (string, bool) {
	var parts []string
	for _, el := range pipe.Elements {
		switch v := el.(type) {
		case *traceql.SpansetFilter:
			parts = append(parts, p.leaf(tr, v.Expression))
		case *traceql.SpansetOperation:
			w, ok := p.operationWhere(tr, v)
			if !ok {
				return "", false
			}
			parts = append(parts, w)
		case traceql.SpansetOperation:
			w, ok := p.operationWhere(tr, &v)
			if !ok {
				return "", false
			}
			parts = append(parts, w)
		case traceql.Pipeline:
			w, ok := p.pipelineWhere(tr, v)
			if !ok {
				return "", false
			}
			parts = append(parts, w)
		default:
			// Scalar filters, by(), coalesce(), select() run after span
			// selection and never widen the set of matching traces.
		}
	}
	return apl.And(parts...), true
}

func (p *plan) leaf(tr *translate.Translator, e traceql.FieldExpression) string {
	f := tr.Filter(e)
	if !f.Exact {
		p.exact = false
	}
	idx := len(p.filters)
	p.filters = append(p.filters, f)
	return fmt.Sprintf("m%d > 0", idx)
}

func (p *plan) exprWhere(tr *translate.Translator, e traceql.SpansetExpression) (string, bool) {
	switch v := e.(type) {
	case *traceql.SpansetFilter:
		return p.leaf(tr, v.Expression), true
	case *traceql.SpansetOperation:
		return p.operationWhere(tr, v)
	case traceql.SpansetOperation:
		return p.operationWhere(tr, &v)
	case traceql.Pipeline:
		return p.pipelineWhere(tr, v)
	}
	return "", false
}

func (p *plan) operationWhere(tr *translate.Translator, op *traceql.SpansetOperation) (string, bool) {
	l, lok := p.exprWhere(tr, op.LHS)
	r, rok := p.exprWhere(tr, op.RHS)
	switch op.Op {
	case traceql.OpSpansetAnd,
		traceql.OpSpansetChild, traceql.OpSpansetParent,
		traceql.OpSpansetDescendant, traceql.OpSpansetAncestor,
		traceql.OpSpansetSibling:
		// Both sides must have spans in the trace.
		if !lok || !rok {
			return "", false
		}
		return apl.And(l, r), true
	case traceql.OpSpansetUnion,
		traceql.OpSpansetUnionChild, traceql.OpSpansetUnionParent,
		traceql.OpSpansetUnionDescendant, traceql.OpSpansetUnionAncestor,
		traceql.OpSpansetUnionSibling:
		if !lok || !rok {
			return "", false
		}
		return apl.Or(l, r), true
	case traceql.OpSpansetNotChild, traceql.OpSpansetNotParent,
		traceql.OpSpansetNotDescendant, traceql.OpSpansetNotAncestor,
		traceql.OpSpansetNotSibling:
		// Result spans come from the right side only.
		if !rok {
			return "", false
		}
		return r, true
	}
	return "", false
}

// FetchTrace pulls one trace by hex id within the window.
func FetchTrace(ctx context.Context, client *axiom.Client, tr *translate.Translator, opts Options, hexID string, start, end time.Time) (*spans.Trace, error) {
	opts = opts.withDefaults()
	m := tr.Mapping()
	q := apl.NewQuery(opts.Dataset).
		Where(translate.TraceIDFilter(m, hexID)).
		Limit(opts.MaxSpans)
	query := q.String()
	if opts.LogQueries {
		opts.Log.Info("axiom query", "apl", query, "start", start, "end", end)
	}
	res, err := client.Query(ctx, query, axiom.QueryOptions{Start: start, End: end})
	if err != nil {
		return nil, fmt.Errorf("axiom query failed: %w", err)
	}
	all := spans.NewParser(m).Parse(res.FirstTable())
	traces := spans.GroupTraces(all)
	if len(traces) == 0 {
		return nil, nil
	}
	return traces[0], nil
}
