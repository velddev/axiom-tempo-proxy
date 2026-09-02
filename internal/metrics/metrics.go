// Package metrics evaluates TraceQL metrics queries as native APL
// aggregations and renders Tempo QueryRangeResponse results.
package metrics

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/tempo/pkg/tempopb"
	common "github.com/grafana/tempo/pkg/tempopb/common/v1"
	"github.com/grafana/tempo/pkg/traceql"

	"github.com/velddev/axiom-tempo-proxy/internal/apl"
	"github.com/velddev/axiom-tempo-proxy/internal/axiom"
	"github.com/velddev/axiom-tempo-proxy/internal/schema"
	"github.com/velddev/axiom-tempo-proxy/internal/spans"
	"github.com/velddev/axiom-tempo-proxy/internal/translate"
)

// Tempo's internal series labels.
const (
	LabelP              = "p"
	LabelTraceID        = "trace:id"
	LabelBucket         = "__bucket"
	LabelMetaType       = "__meta_type"
	LabelMetaError      = "__meta_error"
	MetaBaseline        = "baseline"
	MetaSelection       = "selection"
	MetaBaselineTotal   = "baseline_total"
	MetaSelectionTotal  = "selection_total"
	ErrorTooManyValues  = "__too_many_values__"
	maxCompareValuesDef = 10
)

// defaultMaxTraceIntrinsicTraces bounds how many trace ids a trace-level
// filter may resolve to before the query is refused. It is also the cap
// passed to make_list(), and matches the point where Axiom starts
// truncating a per-trace summarize anyway.
const defaultMaxTraceIntrinsicTraces = 5000

// UnsupportedError reports a query the native translation cannot run
// exactly. Handlers map it to a 400 so users see why.
type UnsupportedError struct{ Reason string }

func (e *UnsupportedError) Error() string { return "unsupported metrics query: " + e.Reason }

// Options configure the evaluator.
type Options struct {
	Dataset string
	// MaxSeries caps the series returned.
	MaxSeries int
	// MaxCompareAttributes caps the attributes examined by compare().
	MaxCompareAttributes int
	// CompareConcurrency bounds parallel compare() queries.
	CompareConcurrency int
	// DefaultExemplars is used when a request asks for no particular
	// number, as Tempo's max_exemplars is.
	DefaultExemplars int
	// MaxExemplars caps the exemplars returned per series.
	MaxExemplars int
	// MaxTraceIntrinsicTraces caps the traces a trace-level filter
	// (traceDuration, rootName, rootServiceName) may resolve to.
	MaxTraceIntrinsicTraces int
	Log                     *slog.Logger
	LogQueries              bool
}

func (o Options) withDefaults() Options {
	if o.MaxSeries <= 0 {
		o.MaxSeries = 1000
	}
	if o.MaxCompareAttributes <= 0 {
		o.MaxCompareAttributes = 40
	}
	if o.CompareConcurrency <= 0 {
		o.CompareConcurrency = 4
	}
	if o.MaxExemplars <= 0 {
		o.MaxExemplars = 1000
	}
	if o.DefaultExemplars < 0 {
		o.DefaultExemplars = 0
	}
	if o.MaxTraceIntrinsicTraces <= 0 {
		o.MaxTraceIntrinsicTraces = defaultMaxTraceIntrinsicTraces
	}
	if o.Log == nil {
		o.Log = slog.Default()
	}
	return o
}

// Request is a metrics query range request.
type Request struct {
	Query   string
	StartNs uint64
	EndNs   uint64
	StepNs  uint64
	// Exemplars is the request's `exemplars` parameter: how many
	// exemplars to return per series. Zero means "unspecified" and falls
	// back to Options.DefaultExemplars, exactly as Tempo's
	// normalizeRequestExemplars does. A with(exemplars=...) hint in the
	// query overrides it.
	Exemplars int
}

// Evaluator runs metrics queries. One Evaluator serves one request: it
// accumulates the partial-result status of every Axiom query it runs so
// the response can say that the numbers are incomplete.
type Evaluator struct {
	client *axiom.Client
	tr     *translate.Translator
	opts   Options

	mu       sync.Mutex
	partial  bool
	warnings []string
	seen     map[string]bool
}

// New creates an Evaluator.
func New(client *axiom.Client, tr *translate.Translator, opts Options) *Evaluator {
	return &Evaluator{client: client, tr: tr, opts: opts.withDefaults()}
}

// exemplar is one trace pinned to a bucket of a series.
type exemplar struct {
	traceID string
	value   float64
	tsMs    int64
}

// series is an intermediate time series keyed by its labels.
type series struct {
	labels  []*common.KeyValue
	samples map[int64]float64 // bucket start ms -> value
	// exemplars holds at most one exemplar per bucket, keyed the same way
	// as samples so second-stage filtering can drop them together.
	exemplars map[int64]exemplar
}

func (s *series) addExemplar(bucketMs int64, ex exemplar) {
	if ex.traceID == "" || ex.tsMs <= 0 || ex.value == 0 {
		// Grafana's Tempo datasource drops exemplars with value 0 or a
		// non-positive timestamp, so never emit them.
		return
	}
	if s.exemplars == nil {
		s.exemplars = map[int64]exemplar{}
	}
	s.exemplars[bucketMs] = ex
}

func (s *series) key() string { return labelsKey(s.labels) }

func labelsKey(labels []*common.KeyValue) string {
	parts := make([]string, 0, len(labels))
	for _, l := range labels {
		parts = append(parts, l.Key+"="+traceql.StaticFromAnyValue(l.Value).EncodeToString(true))
	}
	return strings.Join(parts, ",")
}

// QueryRange evaluates a metrics query over a time range.
func (e *Evaluator) QueryRange(ctx context.Context, req Request) (*tempopb.QueryRangeResponse, error) {
	return e.queryRange(ctx, req, true)
}

func (e *Evaluator) queryRange(ctx context.Context, req Request, exemplars bool) (*tempopb.QueryRangeResponse, error) {
	mq, err := translate.ParseMetrics(req.Query)
	if err != nil {
		return nil, err
	}
	req.StartNs, req.EndNs, req.StepNs = align(req.StartNs, req.EndNs, req.StepNs)
	if exemplars {
		req.Exemplars = e.exemplarCount(mq, req.Exemplars)
	} else {
		req.Exemplars = 0
	}

	out, err := e.eval(ctx, mq, req)
	if err != nil {
		return nil, err
	}
	res := e.render(out, req)
	e.applyStatus(res)
	return res, nil
}

// note records the partial-result status of one Axiom result.
func (e *Evaluator) note(res *axiom.Result) {
	if res == nil {
		return
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if res.Status.IsPartial {
		e.partial = true
	}
	for _, w := range res.Status.Warnings() {
		if e.seen[w] {
			continue
		}
		if e.seen == nil {
			e.seen = map[string]bool{}
		}
		e.seen[w] = true
		e.warnings = append(e.warnings, w)
	}
}

// applyStatus stamps Axiom's partial-result status and status messages
// onto the response, keeping any message render already set.
func (e *Evaluator) applyStatus(res *tempopb.QueryRangeResponse) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if !e.partial && len(e.warnings) == 0 {
		return
	}
	var parts []string
	if res.Message != "" {
		parts = append(parts, res.Message)
	}
	if e.partial {
		res.Status = tempopb.PartialStatus_PARTIAL
		parts = append(parts, "axiom returned partial results")
	}
	parts = append(parts, e.warnings...)
	res.Message = strings.Join(parts, "; ")
	e.opts.Log.Warn("metrics query returned partial results", "partial", e.partial, "message", res.Message)
}

// exemplarCount resolves how many exemplars per series to produce,
// mirroring Tempo's normalizeRequestExemplars: an integer
// with(exemplars=N) hint wins, with(exemplars=false) disables them, and
// a request that names no number gets the configured default.
// with(exemplars=true) is a no-op in Tempo, and an explicit
// exemplars=0 means "unspecified", not "none".
func (e *Evaluator) exemplarCount(mq *translate.MetricsQuery, requested int) int {
	hint := strings.ToLower(strings.TrimSpace(mq.Hints["exemplars"]))
	n, notInt := strconv.Atoi(hint)
	switch {
	case notInt == nil:
		requested = max(n, 0)
	case hint == "false":
		requested = 0
	case requested <= 0:
		requested = e.opts.DefaultExemplars
	}
	if requested < 0 {
		return 0
	}
	return min(requested, e.opts.MaxExemplars)
}

// QueryInstant evaluates a metrics query as a single bucket.
func (e *Evaluator) QueryInstant(ctx context.Context, req Request) (*tempopb.QueryInstantResponse, error) {
	req.StepNs = req.EndNs - req.StartNs
	if req.StepNs == 0 {
		req.StepNs = 1
	}
	// InstantSeries has no exemplars, and Tempo forces exemplars off for
	// instant queries, so do not pay for the aggregation.
	rr, err := e.queryRange(ctx, req, false)
	if err != nil {
		return nil, err
	}
	res := &tempopb.QueryInstantResponse{Metrics: rr.Metrics, Status: rr.Status, Message: rr.Message}
	for _, s := range rr.Series {
		var v float64
		for _, smp := range s.Samples {
			v += smp.Value
		}
		res.Series = append(res.Series, &tempopb.InstantSeries{Labels: s.Labels, Value: v})
	}
	return res, nil
}

func (e *Evaluator) eval(ctx context.Context, mq *translate.MetricsQuery, req Request) ([]*series, error) {
	if mq.Math != nil {
		return e.evalMath(ctx, mq.Math, req)
	}
	var out []*series
	var err error
	if mq.Func == translate.FuncCompare {
		out, err = e.evalCompare(ctx, mq, req)
	} else {
		out, err = e.evalAggregate(ctx, mq, req)
	}
	if errors.Is(err, errNoData) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	for _, st := range mq.SecondStage {
		out = applySecondStage(out, st)
	}
	return out, nil
}

func (e *Evaluator) evalMath(ctx context.Context, me *translate.MathExpr, req Request) ([]*series, error) {
	lhs, err := e.eval(ctx, me.LHS, req)
	if err != nil {
		return nil, err
	}
	op := func(a, b float64) float64 {
		switch me.Op {
		case "+":
			return a + b
		case "-":
			return a - b
		case "*":
			return a * b
		case "/":
			if b == 0 {
				return math.NaN()
			}
			return a / b
		}
		return math.NaN()
	}
	if me.RHSNum != nil {
		for _, s := range lhs {
			for k, v := range s.samples {
				s.samples[k] = op(v, *me.RHSNum)
			}
		}
		return lhs, nil
	}
	rhs, err := e.eval(ctx, me.RHS, req)
	if err != nil {
		return nil, err
	}
	byKey := make(map[string]*series, len(rhs))
	for _, s := range rhs {
		byKey[s.key()] = s
	}
	var out []*series
	for _, l := range lhs {
		r, ok := byKey[l.key()]
		if !ok {
			// A single unlabeled rhs series applies to everything.
			if len(rhs) == 1 && len(rhs[0].labels) == 0 {
				r = rhs[0]
			} else {
				continue
			}
		}
		res := &series{labels: l.labels, samples: map[int64]float64{}}
		for ts, lv := range l.samples {
			if rv, ok := r.samples[ts]; ok {
				v := op(lv, rv)
				if !math.IsNaN(v) && !math.IsInf(v, 0) {
					res.samples[ts] = v
				}
			}
		}
		out = append(out, res)
	}
	return out, nil
}

// groupCol is a by() attribute rendered as an APL column.
type groupCol struct {
	attr  traceql.Attribute
	alias string
	col   schema.Column
}

// errNoData signals that a referenced attribute does not exist in the
// dataset, so the result is empty rather than an error.
var errNoData = errors.New("attribute absent from dataset")

func (e *Evaluator) groupCols(by []traceql.Attribute) ([]groupCol, error) {
	m := e.tr.Mapping()
	out := make([]groupCol, 0, len(by))
	for i, a := range by {
		col, ok := m.Resolve(a)
		if !ok {
			return nil, &UnsupportedError{Reason: fmt.Sprintf("cannot group by %s", a.String())}
		}
		if col.Missing {
			return nil, errNoData
		}
		out = append(out, groupCol{attr: a, alias: fmt.Sprintf("g%d", i), col: col})
	}
	return out, nil
}

// filterWhere renders the query's spanset filter as an APL predicate,
// requiring exactness. Trace-level intrinsics are split off and resolved
// against the dataset first (see traceFilter), so the returned predicate
// is always evaluable row by row.
func (e *Evaluator) filterWhere(ctx context.Context, mq *translate.MetricsQuery, start, end time.Time) (string, error) {
	if !mq.FilterExact {
		return "", &UnsupportedError{Reason: "spanset pipelines other than filters are not supported in metrics queries"}
	}
	split := translate.SplitTrace(mq.Filter)
	if split.Mixed {
		return "", unsupportedMixed(mq.Filter)
	}

	var spanWhere string
	if split.Span != nil {
		f := e.tr.Filter(split.Span)
		if !f.Exact {
			return "", &UnsupportedError{Reason: "filter uses attributes that cannot be evaluated in APL: " + strings.Join(f.Unsupported, ", ")}
		}
		spanWhere = f.Where
	}
	if split.Trace == nil {
		return spanWhere, nil
	}
	traceWhere, err := e.traceFilter(ctx, split, spanWhere, start, end)
	if err != nil {
		return "", err
	}
	return apl.And(spanWhere, traceWhere), nil
}

// valueExpr renders the attribute an aggregation operates on as a
// numeric expression, in seconds for durations as Tempo does.
func (e *Evaluator) valueExpr(a traceql.Attribute) (string, bool, error) {
	col, ok := e.tr.Mapping().Resolve(a)
	if !ok {
		return "", false, &UnsupportedError{Reason: "cannot aggregate over " + a.String()}
	}
	if col.Missing {
		return "", false, errNoData
	}
	switch col.Type {
	case schema.TypeDuration:
		return "toreal(" + col.Expr + " / 1s)", true, nil
	case schema.TypeInt, schema.TypeFloat:
		return "toreal(" + col.Expr + ")", false, nil
	}
	return "toreal(" + col.Expr + ")", false, nil
}

// exemplarAggregation describes the extra summarize aggregation that
// pins one exemplar span to every bucket of every series.
type exemplarAggregation struct {
	ok bool
	// extends are the extend assignments the aggregation reads.
	extends []string
	// agg is the arg_max/arg_min aggregation itself. arg_max(expr, a, b)
	// returns a and b as columns under their own names, so the chosen
	// span comes back as _exid/_exts and its value as _exv.
	agg string
	// spanValue is true when the exemplar's value is the chosen span's
	// own value (_exv) rather than the bucket's sample value. Tempo uses
	// the span value for the *_over_time functions that take an
	// attribute, and a placeholder later filled with the sample value
	// for the counting functions (rate, count_over_time,
	// histogram_over_time).
	spanValue bool
}

// exemplarAgg builds the exemplar aggregation for a metrics function.
// valExpr is the numeric expression the metric aggregates over, empty for
// the counting functions.
func (e *Evaluator) exemplarAgg(fn translate.MetricsFunc, valExpr string, want int) exemplarAggregation {
	if want <= 0 {
		return exemplarAggregation{}
	}
	m := e.tr.Mapping()
	id, ts := m.TraceID(), m.Time()
	if id.Missing || ts.Missing {
		return exemplarAggregation{}
	}
	// The trace id and timestamp are aliased first: arg_max names its
	// extra result columns after the columns it is given, and _time
	// collides with the bin() grouping key.
	out := exemplarAggregation{ok: true, extends: []string{"_exid = " + id.Expr, "_exts = " + ts.Expr}}
	switch fn {
	case translate.FuncRate, translate.FuncCountOverTime, translate.FuncHistogramOverTime:
		// These have no per-span value, so any span in the bucket is a
		// valid exemplar. Prefer the slowest one as the most interesting
		// trace to open; coalesce keeps a trace id even for buckets whose
		// spans all lack a duration.
		order := "_exts"
		if d := m.Duration(); !d.Missing {
			order = "coalesce(toreal(" + d.Expr + " / 1s), 0.0)"
		}
		out.agg = "_exv = arg_max(" + order + ", _exid, _exts)"
	case translate.FuncMinOverTime:
		// So the exemplar is the span the series value reports.
		out.agg = "_exv = arg_min(" + valExpr + ", _exid, _exts)"
		out.spanValue = true
	default:
		out.agg = "_exv = arg_max(" + valExpr + ", _exid, _exts)"
		out.spanValue = true
	}
	return out
}

// read pulls the exemplar of one summarize row. sample is the bucket's
// own value, used when the function has no per-span value.
func (x exemplarAggregation) read(row axiom.Row, bucketMs int64, sample float64) (exemplar, bool) {
	if !x.ok {
		return exemplar{}, false
	}
	id, ok := traceIDHex(row.String("_exid"))
	if !ok {
		return exemplar{}, false
	}
	// Tempo timestamps an exemplar with the span's own start time, not
	// the bucket start.
	tsMs := bucketMs
	if t := row.Time("_exts"); !t.IsZero() {
		tsMs = t.UnixMilli()
	}
	v := sample
	if x.spanValue {
		if v, ok = row.TryFloat64("_exv"); !ok {
			return exemplar{}, false
		}
	}
	return exemplar{traceID: id, value: v, tsMs: tsMs}, true
}

// traceIDHex normalises a dataset trace id to the 32 lowercase hex
// characters Tempo and Grafana expect, rejecting anything else.
func traceIDHex(s string) (string, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	if s == "" || len(s) > 32 {
		return "", false
	}
	for i := 0; i < len(s); i++ {
		if c := s[i]; (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return "", false
		}
	}
	if len(s) < 32 {
		s = strings.Repeat("0", 32-len(s)) + s
	}
	return s, true
}

func (e *Evaluator) evalAggregate(ctx context.Context, mq *translate.MetricsQuery, req Request) ([]*series, error) {
	m := e.tr.Mapping()
	start := time.Unix(0, int64(req.StartNs))
	end := time.Unix(0, int64(req.EndNs))
	where, err := e.filterWhere(ctx, mq, start, end)
	if err != nil {
		return nil, err
	}
	groups, err := e.groupCols(mq.By)
	if err != nil {
		return nil, err
	}

	step := time.Duration(req.StepNs)
	q := apl.NewQuery(e.opts.Dataset).Where(m.SpansOnly()).Where(where)

	var aggs []string
	byCols := []string{"_bucket = bin(" + m.Time().Expr + ", " + apl.Timespan(step) + ")"}
	for _, g := range groups {
		byCols = append(byCols, g.alias+" = "+g.col.Expr)
		q.Where(apl.Call("isnotnull", g.col.Expr))
	}

	var isDuration bool
	var valExpr string
	switch mq.Func {
	case translate.FuncRate, translate.FuncCountOverTime:
		aggs = []string{"v = count()"}
	case translate.FuncMinOverTime, translate.FuncMaxOverTime, translate.FuncAvgOverTime, translate.FuncSumOverTime:
		expr, dur, err := e.valueExpr(mq.Attr)
		if err != nil {
			return nil, err
		}
		isDuration, valExpr = dur, expr
		fn := map[translate.MetricsFunc]string{
			translate.FuncMinOverTime: "min", translate.FuncMaxOverTime: "max",
			translate.FuncAvgOverTime: "avg", translate.FuncSumOverTime: "sum",
		}[mq.Func]
		q.Where(apl.Call("isnotnull", expr))
		aggs = []string{"v = " + fn + "(" + expr + ")"}
	case translate.FuncQuantileOverTime:
		expr, dur, err := e.valueExpr(mq.Attr)
		if err != nil {
			return nil, err
		}
		isDuration, valExpr = dur, expr
		q.Where(apl.Call("isnotnull", expr))
		for i, qt := range mq.Quantiles {
			aggs = append(aggs, fmt.Sprintf("q%d = percentile(%s, %s)", i, expr, apl.Float(qt*100)))
		}
	case translate.FuncHistogramOverTime:
		expr, dur, err := e.valueExpr(mq.Attr)
		if err != nil {
			return nil, err
		}
		isDuration = dur
		q.Where(apl.Call("isnotnull", expr))
		// Tempo buckets values into powers of two of the raw (nanosecond)
		// value; work in the same units so bucket bounds match.
		raw := expr
		if dur {
			raw = "(" + expr + " * 1000000000.0)"
		}
		q.Extend("_bucketv = pow(2.0, ceiling(log2(" + raw + ")))")
		byCols = append(byCols, "_bucketv")
		aggs = []string{"v = count()"}
	default:
		return nil, &UnsupportedError{Reason: "function " + string(mq.Func)}
	}
	_ = isDuration

	ex := e.exemplarAgg(mq.Func, valExpr, req.Exemplars)
	if ex.ok {
		q.Extend(ex.extends...)
		aggs = append(aggs, ex.agg)
	}

	q.Summarize(aggs, byCols)
	q.Sort("_bucket asc")

	res, err := e.run(ctx, q.String(), start, end)
	if err != nil {
		return nil, err
	}

	stepSeconds := float64(req.StepNs) / 1e9
	bySeries := map[string]*series{}
	var order []string
	get := func(labels []*common.KeyValue) *series {
		k := labelsKey(labels)
		s, ok := bySeries[k]
		if !ok {
			s = &series{labels: labels, samples: map[int64]float64{}}
			bySeries[k] = s
			order = append(order, k)
		}
		return s
	}

	t := res.FirstTable()
	if t != nil {
		for _, row := range t.Rows() {
			ts := row.Time("_bucket")
			if ts.IsZero() {
				continue
			}
			tsMs := ts.UnixMilli()
			labels, ok := e.groupLabels(groups, row)
			if !ok {
				continue
			}
			// attach records the exemplar of this row on a series once its
			// sample value is known.
			attach := func(s *series, sample float64) {
				if x, ok := ex.read(row, tsMs, sample); ok {
					s.addExemplar(tsMs, x)
				}
			}
			switch mq.Func {
			case translate.FuncRate:
				v := row.Float64("v") / stepSeconds
				s := get(labels)
				s.samples[tsMs] = v
				attach(s, v)
			case translate.FuncQuantileOverTime:
				// Tempo attaches an exemplar to exactly one quantile
				// series: the one whose value it is closest to.
				best, bestDist := -1, math.Inf(1)
				for i := range mq.Quantiles {
					v, ok := row.TryFloat64(fmt.Sprintf("q%d", i))
					if !ok {
						continue
					}
					if x, ok := ex.read(row, tsMs, v); ok {
						if d := math.Abs(x.value - v); d < bestDist {
							best, bestDist = i, d
						}
					}
				}
				for i, qt := range mq.Quantiles {
					v, ok := row.TryFloat64(fmt.Sprintf("q%d", i))
					if !ok {
						continue
					}
					l := append(append([]*common.KeyValue{}, labels...), &common.KeyValue{Key: LabelP, Value: traceql.NewStaticFloat(qt).AsAnyValue()})
					s := get(l)
					s.samples[tsMs] = v
					if i == best {
						attach(s, v)
					}
				}
			case translate.FuncHistogramOverTime:
				bucket, ok := row.TryFloat64("_bucketv")
				if !ok {
					// Zero durations have no log2 bucket.
					continue
				}
				var bv traceql.Static
				if isDuration {
					bv = traceql.NewStaticDuration(time.Duration(bucket))
				} else {
					bv = traceql.NewStaticFloat(bucket)
				}
				l := append(append([]*common.KeyValue{}, labels...), &common.KeyValue{Key: LabelBucket, Value: bv.AsAnyValue()})
				v := row.Float64("v")
				s := get(l)
				s.samples[tsMs] = v
				attach(s, v)
			default:
				v, ok := row.TryFloat64("v")
				if ok {
					s := get(labels)
					s.samples[tsMs] = v
					attach(s, v)
				}
			}
		}
	}

	// Counting functions report zero for empty intervals; make sure a
	// filter matching nothing still yields one zero-filled series.
	if (mq.Func == translate.FuncRate || mq.Func == translate.FuncCountOverTime) && len(groups) == 0 && len(order) == 0 {
		get(nil)
	}
	out := make([]*series, 0, len(order))
	for _, k := range order {
		s := bySeries[k]
		if mq.Func == translate.FuncRate || mq.Func == translate.FuncCountOverTime {
			zeroFill(s, req)
		}
		out = append(out, s)
	}
	return out, nil
}

// groupLabels builds series labels from the by() columns of a row.
func (e *Evaluator) groupLabels(groups []groupCol, row axiom.Row) ([]*common.KeyValue, bool) {
	labels := make([]*common.KeyValue, 0, len(groups))
	for _, g := range groups {
		raw := row.Raw(g.alias)
		if raw == nil {
			return nil, false
		}
		st, ok := spans.StaticFromJSON(raw)
		if !ok {
			return nil, false
		}
		st = normaliseIntrinsic(g.attr, st)
		labels = append(labels, &common.KeyValue{Key: g.attr.String(), Value: st.AsAnyValue()})
	}
	return labels, true
}

// normaliseIntrinsic maps raw status/kind column values onto TraceQL's
// enum statics so labels read "error" rather than "STATUS_CODE_ERROR".
func normaliseIntrinsic(a traceql.Attribute, st traceql.Static) traceql.Static {
	if st.Type != traceql.TypeString {
		return st
	}
	s := st.EncodeToString(false)
	switch a.Intrinsic {
	case traceql.IntrinsicStatus, traceql.ScopedIntrinsicSpanStatus:
		switch strings.ToLower(strings.TrimPrefix(strings.ToLower(s), "status_code_")) {
		case "error":
			return traceql.NewStaticStatus(traceql.StatusError)
		case "ok":
			return traceql.NewStaticStatus(traceql.StatusOk)
		default:
			return traceql.NewStaticStatus(traceql.StatusUnset)
		}
	case traceql.IntrinsicKind, traceql.ScopedIntrinsicSpanKind:
		k := strings.ToLower(strings.TrimPrefix(strings.ToLower(s), "span_kind_"))
		for _, kind := range []traceql.Kind{traceql.KindServer, traceql.KindClient, traceql.KindProducer, traceql.KindConsumer, traceql.KindInternal} {
			if kind.String() == k {
				return traceql.NewStaticKind(kind)
			}
		}
		return traceql.NewStaticKind(traceql.KindUnspecified)
	}
	return st
}

func zeroFill(s *series, req Request) {
	for ts := req.StartNs; ts < req.EndNs; ts += req.StepNs {
		ms := int64(ts / 1e6)
		if _, ok := s.samples[ms]; !ok {
			s.samples[ms] = 0
		}
	}
}

// applySecondStage handles topk/bottomk and value comparisons.
func applySecondStage(in []*series, st translate.SecondStage) []*series {
	switch st.Op {
	case "topk", "bottomk":
		if st.N <= 0 {
			return nil
		}
		// Per timestamp keep the k best values.
		timestamps := map[int64]struct{}{}
		for _, s := range in {
			for ts := range s.samples {
				timestamps[ts] = struct{}{}
			}
		}
		keep := make([]map[int64]bool, len(in))
		for i := range keep {
			keep[i] = map[int64]bool{}
		}
		for ts := range timestamps {
			type cand struct {
				idx int
				v   float64
			}
			var cands []cand
			for i, s := range in {
				if v, ok := s.samples[ts]; ok && !math.IsNaN(v) {
					cands = append(cands, cand{i, v})
				}
			}
			sort.SliceStable(cands, func(a, b int) bool {
				if st.Op == "topk" {
					return cands[a].v > cands[b].v
				}
				return cands[a].v < cands[b].v
			})
			for i := 0; i < len(cands) && i < st.N; i++ {
				keep[cands[i].idx][ts] = true
			}
		}
		var out []*series
		for i, s := range in {
			if len(keep[i]) == 0 {
				continue
			}
			filtered := &series{labels: s.labels, samples: map[int64]float64{}}
			for ts, v := range s.samples {
				if keep[i][ts] {
					filtered.samples[ts] = v
				}
			}
			for ts, x := range s.exemplars {
				if keep[i][ts] {
					filtered.addExemplar(ts, x)
				}
			}
			out = append(out, filtered)
		}
		return out
	}
	cmp := func(v float64) bool {
		switch st.Op {
		case ">":
			return v > st.Value
		case ">=":
			return v >= st.Value
		case "<":
			return v < st.Value
		case "<=":
			return v <= st.Value
		case "==":
			return v == st.Value
		case "!=":
			return v != st.Value
		}
		return true
	}
	var out []*series
	for _, s := range in {
		filtered := &series{labels: s.labels, samples: map[int64]float64{}}
		for ts, v := range s.samples {
			if cmp(v) {
				filtered.samples[ts] = v
			}
		}
		for ts, x := range s.exemplars {
			if _, ok := filtered.samples[ts]; ok {
				filtered.addExemplar(ts, x)
			}
		}
		if len(filtered.samples) > 0 {
			out = append(out, filtered)
		}
	}
	return out
}

func (e *Evaluator) render(in []*series, req Request) *tempopb.QueryRangeResponse {
	res := &tempopb.QueryRangeResponse{Metrics: &tempopb.SearchMetrics{CompletedJobs: 1, TotalJobs: 1}}
	sort.SliceStable(in, func(i, j int) bool { return in[i].key() < in[j].key() })
	for i, s := range in {
		if i >= e.opts.MaxSeries {
			res.Status = tempopb.PartialStatus_PARTIAL
			res.Message = fmt.Sprintf("series limit of %d reached", e.opts.MaxSeries)
			break
		}
		ts := &tempopb.TimeSeries{Labels: make([]common.KeyValue, 0, len(s.labels))}
		for _, l := range s.labels {
			ts.Labels = append(ts.Labels, *l)
		}
		keys := make([]int64, 0, len(s.samples))
		for k := range s.samples {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(a, b int) bool { return keys[a] < keys[b] })
		for _, k := range keys {
			v := s.samples[k]
			if math.IsNaN(v) || math.IsInf(v, 0) {
				continue
			}
			ts.Samples = append(ts.Samples, tempopb.Sample{TimestampMs: k, Value: v})
		}
		ts.Exemplars = renderExemplars(s, req.Exemplars)
		res.Series = append(res.Series, ts)
	}
	return res
}

// renderExemplars turns a series' per-bucket exemplars into Tempo's wire
// form, oldest first, thinned to at most want of them by keeping an
// evenly spread subset so the dots still cover the whole range.
func renderExemplars(s *series, want int) []tempopb.Exemplar {
	if want <= 0 || len(s.exemplars) == 0 {
		return nil
	}
	keys := make([]int64, 0, len(s.exemplars))
	for k := range s.exemplars {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(a, b int) bool { return keys[a] < keys[b] })
	if len(keys) > want {
		thinned := make([]int64, 0, want)
		for i := range want {
			thinned = append(thinned, keys[i*len(keys)/want])
		}
		keys = thinned
	}
	out := make([]tempopb.Exemplar, 0, len(keys))
	for _, k := range keys {
		x := s.exemplars[k]
		if math.IsNaN(x.value) || math.IsInf(x.value, 0) {
			continue
		}
		out = append(out, tempopb.Exemplar{
			Labels:      []common.KeyValue{{Key: LabelTraceID, Value: traceql.NewStaticString(x.traceID).AsAnyValue()}},
			Value:       x.value,
			TimestampMs: x.tsMs,
		})
	}
	return out
}

func (e *Evaluator) run(ctx context.Context, query string, start, end time.Time) (*axiom.Result, error) {
	if e.opts.LogQueries {
		e.opts.Log.Info("axiom query", "apl", query, "start", start, "end", end)
	}
	res, err := e.client.Query(ctx, query, axiom.QueryOptions{Start: start, End: end})
	if err != nil {
		return nil, fmt.Errorf("axiom query failed: %w", err)
	}
	e.note(res)
	return res, nil
}

// align snaps the range to step boundaries the way Tempo does, so bucket
// timestamps from bin() line up with the samples Grafana expects.
func align(start, end, step uint64) (uint64, uint64, uint64) {
	if step == 0 {
		step = traceql.DefaultQueryRangeStep(start, end)
	}
	if step == 0 {
		step = uint64(time.Second)
	}
	start = start - start%step
	if end%step != 0 {
		end = end - end%step + step
	}
	return start, end, step
}

// DefaultStep exposes Tempo's default step selection.
func DefaultStep(startNs, endNs uint64) uint64 {
	return traceql.DefaultQueryRangeStep(startNs, endNs)
}

var _ = json.Marshal
