package metrics

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	common "github.com/grafana/tempo/pkg/tempopb/common/v1"
	"github.com/grafana/tempo/pkg/traceql"

	"github.com/velddev/axiom-tempo-proxy/internal/apl"
	"github.com/velddev/axiom-tempo-proxy/internal/schema"
	"github.com/velddev/axiom-tempo-proxy/internal/spans"
	"github.com/velddev/axiom-tempo-proxy/internal/translate"
)

// compareAttr is one attribute examined by compare().
type compareAttr struct {
	attr traceql.Attribute
	col  schema.Column
}

// compareAttributes lists the attributes compare() breaks down by: the
// intrinsics Drilldown cares about plus every flattened span and resource
// attribute known in the dataset.
func (e *Evaluator) compareAttributes() []compareAttr {
	m := e.tr.Mapping()
	var out []compareAttr
	add := func(a traceql.Attribute) {
		if col, ok := m.Resolve(a); ok && !col.Missing {
			out = append(out, compareAttr{attr: a, col: col})
		}
	}
	add(traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, "service.name"))
	add(traceql.NewIntrinsic(traceql.IntrinsicName))
	add(traceql.NewIntrinsic(traceql.IntrinsicStatus))
	add(traceql.NewIntrinsic(traceql.IntrinsicKind))
	for _, name := range m.TagNames(traceql.AttributeScopeSpan) {
		add(traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, name))
	}
	for _, name := range m.TagNames(traceql.AttributeScopeResource) {
		if name == "service.name" {
			continue
		}
		add(traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, name))
	}
	if len(out) > e.opts.MaxCompareAttributes {
		out = out[:e.opts.MaxCompareAttributes]
	}
	return out
}

// evalCompare implements compare({selection}, topN, start, end): for each
// attribute, per-value counts of spans in the selection versus the
// baseline (spans matching the query filter but not the selection), plus
// total series for both sets.
func (e *Evaluator) evalCompare(ctx context.Context, mq *translate.MetricsQuery, req Request) ([]*series, error) {
	m := e.tr.Mapping()
	step := time.Duration(req.StepNs)
	start := time.Unix(0, int64(req.StartNs))
	end := time.Unix(0, int64(req.EndNs))
	where, err := e.filterWhere(ctx, mq, start, end)
	if err != nil {
		return nil, err
	}
	if translate.SplitTrace(mq.Compare.Selection).Uses.Any() {
		// The selection splits each bucket in two, so it has to be
		// evaluable per span.
		return nil, &UnsupportedError{Reason: "compare() selections cannot use trace-level intrinsics"}
	}
	sel := e.tr.Filter(mq.Compare.Selection)
	if !sel.Exact {
		return nil, &UnsupportedError{Reason: "compare() selection cannot be evaluated in APL"}
	}
	selWhere := sel.Where
	if selWhere == "" {
		selWhere = "true"
	}
	if mq.Compare.StartNs > 0 && mq.Compare.EndNs > 0 {
		selWhere = apl.And(selWhere,
			m.Time().Expr+" >= "+apl.Datetime(time.Unix(0, mq.Compare.StartNs)),
			m.Time().Expr+" < "+apl.Datetime(time.Unix(0, mq.Compare.EndNs)))
	}
	topN := mq.Compare.TopN
	if topN <= 0 {
		topN = maxCompareValuesDef
	}

	bucket := "_bucket = bin(" + m.Time().Expr + ", " + apl.Timespan(step) + ")"
	selCol := "_sel = iff(" + selWhere + ", true, false)"

	// Totals.
	totalsQ := apl.NewQuery(e.opts.Dataset).Where(m.SpansOnly()).Where(where).Extend(selCol).
		Summarize([]string{"v = count()"}, []string{bucket, "_sel"})
	totals, err := e.run(ctx, totalsQ.String(), start, end)
	if err != nil {
		return nil, err
	}
	var out []*series
	baseTotal := &series{labels: metaLabels(MetaBaselineTotal), samples: map[int64]float64{}}
	selTotal := &series{labels: metaLabels(MetaSelectionTotal), samples: map[int64]float64{}}
	if t := totals.FirstTable(); t != nil {
		for _, row := range t.Rows() {
			ts := row.Time("_bucket")
			if ts.IsZero() {
				continue
			}
			if row.Bool("_sel") {
				selTotal.samples[ts.UnixMilli()] += row.Float64("v")
			} else {
				baseTotal.samples[ts.UnixMilli()] += row.Float64("v")
			}
		}
	}
	zeroFill(baseTotal, req)
	zeroFill(selTotal, req)
	out = append(out, baseTotal, selTotal)

	// Per-attribute breakdowns, in parallel.
	attrs := e.compareAttributes()
	type result struct {
		idx    int
		series []*series
		err    error
	}
	results := make([]result, len(attrs))
	sem := make(chan struct{}, e.opts.CompareConcurrency)
	var wg sync.WaitGroup
	for i, ca := range attrs {
		wg.Add(1)
		go func(i int, ca compareAttr) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			s, err := e.compareAttribute(ctx, ca, where, selCol, bucket, topN, start, end, req)
			results[i] = result{idx: i, series: s, err: err}
		}(i, ca)
	}
	wg.Wait()
	for _, r := range results {
		if r.err != nil {
			if ctx.Err() != nil {
				// The client went away; nothing to report.
				return nil, ctx.Err()
			}
			// One bad attribute (for example a type mismatch) should not
			// fail the whole comparison.
			e.opts.Log.Warn("compare attribute failed", "attribute", attrs[r.idx].attr.String(), "err", r.err)
			continue
		}
		out = append(out, r.series...)
	}
	return out, nil
}

func (e *Evaluator) compareAttribute(ctx context.Context, ca compareAttr, where, selCol, bucket string, topN int, start, end time.Time, req Request) ([]*series, error) {
	q := apl.NewQuery(e.opts.Dataset).Where(e.tr.Mapping().SpansOnly()).Where(where).
		Where(apl.Call("isnotnull", ca.col.Expr)).
		Extend(selCol, "_v = "+ca.col.Expr).
		Summarize([]string{"v = count()"}, []string{bucket, "_sel", "_v"})
	res, err := e.run(ctx, q.String(), start, end)
	if err != nil {
		return nil, err
	}
	t := res.FirstTable()
	if t == nil {
		return nil, nil
	}

	type valueSeries struct {
		static   traceql.Static
		baseline *series
		selected *series
		selCount float64
		total    float64
	}
	byValue := map[string]*valueSeries{}
	for _, row := range t.Rows() {
		ts := row.Time("_bucket")
		raw := row.Raw("_v")
		if ts.IsZero() || raw == nil {
			continue
		}
		st, ok := spans.StaticFromJSON(raw)
		if !ok {
			continue
		}
		st = normaliseIntrinsic(ca.attr, st)
		key := st.EncodeToString(true)
		vs, ok := byValue[key]
		if !ok {
			kv := &common.KeyValue{Key: ca.attr.String(), Value: st.AsAnyValue()}
			vs = &valueSeries{
				static:   st,
				baseline: &series{labels: append(metaLabels(MetaBaseline), kv), samples: map[int64]float64{}},
				selected: &series{labels: append(metaLabels(MetaSelection), kv), samples: map[int64]float64{}},
			}
			byValue[key] = vs
		}
		v := row.Float64("v")
		vs.total += v
		if row.Bool("_sel") {
			vs.selected.samples[ts.UnixMilli()] += v
			vs.selCount += v
		} else {
			vs.baseline.samples[ts.UnixMilli()] += v
		}
	}
	if len(byValue) == 0 {
		return nil, nil
	}
	values := make([]*valueSeries, 0, len(byValue))
	for _, vs := range byValue {
		values = append(values, vs)
	}
	sort.SliceStable(values, func(i, j int) bool {
		if values[i].selCount != values[j].selCount {
			return values[i].selCount > values[j].selCount
		}
		return values[i].total > values[j].total
	})
	var out []*series
	for i, vs := range values {
		if i >= topN {
			// Tempo flags attributes with more values than fit.
			out = append(out, &series{
				labels:  append(metaLabels(MetaBaseline), &common.KeyValue{Key: ca.attr.String(), Value: traceql.NewStaticNil().AsAnyValue()}, &common.KeyValue{Key: LabelMetaError, Value: traceql.NewStaticString(ErrorTooManyValues).AsAnyValue()}),
				samples: map[int64]float64{},
			})
			break
		}
		zeroFill(vs.baseline, req)
		zeroFill(vs.selected, req)
		out = append(out, vs.baseline, vs.selected)
	}
	return out, nil
}

func metaLabels(metaType string) []*common.KeyValue {
	return []*common.KeyValue{{Key: LabelMetaType, Value: traceql.NewStaticString(metaType).AsAnyValue()}}
}

var _ = fmt.Sprintf
