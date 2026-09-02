package translate

import (
	"testing"
)

func TestIsMetricsQuery(t *testing.T) {
	yes := []string{
		`{} | rate()`,
		`{nestedSetParent<0 && true} | rate()  with(sample=true)`,
		`{ } | quantile_over_time(duration, .9) by (span.http.method)`,
		`({status=error} | rate()) / ({} | rate())`,
		`{ span.a = "a || b" } | count_over_time()`,
	}
	no := []string{
		`{}`,
		`{nestedSetParent<0 && true}`,
		`{ span.a = 1 } | select(span.b)`,
		`{ span.a = "rate()" }`,
		`{ span.a = 1 } || { span.b = 2 }`,
	}
	for _, q := range yes {
		if !IsMetricsQuery(q) {
			t.Errorf("expected metrics: %s", q)
		}
	}
	for _, q := range no {
		if IsMetricsQuery(q) {
			t.Errorf("expected not metrics: %s", q)
		}
	}
}

func TestParseMetricsDrilldown(t *testing.T) {
	tr := newTestTranslator(t, true)

	type want struct {
		fn        MetricsFunc
		attr      string
		by        []string
		quantiles []float64
		where     string
		sample    bool
		topN      int
		second    []SecondStage
	}
	cases := []struct {
		q string
		w want
	}{
		{`{nestedSetParent<0 && true} | rate()  with(sample=true)`,
			want{fn: FuncRate, where: `isempty(parent_span_id)`, sample: true}},
		{`{nestedSetParent<0 && true && status=error} | rate()  with(sample=true)`,
			want{fn: FuncRate, where: `(isempty(parent_span_id)) and ((['status.code'] =~ "error") or (['status.code'] =~ "STATUS_CODE_ERROR") or (error == true))`, sample: true}},
		{`{nestedSetParent<0 && true} | histogram_over_time(duration) with(sample=true)`,
			want{fn: FuncHistogramOverTime, attr: "duration", where: `isempty(parent_span_id)`, sample: true}},
		{`{nestedSetParent<0 && true && resource.service.name != nil} | rate() by(resource.service.name)`,
			want{fn: FuncRate, by: []string{"resource.service.name"}, where: `(isempty(parent_span_id)) and (isnotnull(['service.name']))`}},
		{`{nestedSetParent<0 && true && span.http.method != nil} | quantile_over_time(duration, 0.9) by(span.http.method)`,
			want{fn: FuncQuantileOverTime, attr: "duration", quantiles: []float64{0.9}, by: []string{"span.http.method"},
				where: `(isempty(parent_span_id)) and (isnotnull(['attributes.http.method']))`}},
		{`{nestedSetParent<0 && true} | compare({status = error}, 10)`,
			want{fn: FuncCompare, where: `isempty(parent_span_id)`, topN: 10}},
		{`{nestedSetParent<0 && true} | compare({duration >= 1.2s && duration <= 3s}, 10, 1700000000000000000, 1700003600000000000)`,
			want{fn: FuncCompare, where: `isempty(parent_span_id)`, topN: 10}},
		{`{} | rate()`, want{fn: FuncRate}},
		{`{ } | rate() | topk(10)`, want{fn: FuncRate, second: []SecondStage{{Op: "topk", N: 10}}}},
		{`{} | rate() > 5`, want{fn: FuncRate, second: []SecondStage{{Op: ">", Value: 5}}}},
		{`{} | quantile_over_time(duration, .5, .9, .99) by (span.http.method, resource.service.name) | topk(3)`,
			want{fn: FuncQuantileOverTime, attr: "duration", quantiles: []float64{.5, .9, .99},
				by: []string{"span.http.method", "resource.service.name"}, second: []SecondStage{{Op: "topk", N: 3}}}},
		{`{} | max_over_time(span.http.response_size) by (span.http.method) > 100`,
			want{fn: FuncMaxOverTime, attr: "span.http.response_size", by: []string{"span.http.method"}, second: []SecondStage{{Op: ">", Value: 100}}}},
		{`{} | avg_over_time(duration) > 100ms`,
			want{fn: FuncAvgOverTime, attr: "duration", second: []SecondStage{{Op: ">", Value: 0.1}}}},
	}
	for _, c := range cases {
		mq, err := ParseMetrics(c.q)
		if err != nil {
			t.Errorf("%s: %v", c.q, err)
			continue
		}
		if mq.Math != nil {
			t.Errorf("%s: unexpected math", c.q)
			continue
		}
		if mq.Func != c.w.fn {
			t.Errorf("%s: func %q want %q", c.q, mq.Func, c.w.fn)
		}
		if c.w.attr != "" && (!mq.HasAttr || mq.Attr.String() != c.w.attr) {
			t.Errorf("%s: attr %q want %q", c.q, mq.Attr.String(), c.w.attr)
		}
		if len(mq.By) != len(c.w.by) {
			t.Errorf("%s: by %v want %v", c.q, mq.By, c.w.by)
		} else {
			for i := range mq.By {
				if mq.By[i].String() != c.w.by[i] {
					t.Errorf("%s: by[%d] %q want %q", c.q, i, mq.By[i].String(), c.w.by[i])
				}
			}
		}
		if len(mq.Quantiles) != len(c.w.quantiles) {
			t.Errorf("%s: quantiles %v want %v", c.q, mq.Quantiles, c.w.quantiles)
		} else {
			for i := range mq.Quantiles {
				if mq.Quantiles[i] != c.w.quantiles[i] {
					t.Errorf("%s: quantiles %v want %v", c.q, mq.Quantiles, c.w.quantiles)
				}
			}
		}
		if mq.Sample() != c.w.sample {
			t.Errorf("%s: sample %v want %v", c.q, mq.Sample(), c.w.sample)
		}
		if c.w.topN != 0 && (mq.Compare == nil || mq.Compare.TopN != c.w.topN) {
			t.Errorf("%s: compare %+v", c.q, mq.Compare)
		}
		if len(mq.SecondStage) != len(c.w.second) {
			t.Errorf("%s: second stage %+v want %+v", c.q, mq.SecondStage, c.w.second)
		} else {
			for i := range mq.SecondStage {
				g, w := mq.SecondStage[i], c.w.second[i]
				if g.Op != w.Op || g.N != w.N || (g.Value-w.Value) > 1e-9 || (w.Value-g.Value) > 1e-9 {
					t.Errorf("%s: second[%d] %+v want %+v", c.q, i, g, w)
				}
			}
		}
		if got := tr.Filter(mq.Filter).Where; got != c.w.where {
			t.Errorf("%s: where\n  got  %q\n  want %q", c.q, got, c.w.where)
		}
	}
}

func TestParseMetricsCompareArgs(t *testing.T) {
	mq, err := ParseMetrics(`{nestedSetParent<0 && true} | compare({duration >= 1.2s && duration <= 3s}, 10, 1700000000000000000, 1700003600000000000)`)
	if err != nil {
		t.Fatal(err)
	}
	if mq.Compare.StartNs != 1700000000000000000 || mq.Compare.EndNs != 1700003600000000000 {
		t.Errorf("compare window: %+v", mq.Compare)
	}
	tr := newTestTranslator(t, true)
	sel := tr.Filter(mq.Compare.Selection)
	if sel.Where != `(duration >= 1200ms) and (duration <= 3s)` || !sel.Exact {
		t.Errorf("selection = %q exact=%v", sel.Where, sel.Exact)
	}
}

func TestParseMetricsMath(t *testing.T) {
	mq, err := ParseMetrics(`({status=error} | rate()) / ({} | rate())`)
	if err != nil {
		t.Fatal(err)
	}
	if mq.Math == nil || mq.Math.Op != "/" || mq.Math.LHS == nil || mq.Math.RHS == nil {
		t.Fatalf("math = %+v", mq.Math)
	}
	if mq.Math.LHS.Func != FuncRate || mq.Math.RHS.Func != FuncRate {
		t.Errorf("funcs: %s %s", mq.Math.LHS.Func, mq.Math.RHS.Func)
	}
	mq, err = ParseMetrics(`({} | rate()) * 60`)
	if err != nil {
		t.Fatal(err)
	}
	if mq.Math == nil || mq.Math.RHSNum == nil || *mq.Math.RHSNum != 60 {
		t.Errorf("math scalar = %+v", mq.Math)
	}
}

func TestSplitTopLevel(t *testing.T) {
	parts := splitTopLevel(`{ a = "x|y" || b = 1 } | rate() | topk(1)`, '|')
	if len(parts) != 3 {
		t.Errorf("parts = %q", parts)
	}
}
