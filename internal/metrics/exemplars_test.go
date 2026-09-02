package metrics

import (
	"testing"

	"github.com/velddev/axiom-tempo-proxy/internal/schema"
	"github.com/velddev/axiom-tempo-proxy/internal/translate"
)

func testEvaluator(t *testing.T, opts Options) *Evaluator {
	t.Helper()
	m := schema.New(schema.Config{}, nil)
	return New(nil, translate.New(m), opts)
}

func TestExemplarCount(t *testing.T) {
	e := testEvaluator(t, Options{DefaultExemplars: 100, MaxExemplars: 250})
	cases := []struct {
		query     string
		requested int
		want      int
	}{
		// Absent parameter falls back to the configured default, which is
		// what Grafana and Drilldown get since they never send one.
		{`{} | rate()`, 0, 100},
		{`{} | rate()`, 7, 7},
		// Tempo caps at max_exemplars.
		{`{} | rate()`, 5000, 250},
		// An integer hint overrides the parameter.
		{`{} | rate() with(exemplars=3)`, 50, 3},
		{`{} | rate() with(exemplars=3)`, 0, 3},
		// with(exemplars=false) disables them; with(exemplars=true) is a
		// no-op in Tempo and leaves the default in place.
		{`{} | rate() with(exemplars=false)`, 50, 0},
		{`{} | rate() with(exemplars=true)`, 0, 100},
		{`{} | rate() with(sample=true)`, 0, 100},
	}
	for _, c := range cases {
		mq, err := translate.ParseMetrics(c.query)
		if err != nil {
			t.Fatalf("%s: %v", c.query, err)
		}
		if got := e.exemplarCount(mq, c.requested); got != c.want {
			t.Errorf("%s (requested %d) = %d, want %d", c.query, c.requested, got, c.want)
		}
	}

	// A zero default keeps exemplars off unless a request asks.
	off := testEvaluator(t, Options{DefaultExemplars: 0, MaxExemplars: 250})
	mq, _ := translate.ParseMetrics(`{} | rate()`)
	if got := off.exemplarCount(mq, 0); got != 0 {
		t.Errorf("zero default = %d", got)
	}
}

func TestExemplarAgg(t *testing.T) {
	e := testEvaluator(t, Options{})
	cases := []struct {
		fn        translate.MetricsFunc
		valExpr   string
		want      string
		spanValue bool
	}{
		// Counting functions have no per-span value, so the slowest span
		// in the bucket is picked and the exemplar takes the bucket value.
		{translate.FuncRate, "", "_exv = arg_max(coalesce(toreal(duration / 1s), 0.0), _exid, _exts)", false},
		{translate.FuncCountOverTime, "", "_exv = arg_max(coalesce(toreal(duration / 1s), 0.0), _exid, _exts)", false},
		{translate.FuncHistogramOverTime, "toreal(duration / 1s)", "_exv = arg_max(coalesce(toreal(duration / 1s), 0.0), _exid, _exts)", false},
		// min_over_time reports the smallest value, so its exemplar is the
		// span that produced it.
		{translate.FuncMinOverTime, "toreal(duration / 1s)", "_exv = arg_min(toreal(duration / 1s), _exid, _exts)", true},
		{translate.FuncMaxOverTime, "toreal(duration / 1s)", "_exv = arg_max(toreal(duration / 1s), _exid, _exts)", true},
		{translate.FuncAvgOverTime, "toreal(['attributes.http.status_code'])", "_exv = arg_max(toreal(['attributes.http.status_code']), _exid, _exts)", true},
		{translate.FuncQuantileOverTime, "toreal(duration / 1s)", "_exv = arg_max(toreal(duration / 1s), _exid, _exts)", true},
	}
	for _, c := range cases {
		got := e.exemplarAgg(c.fn, c.valExpr, 5)
		if !got.ok {
			t.Fatalf("%s: not ok", c.fn)
		}
		if got.agg != c.want {
			t.Errorf("%s agg = %q, want %q", c.fn, got.agg, c.want)
		}
		if got.spanValue != c.spanValue {
			t.Errorf("%s spanValue = %v", c.fn, got.spanValue)
		}
		if len(got.extends) != 2 || got.extends[0] != "_exid = trace_id" || got.extends[1] != "_exts = _time" {
			t.Errorf("%s extends = %v", c.fn, got.extends)
		}
	}

	// Asking for none adds nothing to the query.
	if e.exemplarAgg(translate.FuncRate, "", 0).ok {
		t.Error("exemplars must not be aggregated when none are wanted")
	}
}

func TestTraceIDHex(t *testing.T) {
	cases := map[string]string{
		"0af7651916cd43dd8448eb211c80319c": "0af7651916cd43dd8448eb211c80319c",
		"0AF7651916CD43DD8448EB211C80319C": "0af7651916cd43dd8448eb211c80319c",
		// Tempo pads short ids to 32 characters.
		"8448eb211c80319c":                   "00000000000000008448eb211c80319c",
		"  8448eb211c80319c  ":               "00000000000000008448eb211c80319c",
		"":                                   "",
		"not-hex":                            "",
		"0af7651916cd43dd8448eb211c80319cff": "",
	}
	for in, want := range cases {
		got, ok := traceIDHex(in)
		if want == "" {
			if ok {
				t.Errorf("traceIDHex(%q) = %q, want rejected", in, got)
			}
			continue
		}
		if !ok || got != want {
			t.Errorf("traceIDHex(%q) = %q, %v", in, got, ok)
		}
	}
}

func TestRenderExemplarsThinsAndSorts(t *testing.T) {
	s := &series{labels: nil, samples: map[int64]float64{}}
	for i := int64(0); i < 10; i++ {
		s.addExemplar(i*1000, exemplar{traceID: "0af7651916cd43dd8448eb211c80319c", value: float64(i + 1), tsMs: i*1000 + 5})
	}
	out := renderExemplars(s, 4)
	if len(out) != 4 {
		t.Fatalf("thinned to %d", len(out))
	}
	for i := 1; i < len(out); i++ {
		if out[i].TimestampMs <= out[i-1].TimestampMs {
			t.Errorf("exemplars not oldest first: %v", out)
		}
	}
	if got := renderExemplars(s, 100); len(got) != 10 {
		t.Errorf("unthinned = %d", len(got))
	}
	if got := renderExemplars(s, 0); got != nil {
		t.Errorf("zero wanted = %v", got)
	}
}

func TestAddExemplarDropsWhatGrafanaWouldDrop(t *testing.T) {
	s := &series{samples: map[int64]float64{}}
	// Grafana's Tempo datasource drops exemplars with value 0 or a
	// non-positive timestamp, so they are never emitted.
	s.addExemplar(1, exemplar{traceID: "0af7651916cd43dd8448eb211c80319c", value: 0, tsMs: 10})
	s.addExemplar(2, exemplar{traceID: "0af7651916cd43dd8448eb211c80319c", value: 1, tsMs: 0})
	s.addExemplar(3, exemplar{traceID: "", value: 1, tsMs: 10})
	if len(s.exemplars) != 0 {
		t.Errorf("exemplars = %v", s.exemplars)
	}
	s.addExemplar(4, exemplar{traceID: "0af7651916cd43dd8448eb211c80319c", value: 1, tsMs: 10})
	if len(s.exemplars) != 1 {
		t.Errorf("exemplars = %v", s.exemplars)
	}
}
