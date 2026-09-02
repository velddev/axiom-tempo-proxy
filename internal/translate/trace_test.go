package translate

import (
	"strings"
	"testing"
)

func TestSplitTrace(t *testing.T) {
	cases := []struct {
		q     string
		trace string
		span  string
		uses  TraceIntrinsics
		mixed bool
	}{
		{q: `{ span.http.method = "GET" }`, span: "{span.http.method = `GET`}"},
		{q: `{ traceDuration > 2s }`, trace: `{traceDuration > 2s}`, uses: TraceIntrinsics{Duration: true}},
		{q: `{ trace:duration > 2s }`, trace: `{traceDuration > 2s}`, uses: TraceIntrinsics{Duration: true}},
		{q: `{ rootName = "GET /x" }`, trace: "{rootName = `GET /x`}", uses: TraceIntrinsics{RootName: true}},
		{q: `{ trace:rootName = "GET /x" }`, trace: "{rootName = `GET /x`}", uses: TraceIntrinsics{RootName: true}},
		{q: `{ trace:rootService = "web" }`, trace: "{rootServiceName = `web`}", uses: TraceIntrinsics{RootService: true}},
		{
			q:     `{ rootServiceName = "web" && status = error }`,
			trace: "{rootServiceName = `web`}",
			span:  `{status = error}`,
			uses:  TraceIntrinsics{RootService: true},
		},
		{
			q:     `{ traceDuration > 2s && rootName = "x" && span.a = 1 && name = "y" }`,
			trace: "{(traceDuration > 2s) && (rootName = `x`)}",
			span:  "{(span.a = 1) && (name = `y`)}",
			uses:  TraceIntrinsics{Duration: true, RootName: true},
		},
		// A disjunction of trace-level terms stays on the trace side.
		{
			q:     `{ (traceDuration > 2s || rootName = "x") && span.a = 1 }`,
			trace: "{(traceDuration > 2s) || (rootName = `x`)}",
			span:  `{span.a = 1}`,
			uses:  TraceIntrinsics{Duration: true, RootName: true},
		},
		// Mixing the two sides inside one conjunct cannot be split.
		{q: `{ traceDuration > 2s || status = error }`, mixed: true},
		{q: `{ span.a = 1 && (traceDuration > 2s || span.b = 2) }`, mixed: true},
	}
	for _, c := range cases {
		expr, err := ParseFilter(c.q)
		if err != nil {
			t.Errorf("%s: %v", c.q, err)
			continue
		}
		got := SplitTrace(expr)
		if got.Mixed != c.mixed {
			t.Errorf("%s: mixed = %v, want %v", c.q, got.Mixed, c.mixed)
			continue
		}
		if c.mixed {
			continue
		}
		if s := exprString(got.Trace); s != c.trace {
			t.Errorf("%s: trace = %q, want %q", c.q, s, c.trace)
		}
		if s := exprString(got.Span); s != c.span {
			t.Errorf("%s: span = %q, want %q", c.q, s, c.span)
		}
		if got.Uses != c.uses {
			t.Errorf("%s: uses = %+v, want %+v", c.q, got.Uses, c.uses)
		}
	}
}

func exprString(e interface{ String() string }) string {
	if e == nil {
		return ""
	}
	return "{" + strings.TrimSpace(e.String()) + "}"
}

func TestTraceFilter(t *testing.T) {
	tr := newTestTranslator(t, true)
	cases := map[string]string{
		`{ traceDuration > 2s }`:                        `_td > 2s`,
		`{ trace:duration <= 1500ms }`:                  `_td <= 1500ms`,
		`{ rootName = "GET /x" }`:                       `_rn == "GET /x"`,
		`{ trace:rootName =~ "GET.*" }`:                 `_rn matches regex @"^(?:GET.*)$"`,
		`{ rootServiceName = "web" }`:                   `_rs == "web"`,
		`{ trace:rootService != "web" }`:                `_rs != "web"`,
		`{ rootName != nil }`:                           `isnotnull(_rn)`,
		`{ traceDuration > 2s && rootName = "x" }`:      `(_td > 2s) and (_rn == "x")`,
		`{ traceDuration > 2s || rootName = "x" }`:      `(_td > 2s) or (_rn == "x")`,
		`{ traceDuration > 1000000000 }`:                `_td > totimespan(1000000000)`,
		`{ !(rootServiceName = "web") }`:                `not(_rs == "web")`,
		`{ rootServiceName = "web" && rootName = "x" }`: `(_rs == "web") and (_rn == "x")`,
	}
	for q, want := range cases {
		expr, err := ParseFilter(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		f := tr.TraceFilter(expr)
		if f.Where != want || !f.Exact {
			t.Errorf("%s:\n  got  %q exact=%v\n  want %q", q, f.Where, f.Exact, want)
		}
	}
}

// Trace-level intrinsics stay unsupported in the ordinary per-span mode,
// so the search prefilter keeps relaxing them and Tempo's engine decides.
func TestFilterStillRelaxesTraceIntrinsics(t *testing.T) {
	tr := newTestTranslator(t, true)
	for _, q := range []string{`{ traceDuration > 1s }`, `{ rootName = "x" }`, `{ trace:rootService = "y" }`} {
		expr, _ := ParseFilter(q)
		if f := tr.Filter(expr); f.Exact || f.Where != "" {
			t.Errorf("%s: got %+v, want relaxed", q, f)
		}
	}
}

func TestTraceAggregates(t *testing.T) {
	tr := newTestTranslator(t, true)
	extend, aggs, ok := tr.TraceAggregates(TraceIntrinsics{Duration: true})
	if !ok || len(extend) != 0 || len(aggs) != 1 ||
		aggs[0] != `_td = todatetime(max((_time + duration))) - todatetime(min(_time))` {
		t.Errorf("duration: extend=%v aggs=%v ok=%v", extend, aggs, ok)
	}

	extend, aggs, ok = tr.TraceAggregates(TraceIntrinsics{RootName: true, RootService: true})
	wantExtend := []string{
		`_rk = iff(isempty(parent_span_id), datetime(1970-01-01T00:00:00Z), _time)`,
		`_rn = name`,
		`_rs = ['service.name']`,
	}
	if !ok || !equalStrings(extend, wantExtend) || len(aggs) != 1 || aggs[0] != `_r = arg_min(_rk, _rn, _rs)` {
		t.Errorf("root: extend=%v aggs=%v ok=%v", extend, aggs, ok)
	}

	// Only the requested values are computed.
	_, aggs, ok = tr.TraceAggregates(TraceIntrinsics{RootName: true})
	if !ok || aggs[0] != `_r = arg_min(_rk, _rn)` {
		t.Errorf("root name only: aggs=%v ok=%v", aggs, ok)
	}
	if _, _, ok := tr.TraceAggregates(TraceIntrinsics{}); ok {
		t.Error("no intrinsics should not produce aggregates")
	}
}

func TestRootSpanFilter(t *testing.T) {
	tr := newTestTranslator(t, true)
	expr, _ := ParseFilter(`{ rootServiceName = "web" }`)
	got, ok := tr.RootSpanFilter(expr)
	want := `(isempty(parent_span_id)) and (['service.name'] == "web")`
	if !ok || got != want {
		t.Errorf("got %q ok=%v, want %q", got, ok, want)
	}
	// traceDuration has no per-span form.
	expr, _ = ParseFilter(`{ traceDuration > 1s }`)
	if got, ok := tr.RootSpanFilter(expr); ok {
		t.Errorf("traceDuration should have no root-span form, got %q", got)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
