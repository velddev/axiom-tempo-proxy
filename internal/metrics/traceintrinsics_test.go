package metrics

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/velddev/axiom-tempo-proxy/internal/axiom"
	"github.com/velddev/axiom-tempo-proxy/internal/schema"
	"github.com/velddev/axiom-tempo-proxy/internal/translate"
)

// --- fake Axiom ---

type fakeTable struct {
	fields   []axiom.Field
	rows     [][]any
	estimate bool
}

type fakeAxiom struct {
	t       *testing.T
	mu      sync.Mutex
	queries []string
	respond func(apl string) fakeTable
	srv     *httptest.Server
}

func newFake(t *testing.T, respond func(string) fakeTable) *fakeAxiom {
	f := &fakeAxiom{t: t, respond: respond}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/datasets/_apl", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			APL string `json:"apl"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		f.mu.Lock()
		f.queries = append(f.queries, body.APL)
		f.mu.Unlock()
		tbl := f.respond(body.APL)
		cols := make([][]any, len(tbl.fields))
		for i := range cols {
			cols[i] = []any{}
		}
		for _, row := range tbl.rows {
			for i := range tbl.fields {
				cols[i] = append(cols[i], row[i])
			}
		}
		out := map[string]any{
			"format": "tabular",
			"tables": []map[string]any{{"name": "0", "fields": tbl.fields, "columns": cols}},
			"status": map[string]any{"isEstimate": tbl.estimate},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeAxiom) recorded() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.queries...)
}

func (f *fakeAxiom) find(substr string) string {
	for _, q := range f.recorded() {
		if strings.Contains(q, substr) {
			return q
		}
	}
	return ""
}

func fields(pairs ...string) []axiom.Field {
	out := make([]axiom.Field, 0, len(pairs)/2)
	for i := 0; i+1 < len(pairs); i += 2 {
		out = append(out, axiom.Field{Name: pairs[i], Type: pairs[i+1]})
	}
	return out
}

func testMapping() *schema.Mapping {
	mk := func(name, typ string) axiom.DatasetField { return axiom.DatasetField{Name: name, Type: typ} }
	return schema.New(schema.DefaultConfig(), []axiom.DatasetField{
		mk("_time", "datetime"), mk("trace_id", "string"), mk("span_id", "string"),
		mk("parent_span_id", "string"), mk("name", "string"), mk("kind", "string"),
		mk("duration", "timespan"), mk("status.code", "string"), mk("error", "bool"),
		mk("service.name", "string"), mk("attributes.http.method", "string"),
		mk("attributes.custom", "map"),
	})
}

func newEvaluator(t *testing.T, f *fakeAxiom) *Evaluator {
	t.Helper()
	client, err := axiom.New(axiom.Config{BaseURL: f.srv.URL, Token: "test"})
	if err != nil {
		t.Fatal(err)
	}
	return New(client, translate.New(testMapping()), Options{
		Dataset: "otel",
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
}

var base = time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)

func request(q string, minutes int) Request {
	return Request{
		Query:   q,
		StartNs: uint64(base.UnixNano()),
		EndNs:   uint64(base.Add(time.Duration(minutes) * time.Minute).UnixNano()),
		StepNs:  uint64(time.Minute),
	}
}

// idList answers a trace-id query with the given ids.
func idList(ids ...string) fakeTable {
	list := make([]any, 0, len(ids))
	for _, id := range ids {
		list = append(list, id)
	}
	return fakeTable{fields: fields("_n", "integer", "_ids", "array"), rows: [][]any{{len(ids), list}}}
}

// --- tests ---

// { traceDuration > 2s } | rate() has no span-level filter, so the
// per-trace aggregation runs over every trace in the window.
func TestTraceDurationRate(t *testing.T) {
	f := newFake(t, func(apl string) fakeTable {
		switch {
		case strings.Contains(apl, "_td = todatetime"):
			return idList("aaaa", "bbbb")
		case strings.Contains(apl, "summarize v = count()"):
			return fakeTable{
				fields: fields("_bucket", "datetime", "v", "integer"),
				rows:   [][]any{{base.Format(time.RFC3339Nano), 120}},
			}
		}
		return fakeTable{}
	})
	e := newEvaluator(t, f)
	res, err := e.QueryRange(context.Background(), request(`{ traceDuration > 2s } | rate()`, 2))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Series) != 1 || len(res.Series[0].Samples) != 2 {
		t.Fatalf("series = %+v", res.Series)
	}
	if res.Series[0].Samples[0].Value != 2 || res.Series[0].Samples[1].Value != 0 {
		t.Errorf("samples = %v", res.Series[0].Samples)
	}

	traceQ := f.find("_td = todatetime")
	want := "['otel']\n" +
		"| where isnotempty(trace_id)\n" +
		"| summarize _td = todatetime(max((_time + duration))) - todatetime(min(_time)) by trace_id\n" +
		"| where _td > 2s\n" +
		"| summarize _n = count(), _ids = make_list(trace_id, 5000)"
	if traceQ != want {
		t.Errorf("trace query:\n%s\nwant:\n%s", traceQ, want)
	}
	// Only two queries: no span filter to narrow the aggregation with.
	if n := len(f.recorded()); n != 2 {
		t.Errorf("queries = %d: %v", n, f.recorded())
	}
	metricQ := f.find("summarize v = count()")
	if !strings.Contains(metricQ, `| where trace_id in ("aaaa", "bbbb")`) {
		t.Errorf("metrics query:\n%s", metricQ)
	}
}

// A span-level conjunct narrows the traces the aggregation looks at: only
// traces holding a matching span can contribute to the metric.
func TestRootServiceAndStatusRateBy(t *testing.T) {
	f := newFake(t, func(apl string) fakeTable {
		switch {
		case strings.Contains(apl, "_rs = ['service.name']"):
			return idList("aaaa")
		case strings.Contains(apl, `summarize by trace_id`):
			return idList("aaaa", "cccc")
		case strings.Contains(apl, "summarize v = count()"):
			return fakeTable{
				fields: fields("_bucket", "datetime", "g0", "string", "v", "integer"),
				rows:   [][]any{{base.Format(time.RFC3339Nano), "POST /checkout", 60}},
			}
		}
		return fakeTable{}
	})
	e := newEvaluator(t, f)
	res, err := e.QueryRange(context.Background(), request(`{ rootServiceName = "web" && status = error } | rate() by (name)`, 1))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Series) != 1 || res.Series[0].Samples[0].Value != 1 {
		t.Fatalf("series = %+v", res.Series)
	}

	errWhere := `(['status.code'] =~ "error") or (['status.code'] =~ "STATUS_CODE_ERROR") or (error == true)`
	candQ := f.find("summarize by trace_id")
	wantCand := "['otel']\n| where isnotempty(trace_id)\n| where " + errWhere +
		"\n| summarize by trace_id\n| summarize _n = count(), _ids = make_list(trace_id, 5000)"
	if candQ != wantCand {
		t.Errorf("candidate query:\n%s\nwant:\n%s", candQ, wantCand)
	}

	traceQ := f.find("_rs = ['service.name']")
	wantTrace := "['otel']\n" +
		"| where isnotempty(trace_id)\n" +
		`| where trace_id in ("aaaa", "cccc")` + "\n" +
		"| extend _rk = iff(isempty(parent_span_id), datetime(1970-01-01T00:00:00Z), _time), _rs = ['service.name']\n" +
		"| summarize _r = arg_min(_rk, _rs) by trace_id\n" +
		`| where _rs == "web"` + "\n" +
		"| summarize _n = count(), _ids = make_list(trace_id, 5000)"
	if traceQ != wantTrace {
		t.Errorf("trace query:\n%s\nwant:\n%s", traceQ, wantTrace)
	}

	metricQ := f.find("summarize v = count()")
	if !strings.Contains(metricQ, "| where ("+errWhere+`) and (trace_id in ("aaaa"))`) {
		t.Errorf("metrics query:\n%s", metricQ)
	}
}

// A root-only filter with no span-level part is narrowed by looking at
// root spans directly, which keeps the aggregation cheap.
func TestRootNameQuantile(t *testing.T) {
	f := newFake(t, func(apl string) fakeTable {
		switch {
		case strings.Contains(apl, "isempty(parent_span_id)) and (name =="):
			return idList("aaaa", "bbbb")
		case strings.Contains(apl, "_rn = name"):
			return idList("aaaa")
		case strings.Contains(apl, "percentile("):
			return fakeTable{
				fields: fields("_bucket", "datetime", "q0", "float"),
				rows:   [][]any{{base.Format(time.RFC3339Nano), 0.42}},
			}
		}
		return fakeTable{}
	})
	e := newEvaluator(t, f)
	res, err := e.QueryRange(context.Background(), request(`{ trace:rootName = "GET /x" } | quantile_over_time(duration, 0.9)`, 1))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Series) != 1 || res.Series[0].Samples[0].Value != 0.42 {
		t.Fatalf("series = %+v", res.Series)
	}

	candQ := f.find("isempty(parent_span_id)) and (name ==")
	wantCand := "['otel']\n| where isnotempty(trace_id)\n" +
		`| where (isempty(parent_span_id)) and (name == "GET /x")` + "\n" +
		"| summarize by trace_id\n| summarize _n = count(), _ids = make_list(trace_id, 5000)"
	if candQ != wantCand {
		t.Errorf("candidate query:\n%s\nwant:\n%s", candQ, wantCand)
	}
	metricQ := f.find("percentile(")
	if !strings.Contains(metricQ, `| where trace_id in ("aaaa")`) {
		t.Errorf("metrics query:\n%s", metricQ)
	}
}

// A trace filter matching nothing yields an empty (zero-filled) result
// without running a second query against every span.
func TestNoQualifyingTraces(t *testing.T) {
	f := newFake(t, func(apl string) fakeTable { return idList() })
	e := newEvaluator(t, f)
	res, err := e.QueryRange(context.Background(), request(`{ traceDuration > 2s } | rate()`, 1))
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if len(res.Series) != 1 || res.Series[0].Samples[0].Value != 0 {
		t.Fatalf("series = %+v", res.Series)
	}
	if q := f.find("summarize v = count()"); !strings.Contains(q, "| where false") {
		t.Errorf("metrics query:\n%s", q)
	}
}

// Truncated per-trace aggregation must never be reported as a result.
func TestTruncatedAggregationIs400(t *testing.T) {
	for _, tc := range []struct {
		name  string
		table fakeTable
		want  string
	}{
		{"estimate", fakeTable{fields: fields("_n", "integer", "_ids", "array"), rows: [][]any{{5000, []any{}}}, estimate: true}, "too many traces"},
		{"over the cap", fakeTable{fields: fields("_n", "integer", "_ids", "array"), rows: [][]any{{5001, []any{}}}}, "more than the limit"},
	} {
		f := newFake(t, func(string) fakeTable { return tc.table })
		e := newEvaluator(t, f)
		_, err := e.QueryRange(context.Background(), request(`{ traceDuration > 2s } | rate()`, 1))
		var unsupported *UnsupportedError
		if err == nil || !asUnsupported(err, &unsupported) || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%s: err = %v", tc.name, err)
		}
	}
}

// The cap is configurable and reaches both make_list() and the check.
func TestConfiguredTraceIDCap(t *testing.T) {
	f := newFake(t, func(string) fakeTable {
		return fakeTable{fields: fields("_n", "integer", "_ids", "array"), rows: [][]any{{3, []any{}}}}
	})
	client, err := axiom.New(axiom.Config{BaseURL: f.srv.URL, Token: "test"})
	if err != nil {
		t.Fatal(err)
	}
	e := New(client, translate.New(testMapping()), Options{
		Dataset:                 "otel",
		MaxTraceIntrinsicTraces: 2,
		Log:                     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	_, err = e.QueryRange(context.Background(), request(`{ traceDuration > 2s } | rate()`, 1))
	if err == nil || !strings.Contains(err.Error(), "more than the limit of 2") {
		t.Fatalf("err = %v", err)
	}
	if q := f.find("make_list"); !strings.Contains(q, "make_list(trace_id, 2)") {
		t.Errorf("trace query:\n%s", q)
	}
}

// A trace-level term that cannot be lifted out of a disjunction is
// refused rather than silently dropped.
func TestMixedFilterIs400(t *testing.T) {
	f := newFake(t, func(string) fakeTable { return fakeTable{} })
	e := newEvaluator(t, f)
	_, err := e.QueryRange(context.Background(), request(`{ traceDuration > 2s || status = error } | rate()`, 1))
	if err == nil || !strings.Contains(err.Error(), "can only be combined with span-level conditions using &&") {
		t.Fatalf("err = %v", err)
	}
	if n := len(f.recorded()); n != 0 {
		t.Errorf("should not query Axiom: %v", f.recorded())
	}
}

// Intrinsics that need the span tree stay unsupported.
func TestStructuralIntrinsicsStillUnsupported(t *testing.T) {
	f := newFake(t, func(string) fakeTable { return fakeTable{} })
	e := newEvaluator(t, f)
	for _, q := range []string{
		`{ span:childCount > 2 } | rate()`,
		`{ nestedSetLeft > 3 } | rate()`,
		`{ nestedSetRight < 9 } | rate()`,
		`{ parent.span.a = 1 } | rate()`,
		`{ link:traceID = "abc" } | rate()`,
	} {
		_, err := e.QueryRange(context.Background(), request(q, 1))
		if err == nil || !strings.Contains(err.Error(), "cannot be evaluated in APL") {
			t.Errorf("%s: err = %v", q, err)
		}
	}
}

func asUnsupported(err error, target **UnsupportedError) bool {
	u, ok := err.(*UnsupportedError)
	if ok {
		*target = u
	}
	return ok
}
