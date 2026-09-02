package server

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/util"

	"github.com/velddev/axiom-tempo-proxy/internal/axiom"
	"github.com/velddev/axiom-tempo-proxy/internal/config"
)

// --- fake Axiom ---

type fakeAxiom struct {
	t       *testing.T
	mu      sync.Mutex
	queries []string
	respond func(apl string) (fields []axiom.Field, rows [][]any)
	srv     *httptest.Server
	// fieldsForbidden simulates a query-only token.
	fieldsForbidden bool
	// queryError, when set, lets a test fail specific queries.
	queryError func(apl string) (int, string)
}

func newFakeAxiom(t *testing.T, respond func(apl string) ([]axiom.Field, [][]any)) *fakeAxiom {
	f := &fakeAxiom{t: t, respond: respond}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v2/datasets/otel/fields", func(w http.ResponseWriter, r *http.Request) {
		if f.fieldsForbidden {
			http.Error(w, `{"message":"token does not have access to resource: datasets with action: read"}`, http.StatusForbidden)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(datasetFields())
	})
	mux.HandleFunc("POST /v1/datasets/_apl", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-token" {
			http.Error(w, `{"message":"forbidden"}`, http.StatusForbidden)
			return
		}
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
		if f.queryError != nil {
			if status, msg := f.queryError(body.APL); status != 0 {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_ = json.NewEncoder(w).Encode(map[string]string{"message": msg})
				return
			}
		}
		fields, rows := f.respond(body.APL)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(tabular(fields, rows))
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

func datasetFields() []axiom.DatasetField {
	mk := func(name, typ string) axiom.DatasetField { return axiom.DatasetField{Name: name, Type: typ} }
	return []axiom.DatasetField{
		mk("_time", "datetime"), mk("trace_id", "string"), mk("span_id", "string"),
		mk("parent_span_id", "string"), mk("name", "string"), mk("kind", "string"),
		mk("duration", "timespan"), mk("status.code", "string"), mk("status.message", "string"),
		mk("error", "bool"), mk("service.name", "string"), mk("scope.name", "string"),
		mk("attributes.http.method", "string"), mk("attributes.http.status_code", "integer"),
		mk("attributes.custom", "map"), mk("resource.custom", "map"),
		mk("resource.k8s.pod.name", "string"), mk("events", "array"),
	}
}

// tabular encodes column-major tabular JSON.
func tabular(fields []axiom.Field, rows [][]any) []byte {
	cols := make([][]any, len(fields))
	for i := range cols {
		cols[i] = []any{}
	}
	for _, row := range rows {
		for i := range fields {
			cols[i] = append(cols[i], row[i])
		}
	}
	res := map[string]any{
		"format":       "tabular",
		"datasetNames": []string{"otel"},
		"tables": []map[string]any{{
			"name":    "0",
			"fields":  fields,
			"columns": cols,
		}},
		"status": map[string]any{"rowsMatched": len(rows)},
	}
	b, _ := json.Marshal(res)
	return b
}

func fieldsOf(names ...string) []axiom.Field {
	out := make([]axiom.Field, 0, len(names)/2)
	for i := 0; i+1 < len(names); i += 2 {
		out = append(out, axiom.Field{Name: names[i], Type: names[i+1]})
	}
	return out
}

// --- test data: two traces ---

const (
	traceA = "0af7651916cd43dd8448eb211c80319c"
	traceB = "1af7651916cd43dd8448eb211c80319d"
)

var t0 = time.Date(2026, 9, 2, 12, 0, 0, 0, time.UTC)

// spanRows returns the raw span rows of the dataset in the full span
// column layout.
func spanFields() []axiom.Field {
	return fieldsOf(
		"_time", "datetime", "trace_id", "string", "span_id", "string", "parent_span_id", "string",
		"name", "string", "kind", "string", "duration", "timespan", "status.code", "string",
		"status.message", "string", "error", "bool", "service.name", "string", "scope.name", "string",
		"attributes.http.method", "string", "attributes.http.status_code", "integer",
		"attributes.custom", "map", "resource.custom", "map", "resource.k8s.pod.name", "string",
		"events", "array",
	)
}

func spanRows(traceIDs ...string) [][]any {
	want := map[string]bool{}
	for _, id := range traceIDs {
		want[id] = true
	}
	all := [][]any{
		// trace A: root server span in frontend, client child, backend server child with error
		{t0.Format(time.RFC3339Nano), traceA, "a000000000000001", nil, "GET /cart", "server", "120ms", "OK", "", false, "frontend", "http",
			"GET", 200, map[string]any{"app.user.id": "u-1", "cart.items": 3}, map[string]any{"deployment.environment": "prod"}, "frontend-abc", nil},
		{t0.Add(10 * time.Millisecond).Format(time.RFC3339Nano), traceA, "a000000000000002", "a000000000000001", "call backend", "client", "100ms", "OK", "", false, "frontend", "http",
			"POST", 500, map[string]any{"peer.service": "backend"}, map[string]any{"deployment.environment": "prod"}, "frontend-abc", nil},
		{t0.Add(20 * time.Millisecond).Format(time.RFC3339Nano), traceA, "a000000000000003", "a000000000000002", "POST /checkout", "server", "80ms", "error", "boom", true, "backend", "http",
			"POST", 500, map[string]any{"db.system.name": "postgres"}, map[string]any{"deployment.environment": "prod"}, "backend-xyz",
			[]map[string]any{{"name": "exception", "timestamp": t0.Add(50 * time.Millisecond).Format(time.RFC3339Nano), "attributes": map[string]any{"exception.message": "boom", "exception.type": "DbError"}}}},
		// trace B: single healthy root span
		{t0.Add(-time.Minute).Format(time.RFC3339Nano), traceB, "b000000000000001", nil, "GET /health", "server", "5ms", "OK", "", false, "frontend", "http",
			"GET", 200, map[string]any{}, map[string]any{"deployment.environment": "prod"}, "frontend-abc", nil},
	}
	var out [][]any
	for _, row := range all {
		if len(want) == 0 || want[row[1].(string)] {
			out = append(out, row)
		}
	}
	return out
}

// defaultRespond answers the query shapes the proxy generates.
func defaultRespond(apl string) ([]axiom.Field, [][]any) {
	switch {
	case strings.Contains(apl, "bag_keys(['attributes.custom'])"):
		return fieldsOf("k", "array"), [][]any{{[]string{"app.user.id", "cart.items"}}, {[]string{"peer.service", "db.system.name"}}}
	case strings.Contains(apl, "bag_keys(['resource.custom'])"):
		return fieldsOf("k", "array"), [][]any{{[]string{"deployment.environment"}}}
	case strings.HasSuffix(apl, "| limit 1"):
		// Schema probe: every column, one row.
		return spanFields(), spanRows(traceA)[:1]
	case strings.Contains(apl, "trace_id == "):
		for _, id := range []string{traceA, traceB} {
			if strings.Contains(apl, `trace_id == "`+id+`"`) {
				return spanFields(), spanRows(id)
			}
		}
		return spanFields(), nil
	case strings.Contains(apl, "trace_id in ("):
		var ids []string
		for _, id := range []string{traceA, traceB} {
			if strings.Contains(apl, id) {
				ids = append(ids, id)
			}
		}
		return spanFields(), spanRows(ids...)
	case strings.Contains(apl, "summarize m0"):
		// Candidate traces: return both and let the engine decide, which is
		// what happens when the prefilter is a superset.
		return fieldsOf("trace_id", "string", "start", "datetime"), [][]any{
			{traceA, t0.Format(time.RFC3339Nano)},
			{traceB, t0.Add(-time.Minute).Format(time.RFC3339Nano)},
		}
	}
	return nil, nil
}

// --- harness ---

type harness struct {
	fake *fakeAxiom
	web  *httptest.Server
}

func newHarness(t *testing.T, respond func(string) ([]axiom.Field, [][]any)) *harness {
	t.Helper()
	if respond == nil {
		respond = defaultRespond
	}
	fake := newFakeAxiom(t, respond)
	cfg := config.Default()
	cfg.AxiomURL = fake.srv.URL
	cfg.AxiomToken = "test-token"
	cfg.Dataset = "otel"
	cfg.LogQueries = true
	client, err := axiom.New(axiom.Config{BaseURL: cfg.AxiomURL, Token: cfg.AxiomToken})
	if err != nil {
		t.Fatal(err)
	}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	srv := New(cfg, client, log)
	srv.Warm(context.Background())
	web := httptest.NewServer(srv.Handler())
	t.Cleanup(web.Close)
	return &harness{fake: fake, web: web}
}

func (h *harness) get(t *testing.T, path string, accept string) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, h.web.URL+path, nil)
	if accept != "" {
		req.Header.Set("Accept", accept)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, body
}

func (h *harness) getJSON(t *testing.T, path string, out any) {
	t.Helper()
	status, body := h.get(t, path, "application/json")
	if status != http.StatusOK {
		t.Fatalf("GET %s: %d %s", path, status, body)
	}
	if msg, ok := out.(proto.Message); ok {
		if err := jsonpb.Unmarshal(bytes.NewReader(body), msg); err != nil {
			t.Fatalf("GET %s: bad jsonpb %v: %s", path, err, body)
		}
		return
	}
	if err := json.Unmarshal(body, out); err != nil {
		t.Fatalf("GET %s: bad json %v: %s", path, err, body)
	}
}

// --- tests ---

func TestEchoAndBuildInfo(t *testing.T) {
	h := newHarness(t, nil)
	status, body := h.get(t, "/api/echo", "")
	if status != 200 || string(body) != "echo" {
		t.Errorf("echo: %d %q", status, body)
	}
	var info map[string]any
	h.getJSON(t, "/api/status/buildinfo", &info)
	if info["version"] == "" {
		t.Errorf("buildinfo: %v", info)
	}
}

func TestTraceByIDProto(t *testing.T) {
	h := newHarness(t, nil)
	start := t0.Add(-time.Hour).Unix()
	end := t0.Add(time.Hour).Unix()
	status, body := h.get(t, fmt.Sprintf("/api/v2/traces/%s?start=%d&end=%d", traceA, start, end), "application/protobuf")
	if status != 200 {
		t.Fatalf("status %d: %s", status, body)
	}
	var res tempopb.TraceByIDResponse
	if err := proto.Unmarshal(body, &res); err != nil {
		t.Fatal(err)
	}
	if len(res.Trace.ResourceSpans) != 2 {
		t.Fatalf("resource spans = %d", len(res.Trace.ResourceSpans))
	}
	var names []string
	var parentOK bool
	for _, rs := range res.Trace.ResourceSpans {
		for _, ss := range rs.ScopeSpans {
			if ss.Scope == nil {
				t.Error("scope must never be nil (Grafana dereferences it)")
			}
			for _, sp := range ss.Spans {
				names = append(names, sp.Name)
				if sp.Status == nil {
					t.Errorf("span %s: status must never be nil (Grafana dereferences it)", sp.Name)
				}
				if hex.EncodeToString(sp.TraceId) != traceA {
					t.Errorf("trace id = %x", sp.TraceId)
				}
				if sp.Name == "call backend" && util.SpanIDToHexString(sp.ParentSpanId) == "a000000000000001" {
					parentOK = true
				}
				if sp.Name == "POST /checkout" {
					if sp.Status == nil || sp.Status.Code.String() != "STATUS_CODE_ERROR" {
						t.Errorf("checkout status = %v", sp.Status)
					}
					if len(sp.Events) != 1 || sp.Events[0].Name != "exception" {
						t.Errorf("checkout events = %v", sp.Events)
					}
					if sp.EndTimeUnixNano-sp.StartTimeUnixNano != 80_000_000 {
						t.Errorf("checkout duration = %d", sp.EndTimeUnixNano-sp.StartTimeUnixNano)
					}
				}
			}
		}
	}
	if len(names) != 3 || !parentOK {
		t.Errorf("names = %v parentOK=%v", names, parentOK)
	}
	if q := h.fake.find("trace_id == "); !strings.Contains(q, `trace_id == "`+traceA+`"`) {
		t.Errorf("trace query = %q", q)
	}
}

func TestTraceByIDJSONLegacy(t *testing.T) {
	h := newHarness(t, nil)
	status, body := h.get(t, "/api/traces/"+traceA, "application/json")
	if status != 200 {
		t.Fatalf("status %d: %s", status, body)
	}
	if !strings.HasPrefix(string(body), `{"batches":[`) {
		t.Errorf("legacy body should start with batches: %s", body[:60])
	}
	var res map[string]any
	if err := json.Unmarshal(body, &res); err != nil {
		t.Fatal(err)
	}
	status, _ = h.get(t, "/api/traces/"+traceB[:8]+"ffffffffffffffffffffffff", "application/json")
	if status != http.StatusNotFound {
		t.Errorf("unknown trace status = %d", status)
	}
}

func TestSearchSimpleFilter(t *testing.T) {
	h := newHarness(t, nil)
	q := url.QueryEscape(`{ status = error }`)
	var res map[string]any
	h.getJSON(t, fmt.Sprintf("/api/search?q=%s&start=%d&end=%d&limit=20&spss=3", q, t0.Add(-time.Hour).Unix(), t0.Add(time.Hour).Unix()), &res)

	traces, _ := res["traces"].([]any)
	if len(traces) != 1 {
		t.Fatalf("traces = %v", res)
	}
	tr := traces[0].(map[string]any)
	if tr["traceID"] != traceA || tr["rootServiceName"] != "frontend" || tr["rootTraceName"] != "GET /cart" {
		t.Errorf("trace meta = %v", tr)
	}
	if tr["durationMs"] != float64(120) {
		t.Errorf("durationMs = %v", tr["durationMs"])
	}
	spanSets := tr["spanSets"].([]any)
	spansOut := spanSets[0].(map[string]any)["spans"].([]any)
	if len(spansOut) != 1 || spansOut[0].(map[string]any)["spanID"] != "a000000000000003" {
		t.Errorf("spans = %v", spansOut)
	}
	attrs := spansOut[0].(map[string]any)["attributes"].([]any)
	if len(attrs) != 1 || attrs[0].(map[string]any)["key"] != "status" {
		t.Errorf("attrs should be just status: %v", attrs)
	}

	cand := h.fake.find("summarize m0")
	if !strings.Contains(cand, `countif((['status.code'] =~ "error")`) || !strings.Contains(cand, "| where m0 > 0") {
		t.Errorf("candidate query:\n%s", cand)
	}
	if pull := h.fake.find("trace_id in ("); !strings.Contains(pull, traceA) {
		t.Errorf("span pull query:\n%s", pull)
	}
}

func TestSearchStructural(t *testing.T) {
	h := newHarness(t, nil)
	q := url.QueryEscape(`{ resource.service.name = "frontend" } >> { status = error }`)
	var res map[string]any
	h.getJSON(t, fmt.Sprintf("/api/search?q=%s&start=%d&end=%d", q, t0.Add(-time.Hour).Unix(), t0.Add(time.Hour).Unix()), &res)
	traces, _ := res["traces"].([]any)
	if len(traces) != 1 {
		t.Fatalf("traces = %v", res)
	}
	spanSets := traces[0].(map[string]any)["spanSets"].([]any)
	spansOut := spanSets[0].(map[string]any)["spans"].([]any)
	if len(spansOut) != 1 || spansOut[0].(map[string]any)["spanID"] != "a000000000000003" {
		t.Errorf("descendant spans = %v", spansOut)
	}
	cand := h.fake.find("summarize m0")
	if !strings.Contains(cand, "m1 = countif") || !strings.Contains(cand, "(m0 > 0) and (m1 > 0)") {
		t.Errorf("candidate query:\n%s", cand)
	}
}

func TestSearchRootSpansSelect(t *testing.T) {
	h := newHarness(t, nil)
	q := url.QueryEscape(`{nestedSetParent<0 && true} | select(resource.service.name, span.http.method)`)
	var res map[string]any
	h.getJSON(t, fmt.Sprintf("/api/search?q=%s&start=%d&end=%d&limit=10&spss=10", q, t0.Add(-time.Hour).Unix(), t0.Add(time.Hour).Unix()), &res)
	traces, _ := res["traces"].([]any)
	if len(traces) != 2 {
		t.Fatalf("traces = %v", res)
	}
	for _, tr := range traces {
		spanSets := tr.(map[string]any)["spanSets"].([]any)
		spansOut := spanSets[0].(map[string]any)["spans"].([]any)
		if len(spansOut) != 1 {
			t.Errorf("root spans = %v", spansOut)
		}
		attrs := spansOut[0].(map[string]any)["attributes"].([]any)
		keys := map[string]bool{}
		for _, a := range attrs {
			keys[a.(map[string]any)["key"].(string)] = true
		}
		// Tempo keys attributes by bare name and only returns what the
		// query referenced (plus the root-span intrinsic used to filter).
		if !keys["service.name"] || !keys["http.method"] || !keys["nestedSetParent"] || keys["status"] || keys["k8s.pod.name"] {
			t.Errorf("selected attrs = %v", keys)
		}
	}
	cand := h.fake.find("summarize m0")
	if !strings.Contains(cand, "countif(isempty(parent_span_id))") {
		t.Errorf("candidate query:\n%s", cand)
	}
}

func TestSearchRejectsBadParams(t *testing.T) {
	h := newHarness(t, nil)
	status, _ := h.get(t, "/api/search?q=%7B%7D&tags=foo%3Dbar", "")
	if status != 400 {
		t.Errorf("q+tags status = %d", status)
	}
	status, _ = h.get(t, "/api/search?q=%7B%7D&start=10&end=5", "")
	if status != 400 {
		t.Errorf("start>end status = %d", status)
	}
	status, _ = h.get(t, "/api/search?q=%7B%7D&limit=0", "")
	if status != 400 {
		t.Errorf("limit=0 status = %d", status)
	}
	status, body := h.get(t, "/api/search?q="+url.QueryEscape("{ span.a = }"), "")
	if status != 400 {
		t.Errorf("bad query status = %d %s", status, body)
	}
}

func TestSearchLegacyTags(t *testing.T) {
	h := newHarness(t, nil)
	var res map[string]any
	h.getJSON(t, "/api/search?tags="+url.QueryEscape(`service.name=front http.method=GET`)+"&minDuration=1ms&start=1&end=2", &res)
	cand := h.fake.find("summarize m0")
	if !strings.Contains(cand, `matches regex @"^(?:(?i).*front.*)$"`) || !strings.Contains(cand, "duration >= 1ms") {
		t.Errorf("legacy candidate query:\n%s", cand)
	}
}

func TestSearchTagsV2(t *testing.T) {
	h := newHarness(t, nil)
	var res tempopb.SearchTagsV2Response
	h.getJSON(t, "/api/v2/search/tags?limit=5000", &res)
	scopes := map[string][]string{}
	for _, sc := range res.Scopes {
		scopes[sc.Name] = sc.Tags
	}
	if !contains(scopes["resource"], "service.name") || !contains(scopes["resource"], "k8s.pod.name") || !contains(scopes["resource"], "deployment.environment") {
		t.Errorf("resource tags = %v", scopes["resource"])
	}
	if !contains(scopes["span"], "http.method") || !contains(scopes["span"], "app.user.id") || contains(scopes["span"], "custom") {
		t.Errorf("span tags = %v", scopes["span"])
	}
	if !contains(scopes["intrinsic"], "status") || !contains(scopes["intrinsic"], "rootServiceName") {
		t.Errorf("intrinsic tags = %v", scopes["intrinsic"])
	}

	var v1 tempopb.SearchTagsResponse
	h.getJSON(t, "/api/search/tags", &v1)
	if !contains(v1.TagNames, "http.method") || !contains(v1.TagNames, "service.name") {
		t.Errorf("v1 tags = %v", v1.TagNames)
	}
	status, _ := h.get(t, "/api/v2/search/tags?scope=bogus", "")
	if status != 400 {
		t.Errorf("bad scope status = %d", status)
	}
}

func TestTagValuesV2(t *testing.T) {
	h := newHarness(t, func(apl string) ([]axiom.Field, [][]any) {
		if strings.Contains(apl, "summarize c = count() by v = ") && strings.Contains(apl, "['service.name']") {
			return fieldsOf("v", "string", "c", "integer"), [][]any{{"frontend", 10}, {"backend", 3}}
		}
		if strings.Contains(apl, "by v = ['status.code']") {
			return fieldsOf("v", "string", "c", "integer"), [][]any{{"OK", 10}, {"error", 3}}
		}
		if strings.Contains(apl, "by v = ['attributes.http.status_code']") {
			return fieldsOf("v", "integer", "c", "integer"), [][]any{{200, 10}, {500, 3}}
		}
		return defaultRespond(apl)
	})
	var res tempopb.SearchTagValuesV2Response
	h.getJSON(t, "/api/v2/search/tag/resource.service.name/values?q="+url.QueryEscape(`{ status = error }`)+"&limit=100", &res)
	if len(res.TagValues) != 2 || res.TagValues[0].Value != "frontend" || res.TagValues[0].Type != "string" {
		t.Errorf("values = %v", res.TagValues)
	}
	q := h.fake.find("summarize c = count() by v = ['service.name']")
	if !strings.Contains(q, `['status.code'] =~ "error"`) || !strings.Contains(q, "top 100 by c") {
		t.Errorf("tag values query:\n%s", q)
	}

	// Incomplete filter degrades to unfiltered.
	h.getJSON(t, "/api/v2/search/tag/resource.service.name/values?q="+url.QueryEscape(`{ resource.cluster = }`), &res)
	if len(res.TagValues) != 2 {
		t.Errorf("unfiltered values = %v", res.TagValues)
	}

	h.getJSON(t, "/api/v2/search/tag/status/values", &res)
	if len(res.TagValues) != 2 || res.TagValues[1].Type != "status" || res.TagValues[1].Value != "error" {
		t.Errorf("status values = %v", res.TagValues)
	}

	h.getJSON(t, "/api/v2/search/tag/span.http.status_code/values", &res)
	if len(res.TagValues) != 2 || res.TagValues[1].Type != "int" || res.TagValues[1].Value != "500" {
		t.Errorf("int values = %v", res.TagValues)
	}

	var v1 tempopb.SearchTagValuesResponse
	h.getJSON(t, "/api/search/tag/service.name/values", &v1)
	if len(v1.TagValues) != 2 {
		t.Errorf("v1 values = %v", v1.TagValues)
	}
}

func TestMetricsRateBy(t *testing.T) {
	bucket := t0.Truncate(time.Minute)
	h := newHarness(t, func(apl string) ([]axiom.Field, [][]any) {
		if strings.Contains(apl, "summarize v = count() by _bucket = bin(_time, 1m), g0 = ['service.name']") {
			return fieldsOf("_bucket", "datetime", "g0", "string", "v", "integer"), [][]any{
				{bucket.Format(time.RFC3339Nano), "frontend", 120},
				{bucket.Format(time.RFC3339Nano), "backend", 60},
				{bucket.Add(time.Minute).Format(time.RFC3339Nano), "frontend", 240},
			}
		}
		return defaultRespond(apl)
	})
	q := url.QueryEscape(`{nestedSetParent<0 && true && resource.service.name != nil} | rate() by(resource.service.name)`)
	var res tempopb.QueryRangeResponse
	start := bucket.Unix()
	end := bucket.Add(3 * time.Minute).Unix()
	h.getJSON(t, fmt.Sprintf("/api/metrics/query_range?q=%s&start=%d&end=%d&step=60", q, start, end), &res)
	if len(res.Series) != 2 {
		t.Fatalf("series = %d", len(res.Series))
	}
	byName := map[string]*tempopb.TimeSeries{}
	for _, s := range res.Series {
		byName[s.Labels[0].Value.GetStringValue()] = s
		if s.Labels[0].Key != "resource.service.name" {
			t.Errorf("label key = %s", s.Labels[0].Key)
		}
	}
	fe := byName["frontend"]
	if fe == nil || len(fe.Samples) != 3 {
		t.Fatalf("frontend samples = %v", fe)
	}
	if fe.Samples[0].TimestampMs != bucket.UnixMilli() || fe.Samples[0].Value != 2 || fe.Samples[1].Value != 4 || fe.Samples[2].Value != 0 {
		t.Errorf("frontend samples = %v", fe.Samples)
	}
	be := byName["backend"]
	if be == nil || len(be.Samples) != 3 || be.Samples[0].Value != 1 || be.Samples[1].Value != 0 {
		t.Errorf("backend samples = %v", be)
	}
	apl := h.fake.find("summarize v = count()")
	if !strings.Contains(apl, "where isempty(parent_span_id)") && !strings.Contains(apl, "(isempty(parent_span_id)) and (isnotnull(['service.name']))") {
		t.Errorf("metrics query:\n%s", apl)
	}
}

func TestMetricsQuantileAndHistogram(t *testing.T) {
	bucket := t0.Truncate(time.Minute)
	h := newHarness(t, func(apl string) ([]axiom.Field, [][]any) {
		switch {
		case strings.Contains(apl, "q0 = percentile(toreal(duration / 1s), 90.0)"):
			return fieldsOf("_bucket", "datetime", "g0", "string", "q0", "float", "q1", "float"), [][]any{
				{bucket.Format(time.RFC3339Nano), "GET", 0.25, 0.5},
			}
		case strings.Contains(apl, "_bucketv = pow(2.0, ceiling(log2("):
			return fieldsOf("_bucket", "datetime", "_bucketv", "float", "v", "integer"), [][]any{
				{bucket.Format(time.RFC3339Nano), 134217728, 7},
				{bucket.Format(time.RFC3339Nano), 268435456, 2},
			}
		}
		return defaultRespond(apl)
	})
	start := bucket.Unix()
	end := bucket.Add(time.Minute).Unix()

	var res tempopb.QueryRangeResponse
	q := url.QueryEscape(`{ nestedSetParent<0 } | quantile_over_time(duration, 0.9, 0.99) by (span.http.method)`)
	h.getJSON(t, fmt.Sprintf("/api/metrics/query_range?q=%s&start=%d&end=%d&step=60", q, start, end), &res)
	if len(res.Series) != 2 {
		t.Fatalf("quantile series = %d", len(res.Series))
	}
	for _, s := range res.Series {
		labels := map[string]any{}
		for _, l := range s.Labels {
			labels[l.Key] = l.Value
		}
		p := labels["p"].(*tempopbAnyValue).GetDoubleValue()
		if labels["span.http.method"].(*tempopbAnyValue).GetStringValue() != "GET" {
			t.Errorf("labels = %v", labels)
		}
		if (p == 0.9 && s.Samples[0].Value != 0.25) || (p == 0.99 && s.Samples[0].Value != 0.5) {
			t.Errorf("p=%v samples = %v", p, s.Samples)
		}
	}

	q = url.QueryEscape(`{ nestedSetParent<0 } | histogram_over_time(duration)`)
	h.getJSON(t, fmt.Sprintf("/api/metrics/query_range?q=%s&start=%d&end=%d&step=60", q, start, end), &res)
	if len(res.Series) != 2 {
		t.Fatalf("histogram series = %d", len(res.Series))
	}
	for _, s := range res.Series {
		if s.Labels[0].Key != "__bucket" {
			t.Errorf("label = %v", s.Labels)
		}
	}
}

func TestMetricsCompare(t *testing.T) {
	bucket := t0.Truncate(time.Hour)
	h := newHarness(t, func(apl string) ([]axiom.Field, [][]any) {
		switch {
		case strings.Contains(apl, "by _bucket = bin(_time, 1h), _sel\n"):
			return fieldsOf("_bucket", "datetime", "_sel", "bool", "v", "integer"), [][]any{
				{bucket.Format(time.RFC3339Nano), false, 90},
				{bucket.Format(time.RFC3339Nano), true, 10},
			}
		case strings.Contains(apl, "_v = ['service.name']"):
			return fieldsOf("_bucket", "datetime", "_sel", "bool", "_v", "string", "v", "integer"), [][]any{
				{bucket.Format(time.RFC3339Nano), false, "frontend", 80},
				{bucket.Format(time.RFC3339Nano), false, "backend", 10},
				{bucket.Format(time.RFC3339Nano), true, "backend", 10},
			}
		case strings.Contains(apl, "_v = "):
			return fieldsOf("_bucket", "datetime", "_sel", "bool", "_v", "string", "v", "integer"), nil
		}
		return defaultRespond(apl)
	})
	q := url.QueryEscape(`{nestedSetParent<0 && true} | compare({status = error}, 10)`)
	var res tempopb.QueryRangeResponse
	h.getJSON(t, fmt.Sprintf("/api/metrics/query_range?q=%s&start=%d&end=%d&step=3600", q, bucket.Unix(), bucket.Add(time.Hour).Unix()), &res)
	var kinds []string
	for _, s := range res.Series {
		kind := s.Labels[0].Value.GetStringValue()
		if len(s.Labels) > 1 {
			kind += ":" + s.Labels[1].Key + "=" + s.Labels[1].Value.GetStringValue()
		}
		kinds = append(kinds, kind)
	}
	want := []string{"baseline_total", "selection_total", "baseline:resource.service.name=backend", "selection:resource.service.name=backend", "baseline:resource.service.name=frontend", "selection:resource.service.name=frontend"}
	for _, w := range want {
		if !contains(kinds, w) {
			t.Errorf("missing series %q in %v", w, kinds)
		}
	}
	sel := h.fake.find("_sel = iff(")
	if !strings.Contains(sel, `iff((['status.code'] =~ "error")`) {
		t.Errorf("compare query:\n%s", sel)
	}
}

func TestMetricsUnsupportedFilterIs400(t *testing.T) {
	h := newHarness(t, nil)
	q := url.QueryEscape(`{ traceDuration > 1s } | rate()`)
	status, body := h.get(t, fmt.Sprintf("/api/metrics/query_range?q=%s&start=%d&end=%d", q, t0.Unix(), t0.Add(time.Minute).Unix()), "")
	if status != 400 || !strings.Contains(string(body), "unsupported") {
		t.Errorf("status = %d body = %s", status, body)
	}
}

func TestMetricsInstant(t *testing.T) {
	bucket := t0.Truncate(time.Hour)
	h := newHarness(t, func(apl string) ([]axiom.Field, [][]any) {
		if strings.Contains(apl, "summarize v = count()") {
			return fieldsOf("_bucket", "datetime", "v", "integer"), [][]any{{bucket.Format(time.RFC3339Nano), 3600}}
		}
		return defaultRespond(apl)
	})
	var res tempopb.QueryInstantResponse
	h.getJSON(t, fmt.Sprintf("/api/metrics/query?q=%s&start=%d&end=%d", url.QueryEscape(`{} | rate()`), bucket.Unix(), bucket.Add(time.Hour).Unix()), &res)
	if len(res.Series) != 1 || res.Series[0].Value != 1 {
		t.Errorf("instant = %v", res.Series)
	}
}

func TestSchemaFallbackWithQueryOnlyToken(t *testing.T) {
	fake := newFakeAxiom(t, defaultRespond)
	fake.fieldsForbidden = true
	cfg := config.Default()
	cfg.AxiomURL = fake.srv.URL
	cfg.AxiomToken = "test-token"
	cfg.Dataset = "otel"
	client, err := axiom.New(axiom.Config{BaseURL: cfg.AxiomURL, Token: cfg.AxiomToken})
	if err != nil {
		t.Fatal(err)
	}
	srv := New(cfg, client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv.Warm(context.Background())
	ds, err := srv.schemas.get(context.Background(), "otel")
	if err != nil {
		t.Fatalf("schema: %v", err)
	}
	if !ds.discovered || !ds.mapping.HasField("attributes.http.method") || ds.mapping.FieldType("duration").String() != "duration" {
		t.Errorf("schema not discovered from probe: discovered=%v", ds.discovered)
	}
	if fake.find("| limit 1") == "" {
		t.Errorf("no probe query issued: %v", fake.recorded())
	}
	// Every span query must exclude log rows that share the dataset.
	web := httptest.NewServer(srv.Handler())
	defer web.Close()
	h := &harness{fake: fake, web: web}
	var res map[string]any
	h.getJSON(t, fmt.Sprintf("/api/search?q=%s&start=%d&end=%d", url.QueryEscape(`{ status = error }`), t0.Add(-time.Hour).Unix(), t0.Add(time.Hour).Unix()), &res)
	if cand := fake.find("summarize m0"); !strings.Contains(cand, "| where isnotempty(trace_id)") {
		t.Errorf("candidate query lacks span guard:\n%s", cand)
	}
}

func TestDatasetPathPrefixAndParam(t *testing.T) {
	h := newHarness(t, nil)
	h.fake.queryError = func(apl string) (int, string) {
		if strings.HasPrefix(apl, "['missing']") {
			return 400, "line: 1, col: 1: unable to find dataset 'missing'"
		}
		return 0, ""
	}
	// Prefix selects the dataset for every endpoint, including v2 detection.
	status, body := h.get(t, "/other/api/v2/traces/"+traceA, "application/json")
	if status != 200 {
		t.Errorf("prefixed trace status = %d %s", status, body)
	}
	if q := h.fake.find("['other']\n| where trace_id"); q == "" {
		t.Errorf("query did not target prefixed dataset: %v", h.fake.recorded())
	}
	// A dataset Axiom rejects is a 404, not a guessed schema.
	status, body = h.get(t, "/missing/api/v2/search/tags", "")
	if status != 404 || !strings.Contains(string(body), "unable to find dataset") {
		t.Errorf("missing dataset = %d %s", status, body)
	}
	status, body = h.get(t, "/otel/api/v2/traces/"+traceA, "application/json")
	if status != 200 || !strings.HasPrefix(string(body), `{"trace":{"resourceSpans":`) {
		t.Errorf("prefixed v2 trace = %d %s", status, body[:min(len(body), 60)])
	}
	status, body = h.get(t, "/otel/api/traces/"+traceA, "application/json")
	if status != 200 || !strings.HasPrefix(string(body), `{"batches":`) {
		t.Errorf("prefixed v1 trace = %d %s", status, body[:min(len(body), 60)])
	}
	status, _ = h.get(t, "/otel/api/echo", "")
	if status != 200 {
		t.Errorf("prefixed echo = %d", status)
	}
	var tags tempopb.SearchTagsV2Response
	h.getJSON(t, "/otel/api/v2/search/tags", &tags)
	if len(tags.Scopes) == 0 {
		t.Errorf("prefixed tags = %v", tags)
	}
	// Query parameter works too.
	status, _ = h.get(t, "/api/traces/"+traceA+"?dataset=third", "")
	if status != 200 || h.fake.find("['third']\n| where trace_id") == "" {
		t.Errorf("query param dataset: status=%d queries=%v", status, h.fake.recorded())
	}
}

func TestNoDefaultDataset(t *testing.T) {
	fake := newFakeAxiom(t, defaultRespond)
	cfg := config.Default()
	cfg.AxiomURL = fake.srv.URL
	cfg.AxiomToken = "test-token"
	cfg.Dataset = ""
	cfg.AllowedDatasets = []string{"otel"}
	client, _ := axiom.New(axiom.Config{BaseURL: cfg.AxiomURL, Token: cfg.AxiomToken})
	srv := New(cfg, client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	srv.Warm(context.Background())
	web := httptest.NewServer(srv.Handler())
	defer web.Close()
	h := &harness{fake: fake, web: web}
	status, body := h.get(t, "/api/traces/"+traceA, "")
	if status != 400 || !strings.Contains(string(body), "no dataset") {
		t.Errorf("no dataset: %d %s", status, body)
	}
	status, _ = h.get(t, "/otel/api/traces/"+traceA, "")
	if status != 200 {
		t.Errorf("allowed prefix: %d", status)
	}
	status, body = h.get(t, "/secret/api/traces/"+traceA, "")
	if status != 400 || !strings.Contains(string(body), "not allowed") {
		t.Errorf("disallowed prefix: %d %s", status, body)
	}
}

func TestDatasetHeader(t *testing.T) {
	h := newHarness(t, nil)
	req, _ := http.NewRequest(http.MethodGet, h.web.URL+"/api/traces/"+traceA, nil)
	req.Header.Set("X-Axiom-Dataset", "other")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	// "other" has no fields endpoint in the fake, so discovery fails and
	// the guessed layout is used; the query must still target it.
	if q := h.fake.find("['other']"); q == "" {
		t.Errorf("query did not target header dataset: %v", h.fake.recorded())
	}
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// tempopbAnyValue aliases the common AnyValue for readability in tests.
type tempopbAnyValue = commonAnyValue

var _ = util.TraceIDToHexString
