package server

import (
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/grafana/tempo/pkg/tempopb"

	"github.com/velddev/axiom-tempo-proxy/internal/axiom"
	"github.com/velddev/axiom-tempo-proxy/internal/config"
)

// These tests cover what the proxy does when Axiom cannot return
// everything: batched span pulls, dropped traces, and partial results.

// findAll returns every recorded query containing substr, in order.
func (f *fakeAxiom) findAll(substr string) []string {
	var out []string
	for _, q := range f.recorded() {
		if strings.Contains(q, substr) {
			out = append(out, q)
		}
	}
	return out
}

// limitOf returns the row limit of a query's trailing limit stage.
func limitOf(apl string) int {
	i := strings.LastIndex(apl, "| limit ")
	if i < 0 {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(apl[i+len("| limit "):]))
	if err != nil {
		return 0
	}
	return n
}

// synthTraceID returns the id of the i-th synthetic trace. Ids ascend
// with i, so candidate order, trace-id order and row order all agree.
func synthTraceID(i int) string { return fmt.Sprintf("a%031x", i+1) }

func synthSpanRow(trace, span int) []any {
	var parent any
	if span > 0 {
		parent = fmt.Sprintf("b%015x", trace*1000+1)
	}
	return []any{
		t0.Add(time.Duration(span) * time.Millisecond).Format(time.RFC3339Nano),
		synthTraceID(trace), fmt.Sprintf("b%015x", trace*1000+span+1), parent,
		fmt.Sprintf("op %d", span), "server", "10ms", "OK", "", false, "svc", "http",
		"GET", 200, map[string]any{}, map[string]any{}, "pod-1", nil,
	}
}

// synthRespond serves a search over n candidate traces of spansPer spans
// each. A span pull returns the requested traces in trace-id order and
// honours the query's row limit, which is how Axiom answers
// "sort by trace_id asc | limit n" once the limit bites.
func synthRespond(n, spansPer int) func(string) ([]axiom.Field, [][]any) {
	return func(apl string) ([]axiom.Field, [][]any) {
		switch {
		case strings.Contains(apl, "summarize m0"):
			rows := make([][]any, 0, n)
			for i := range n {
				rows = append(rows, []any{synthTraceID(i), t0.Add(-time.Duration(i) * time.Second).Format(time.RFC3339Nano)})
			}
			return fieldsOf("trace_id", "string", "start", "datetime"), rows
		case strings.Contains(apl, "trace_id in ("):
			var rows [][]any
			for i := range n {
				if !strings.Contains(apl, `"`+synthTraceID(i)+`"`) {
					continue
				}
				for s := range spansPer {
					rows = append(rows, synthSpanRow(i, s))
				}
			}
			if lim := limitOf(apl); lim > 0 && len(rows) > lim {
				rows = rows[:lim]
			}
			return spanFields(), rows
		}
		return defaultRespond(apl)
	}
}

func searchURL(q string, limit int) string {
	return fmt.Sprintf("/api/search?q=%s&start=%d&end=%d&limit=%d&spss=10",
		url.QueryEscape(q), t0.Add(-time.Hour).Unix(), t0.Add(time.Hour).Unix(), limit)
}

// traceIDs returns the ids of the returned traces, padded as Tempo does.
func traceIDs(res *tempopb.SearchResponse) []string {
	out := make([]string, 0, len(res.Traces))
	for _, tr := range res.Traces {
		out = append(out, tr.TraceID)
	}
	return out
}

func TestSearchPullsSpansInBatchesInCandidateOrder(t *testing.T) {
	const traces, spansPer = 5, 2
	h := newHarness(t, synthRespond(traces, spansPer), func(c *config.Config) {
		c.SearchBatchTraces = 2
	})
	var res tempopb.SearchResponse
	h.getJSON(t, searchURL("{}", 20), &res)

	pulls := h.fake.findAll("trace_id in (")
	if len(pulls) != 3 {
		t.Fatalf("expected 3 batched pulls, got %d:\n%s", len(pulls), strings.Join(pulls, "\n---\n"))
	}
	// Batches follow candidate order and never overlap.
	for i, want := range [][]int{{0, 1}, {2, 3}, {4}} {
		for j := range traces {
			got := strings.Contains(pulls[i], synthTraceID(j))
			expect := contains(intsAsStrings(want), strconv.Itoa(j))
			if got != expect {
				t.Errorf("batch %d: trace %d present=%v, want %v:\n%s", i, j, got, expect, pulls[i])
			}
		}
		if !strings.Contains(pulls[i], "| sort by trace_id asc") {
			t.Errorf("batch %d must sort so truncation lands on a trace boundary:\n%s", i, pulls[i])
		}
	}
	// Every candidate came back whole, so nothing is reported dropped.
	if len(res.Traces) != traces {
		t.Fatalf("traces = %v", traceIDs(&res))
	}
	for _, tr := range res.Traces {
		if len(tr.SpanSets) == 0 || len(tr.SpanSets[0].Spans) != spansPer {
			t.Errorf("trace %s returned %v spansets", tr.TraceID, tr.SpanSets)
		}
	}
	if n := res.Metrics.AdditionalMetrics["droppedTraces"]; n != 0 {
		t.Errorf("droppedTraces = %d, want 0", n)
	}
}

func TestSearchDropsTracesItCannotFetchWhole(t *testing.T) {
	const traces, spansPer = 4, 2
	// The budget cuts inside the third trace: 2 + 2 spans fit, the fifth
	// row is half of trace 2 and trace 3 is never fetched.
	h := newHarness(t, synthRespond(traces, spansPer), func(c *config.Config) {
		c.SearchBatchTraces = 10
		c.MaxSpansPerFetch = 5
	})
	var res tempopb.SearchResponse
	h.getJSON(t, searchURL("{}", 20), &res)

	if pulls := h.fake.findAll("trace_id in ("); len(pulls) != 1 {
		t.Fatalf("expected the pull to stop once the budget is spent, got %d queries", len(pulls))
	}
	want := []string{synthTraceID(0), synthTraceID(1)}
	if got := traceIDs(&res); len(got) != 2 || !contains(got, want[0]) || !contains(got, want[1]) {
		t.Fatalf("traces = %v, want the two complete ones %v", got, want)
	}
	// A returned trace always carries all of its spans.
	for _, tr := range res.Traces {
		if len(tr.SpanSets) == 0 || len(tr.SpanSets[0].Spans) != spansPer {
			t.Errorf("trace %s is truncated: %v", tr.TraceID, tr.SpanSets)
		}
	}
	if n := res.Metrics.AdditionalMetrics["droppedTraces"]; n != 2 {
		t.Errorf("droppedTraces = %d, want 2 (the cut trace and the unfetched one)", n)
	}
}

func TestSearchDropsCandidatesLeftOverByTheBudget(t *testing.T) {
	const traces, spansPer = 6, 2
	// Two traces per batch, and a budget for only the first two batches.
	h := newHarness(t, synthRespond(traces, spansPer), func(c *config.Config) {
		c.SearchBatchTraces = 2
		c.MaxSpansPerFetch = 8
	})
	var res tempopb.SearchResponse
	h.getJSON(t, searchURL("{}", 20), &res)

	if pulls := h.fake.findAll("trace_id in ("); len(pulls) != 2 {
		t.Fatalf("pull queries = %d, want 2", len(pulls))
	}
	if len(res.Traces) != 4 {
		t.Fatalf("traces = %v", traceIDs(&res))
	}
	if n := res.Metrics.AdditionalMetrics["droppedTraces"]; n != 2 {
		t.Errorf("droppedTraces = %d, want 2", n)
	}
}

func TestMetricsReportsAxiomPartialResults(t *testing.T) {
	bucket := t0.Truncate(time.Minute)
	rate := func(apl string) ([]axiom.Field, [][]any) {
		if strings.Contains(apl, "summarize v = count()") {
			return fieldsOf("_bucket", "datetime", "v", "integer"), [][]any{
				{bucket.Format(time.RFC3339Nano), 60},
			}
		}
		return defaultRespond(apl)
	}
	h := newHarness(t, rate)
	h.fake.status = func(apl string) map[string]any {
		if !strings.Contains(apl, "summarize v = count()") {
			return nil
		}
		return map[string]any{
			"isPartial": true,
			"messages": []map[string]any{
				{"code": "query_limit_reached", "msg": "query stopped early", "priority": "warn", "count": 1},
			},
		}
	}
	q := url.QueryEscape(`{} | rate()`)
	start, end := bucket.Unix(), bucket.Add(time.Minute).Unix()

	var res tempopb.QueryRangeResponse
	h.getJSON(t, fmt.Sprintf("/api/metrics/query_range?q=%s&start=%d&end=%d&step=60", q, start, end), &res)
	if res.Status != tempopb.PartialStatus_PARTIAL {
		t.Errorf("status = %v, want PARTIAL", res.Status)
	}
	if !strings.Contains(res.Message, "partial results") || !strings.Contains(res.Message, "query stopped early") {
		t.Errorf("message = %q", res.Message)
	}

	var inst tempopb.QueryInstantResponse
	h.getJSON(t, fmt.Sprintf("/api/metrics/query?q=%s&start=%d&end=%d", q, start, end), &inst)
	if inst.Status != tempopb.PartialStatus_PARTIAL || !strings.Contains(inst.Message, "partial results") {
		t.Errorf("instant status = %v message = %q", inst.Status, inst.Message)
	}
}

func TestMetricsSurfacesAxiomMessagesWithoutPartial(t *testing.T) {
	bucket := t0.Truncate(time.Minute)
	h := newHarness(t, func(apl string) ([]axiom.Field, [][]any) {
		if strings.Contains(apl, "summarize v = count()") {
			return fieldsOf("_bucket", "datetime", "v", "integer"), [][]any{{bucket.Format(time.RFC3339Nano), 60}}
		}
		return defaultRespond(apl)
	})
	h.fake.status = func(apl string) map[string]any {
		if !strings.Contains(apl, "summarize v = count()") {
			return nil
		}
		return map[string]any{"messages": []map[string]any{{"code": "field_deprecated", "msg": "field is deprecated", "count": 2}}}
	}
	var res tempopb.QueryRangeResponse
	h.getJSON(t, fmt.Sprintf("/api/metrics/query_range?q=%s&start=%d&end=%d&step=60",
		url.QueryEscape(`{} | rate()`), bucket.Unix(), bucket.Add(time.Minute).Unix()), &res)
	if res.Status != tempopb.PartialStatus_COMPLETE {
		t.Errorf("status = %v, want COMPLETE", res.Status)
	}
	if !strings.Contains(res.Message, "field is deprecated (x2)") {
		t.Errorf("message = %q", res.Message)
	}
}

func TestTraceByIDPartialWhenTruncated(t *testing.T) {
	// Trace A has three spans; the query may only return two.
	h := newHarness(t, func(apl string) ([]axiom.Field, [][]any) {
		fields, rows := defaultRespond(apl)
		if lim := limitOf(apl); lim > 0 && len(rows) > lim {
			rows = rows[:lim]
		}
		return fields, rows
	}, func(c *config.Config) { c.MaxSpansPerFetch = 2 })

	path := fmt.Sprintf("/api/v2/traces/%s?start=%d&end=%d", traceA, t0.Add(-time.Hour).Unix(), t0.Add(time.Hour).Unix())
	var res tempopb.TraceByIDResponse
	h.getJSON(t, path, &res)
	if res.Status != tempopb.PartialStatus_PARTIAL {
		t.Errorf("status = %v, want PARTIAL", res.Status)
	}
	if !strings.Contains(res.Message, "truncated") {
		t.Errorf("message = %q", res.Message)
	}
	// The trace is still returned, just flagged.
	if res.Trace == nil || len(res.Trace.ResourceSpans) == 0 {
		t.Errorf("trace = %v", res.Trace)
	}

	// v1 has no status field, so it just returns the trace.
	status, body := h.get(t, "/api/traces/"+traceA, "application/json")
	if status != 200 || !strings.HasPrefix(string(body), `{"batches":[`) {
		t.Errorf("v1 trace = %d %s", status, body)
	}
}

func TestTraceByIDCompleteHasNoStatus(t *testing.T) {
	h := newHarness(t, nil)
	var res tempopb.TraceByIDResponse
	h.getJSON(t, fmt.Sprintf("/api/v2/traces/%s?start=%d&end=%d", traceA, t0.Add(-time.Hour).Unix(), t0.Add(time.Hour).Unix()), &res)
	if res.Status != tempopb.PartialStatus_COMPLETE || res.Message != "" {
		t.Errorf("complete trace reported %v %q", res.Status, res.Message)
	}
}

func intsAsStrings(in []int) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		out = append(out, strconv.Itoa(v))
	}
	return out
}
