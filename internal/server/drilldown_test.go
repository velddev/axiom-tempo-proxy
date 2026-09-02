package server

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"
)

// These tests replay the literal queries Grafana Traces Drilldown issues.

func TestDrilldownStructureQuery(t *testing.T) {
	h := newHarness(t, nil)
	q := `({nestedSetParent<0 && true  && status = error} &>> { status = error }) || ({nestedSetParent<0 && true  && status = error}) | select(status, resource.service.name, name, nestedSetParent, nestedSetLeft, nestedSetRight)`
	var res map[string]any
	h.getJSON(t, fmt.Sprintf("/api/search?q=%s&start=%d&end=%d&limit=200&spss=20", url.QueryEscape(q), t0.Add(-time.Hour).Unix(), t0.Add(time.Hour).Unix()), &res)
	traces, _ := res["traces"].([]any)
	// Trace A has an error span but its root is not an error, so neither
	// side of the union matches. Trace B has no errors.
	if len(traces) != 0 {
		t.Errorf("expected no traces, got %v", traces)
	}
	cand := h.fake.find("summarize m0")
	if cand == "" {
		t.Fatal("no candidate query")
	}
	// Three spanset filters: root-error (lhs), error (rhs), root-error (union rhs).
	if !strings.Contains(cand, "m2 = countif") || !strings.Contains(cand, "((m0 > 0) or (m1 > 0)) or (m2 > 0)") {
		t.Errorf("candidate query:\n%s", cand)
	}
}

func TestDrilldownStructureQueryMatches(t *testing.T) {
	h := newHarness(t, nil)
	// Same shape but rooted at frontend, so the union returns the root and
	// its error descendant.
	q := `({nestedSetParent<0 && resource.service.name = "frontend"} &>> { status = error }) || ({nestedSetParent<0 && resource.service.name = "frontend"}) | select(status, resource.service.name, name, nestedSetParent, nestedSetLeft, nestedSetRight)`
	var res map[string]any
	h.getJSON(t, fmt.Sprintf("/api/search?q=%s&start=%d&end=%d&limit=200&spss=20", url.QueryEscape(q), t0.Add(-time.Hour).Unix(), t0.Add(time.Hour).Unix()), &res)
	traces, _ := res["traces"].([]any)
	if len(traces) != 2 {
		t.Fatalf("traces = %d: %v", len(traces), res)
	}
	for _, tr := range traces {
		m := tr.(map[string]any)
		spanSets := m["spanSets"].([]any)
		spansOut := spanSets[0].(map[string]any)["spans"].([]any)
		ids := map[string]bool{}
		for _, sp := range spansOut {
			ids[sp.(map[string]any)["spanID"].(string)] = true
			attrs := sp.(map[string]any)["attributes"].([]any)
			keys := map[string]bool{}
			for _, a := range attrs {
				keys[a.(map[string]any)["key"].(string)] = true
			}
			if !keys["nestedSetLeft"] || !keys["nestedSetRight"] || !keys["nestedSetParent"] || !keys["service.name"] || !keys["status"] {
				t.Errorf("structure attrs = %v", keys)
			}
		}
		switch m["traceID"] {
		case traceA:
			if !ids["a000000000000001"] || !ids["a000000000000003"] || ids["a000000000000002"] {
				t.Errorf("trace A spans = %v", ids)
			}
		case traceB:
			if !ids["b000000000000001"] || len(ids) != 1 {
				t.Errorf("trace B spans = %v", ids)
			}
		default:
			t.Errorf("unexpected trace %v", m["traceID"])
		}
	}
}

func TestDrilldownExceptionsQuery(t *testing.T) {
	h := newHarness(t, nil)
	q := `{nestedSetParent<0 && true && status = error} | select(resource.service.name, event.exception.message,event.exception.stacktrace,event.exception.type) with(most_recent=true)`
	// No root span is an error in the fixture; relax to any error span so
	// the event attributes get exercised.
	q = strings.Replace(q, "nestedSetParent<0 && true && ", "", 1)
	var res map[string]any
	h.getJSON(t, fmt.Sprintf("/api/search?q=%s&start=%d&end=%d&limit=400&spss=10", url.QueryEscape(q), t0.Add(-time.Hour).Unix(), t0.Add(time.Hour).Unix()), &res)
	traces, _ := res["traces"].([]any)
	if len(traces) != 1 {
		t.Fatalf("traces = %v", res)
	}
	spanSets := traces[0].(map[string]any)["spanSets"].([]any)
	spansOut := spanSets[0].(map[string]any)["spans"].([]any)
	attrs := spansOut[0].(map[string]any)["attributes"].([]any)
	vals := map[string]any{}
	for _, a := range attrs {
		kv := a.(map[string]any)
		vals[kv["key"].(string)] = kv["value"].(map[string]any)["stringValue"]
	}
	if vals["exception.message"] != "boom" || vals["exception.type"] != "DbError" || vals["service.name"] != "backend" {
		t.Errorf("exception attrs = %v", vals)
	}
	// Traces carrying the selected event attributes are ranked first so a
	// bounded result is not dominated by exception-free errors.
	cand := h.fake.find("summarize m0")
	if !strings.Contains(cand, "sel = countif((isnotnull(events)) or (isnotnull(['service.name'])))") ||
		!strings.Contains(cand, "| extend _pref = iff(sel > 0, 1, 0)\n| sort by _pref desc, start desc") {
		t.Errorf("candidate query should prefer traces with selected attributes:\n%s", cand)
	}
}

func TestSearchWithoutSelectKeepsRecencyOrder(t *testing.T) {
	h := newHarness(t, nil)
	var res map[string]any
	h.getJSON(t, fmt.Sprintf("/api/search?q=%s&start=%d&end=%d", url.QueryEscape(`{ status = error }`), t0.Add(-time.Hour).Unix(), t0.Add(time.Hour).Unix()), &res)
	cand := h.fake.find("summarize m0")
	if strings.Contains(cand, "_pref") || !strings.Contains(cand, "| sort by start desc") {
		t.Errorf("unexpected ranking without select():\n%s", cand)
	}
}

func TestDrilldownDurationAndSpanFilters(t *testing.T) {
	h := newHarness(t, nil)
	q := `{nestedSetParent<0 && true && duration > 100ms}`
	var res map[string]any
	h.getJSON(t, fmt.Sprintf("/api/search?q=%s&start=%d&end=%d&limit=200&spss=10", url.QueryEscape(q), t0.Add(-time.Hour).Unix(), t0.Add(time.Hour).Unix()), &res)
	traces, _ := res["traces"].([]any)
	if len(traces) != 1 || traces[0].(map[string]any)["traceID"] != traceA {
		t.Errorf("traces = %v", traces)
	}
	cand := h.fake.find("summarize m0")
	if !strings.Contains(cand, "(isempty(parent_span_id)) and (duration > 100ms)") {
		t.Errorf("candidate query:\n%s", cand)
	}

	// A custom-map attribute filter Drilldown builds from an ad-hoc filter.
	q = `{nestedSetParent<0 && true && span.app.user.id = "u-1"}`
	h.getJSON(t, fmt.Sprintf("/api/search?q=%s&start=%d&end=%d", url.QueryEscape(q), t0.Add(-time.Hour).Unix(), t0.Add(time.Hour).Unix()), &res)
	traces, _ = res["traces"].([]any)
	if len(traces) != 1 {
		t.Errorf("custom attr traces = %v", traces)
	}
	cand = h.fake.find(`tostring(['attributes.custom']['app.user.id']) == "u-1"`)
	if cand == "" {
		t.Errorf("custom attr not pushed down: %v", h.fake.recorded())
	}
}

func TestDrilldownIssueDetectorProbe(t *testing.T) {
	h := newHarness(t, func(apl string) ([]axiomField, [][]any) {
		if strings.Contains(apl, "summarize v = count()") {
			return fieldsOf("_bucket", "datetime", "v", "integer"), nil
		}
		return defaultRespond(apl)
	})
	status, body := h.get(t, fmt.Sprintf("/api/metrics/query_range?q=%s&start=%d&end=%d&step=30s", url.QueryEscape(`{} | rate()`), t0.Unix(), t0.Add(time.Minute).Unix()), "")
	if status != 200 {
		t.Fatalf("probe status = %d %s", status, body)
	}
	// An empty result still returns one zero-filled series so the panel
	// renders instead of showing "metrics not configured".
	if !strings.Contains(string(body), `"samples":[{"timestampMs":`) {
		t.Errorf("probe body = %s", body)
	}
}
