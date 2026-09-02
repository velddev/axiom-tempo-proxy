package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/velddev/axiom-tempo-proxy/internal/axiom"
	"github.com/velddev/axiom-tempo-proxy/internal/config"
)

// intrinsicProject is the column list every projected span pull starts
// with: the intrinsics of the fixture schema plus its top-level resource
// fields (only service.name exists in the fixture).
const intrinsicProject = "_time, trace_id, span_id, parent_span_id, name, ['kind'], duration, " +
	"['status.code'], ['status.message'], error, ['service.name'], ['scope.name']"

// The span pull is the expensive query of a search: it returns whole
// spans for every candidate trace. It must therefore carry only the
// columns the query's attributes need, and never a column the dataset
// does not have (APL fails such a query outright).
func TestSearchPullProjectsNeededColumns(t *testing.T) {
	cases := []struct {
		name   string
		query  string
		want   string
		absent []string
	}{{
		name:   "status only",
		query:  `{ status = error }`,
		want:   "| project " + intrinsicProject + "\n",
		absent: []string{"attributes.custom", "attributes.http.method", "resource.k8s.pod.name", "events"},
	}, {
		name:   "selected attributes",
		query:  `{nestedSetParent<0 && true} | select(resource.service.name, span.http.method)`,
		want:   "| project " + intrinsicProject + ", ['attributes.http.method']\n",
		absent: []string{"attributes.custom", "resource.k8s.pod.name", "events"},
	}, {
		name:   "event attributes",
		query:  `{ status = error } | select(resource.service.name, event.exception.message, event.exception.type)`,
		want:   "| project " + intrinsicProject + ", events\n",
		absent: []string{"attributes.custom", "attributes.http.method", "resource.k8s.pod.name"},
	}, {
		name:   "custom map attribute",
		query:  `{ span.app.user.id = "u-1" }`,
		want:   "| project " + intrinsicProject + ", ['attributes.custom']\n",
		absent: []string{"attributes.http.method", "resource.k8s.pod.name", "events"},
	}}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newHarness(t, nil)
			var res map[string]any
			h.getJSON(t, fmt.Sprintf("/api/search?q=%s&start=%d&end=%d&limit=20&spss=10",
				url.QueryEscape(tc.query), t0.Add(-time.Hour).Unix(), t0.Add(time.Hour).Unix()), &res)
			pull := h.fake.find("trace_id in (")
			if pull == "" {
				t.Fatalf("no span pull query: %v", h.fake.recorded())
			}
			if !strings.Contains(pull+"\n", tc.want) {
				t.Errorf("pull query:\n%s\nwant project stage:\n%s", pull, tc.want)
			}
			for _, col := range tc.absent {
				if strings.Contains(pull, col) {
					t.Errorf("pull query projects %q it does not need:\n%s", col, pull)
				}
			}
		})
	}
}

// Trace by id must keep fetching every column: the trace view shows all
// attributes of a span, not just the ones a query mentioned.
func TestTraceByIDIsNotProjected(t *testing.T) {
	h := newHarness(t, nil)
	var res map[string]any
	h.getJSON(t, "/api/traces/"+traceA, &res)
	q := h.fake.find("trace_id == ")
	if q == "" {
		t.Fatalf("no trace query: %v", h.fake.recorded())
	}
	if strings.Contains(q, "| project") {
		t.Errorf("trace-by-id query must not project:\n%s", q)
	}
}

// Without a discovered schema there is no way to tell which columns
// exist, and naming a missing one fails the whole query, so the pull asks
// for everything.
func TestSearchPullNotProjectedWithoutSchema(t *testing.T) {
	fake := newFakeAxiom(t, func(apl string) ([]axiom.Field, [][]any) {
		if strings.HasSuffix(apl, "| limit 1") {
			// Probe returns no columns, so discovery fails entirely.
			return nil, nil
		}
		return defaultRespond(apl)
	})
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
	ds, _ := srv.schemas.get(context.Background(), "otel")
	if ds != nil && ds.discovered {
		t.Fatalf("schema should not be discovered in this fake")
	}
	web := httptest.NewServer(srv.Handler())
	defer web.Close()
	h := &harness{fake: fake, web: web}

	var res map[string]any
	h.getJSON(t, fmt.Sprintf("/api/search?q=%s&start=%d&end=%d",
		url.QueryEscape(`{ status = error }`), t0.Add(-time.Hour).Unix(), t0.Add(time.Hour).Unix()), &res)
	pull := h.fake.find("trace_id in (")
	if pull == "" {
		t.Fatalf("no span pull query: %v", h.fake.recorded())
	}
	if strings.Contains(pull, "| project") {
		t.Errorf("pull query must not project without a discovered schema:\n%s", pull)
	}
}
