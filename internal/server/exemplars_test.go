package server

import (
	"fmt"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/grafana/tempo/pkg/tempopb"
)

// These tests pin the exemplars a metrics response carries: Grafana's
// Tempo datasource turns them into the "exemplar" frame that Traces
// Drilldown renders as clickable trace dots on its RED and breakdown
// panels.

// exemplarHarness answers the *_over_time query shapes with rows that
// carry the exemplar columns the evaluator adds.
func exemplarHarness(t *testing.T, bucket time.Time) *harness {
	t.Helper()
	return newHarness(t, func(apl string) ([]axiomField, [][]any) {
		switch {
		case strings.Contains(apl, "q0 = percentile(toreal(duration / 1s), 90.0)"):
			return fieldsOf("_bucket", "datetime", "q0", "float", "q1", "float",
					"_exv", "float", "_exid", "string", "_exts", "datetime"), [][]any{
					{bucket.Format(time.RFC3339Nano), 0.25, 0.5, 0.52, traceA, bucket.Add(30 * time.Second).Format(time.RFC3339Nano)},
				}
		case strings.Contains(apl, "_bucketv = pow(2.0, ceiling(log2("):
			return fieldsOf("_bucket", "datetime", "_bucketv", "float", "v", "integer",
					"_exv", "float", "_exid", "string", "_exts", "datetime"), [][]any{
					{bucket.Format(time.RFC3339Nano), 134217728, 7, 0.13, traceA, bucket.Add(5 * time.Second).Format(time.RFC3339Nano)},
					{bucket.Format(time.RFC3339Nano), 268435456, 2, 0.26, traceB, bucket.Add(9 * time.Second).Format(time.RFC3339Nano)},
				}
		case strings.Contains(apl, "v = min(toreal(duration / 1s))"):
			return fieldsOf("_bucket", "datetime", "v", "float",
					"_exv", "float", "_exid", "string", "_exts", "datetime"), [][]any{
					{bucket.Format(time.RFC3339Nano), 0.005, 0.005, traceB, bucket.Add(15 * time.Second).Format(time.RFC3339Nano)},
				}
		case strings.Contains(apl, "summarize v = count()"):
			return fieldsOf("_bucket", "datetime", "v", "integer",
					"_exv", "float", "_exid", "string", "_exts", "datetime"), [][]any{
					{bucket.Format(time.RFC3339Nano), 60, 0.4, traceA, bucket.Add(20 * time.Second).Format(time.RFC3339Nano)},
				}
		}
		return defaultRespond(apl)
	})
}

// TestExemplarJSONShape pins the wire format Grafana parses.
func TestExemplarJSONShape(t *testing.T) {
	bucket := t0.Truncate(time.Minute)
	h := exemplarHarness(t, bucket)
	q := url.QueryEscape(`{nestedSetParent<0 && true} | rate()  with(sample=true)`)
	status, body := h.get(t, fmt.Sprintf("/api/metrics/query_range?q=%s&start=%d&end=%d&step=60", q, bucket.Unix(), bucket.Add(time.Minute).Unix()), "application/json")
	if status != 200 {
		t.Fatalf("status = %d %s", status, body)
	}
	want := fmt.Sprintf(`"exemplars":[{"labels":[{"key":"trace:id","value":{"stringValue":"%s"}}],"value":1,"timestampMs":"%d"}]`,
		traceA, bucket.Add(20*time.Second).UnixMilli())
	if !strings.Contains(string(body), want) {
		t.Errorf("body does not carry\n\t%s\ngot:\n\t%s", want, body)
	}
}

// TestExemplarQuantileAttachedToClosestSeries mirrors Tempo, which gives
// an exemplar to exactly one quantile series: the closest by value.
func TestExemplarQuantileAttachedToClosestSeries(t *testing.T) {
	bucket := t0.Truncate(time.Minute)
	h := exemplarHarness(t, bucket)
	q := url.QueryEscape(`{ nestedSetParent<0 } | quantile_over_time(duration, 0.9, 0.99)`)
	var res tempopb.QueryRangeResponse
	h.getJSON(t, fmt.Sprintf("/api/metrics/query_range?q=%s&start=%d&end=%d&step=60", q, bucket.Unix(), bucket.Add(time.Minute).Unix()), &res)
	if len(res.Series) != 2 {
		t.Fatalf("series = %d", len(res.Series))
	}
	seen := map[float64]int{}
	for _, s := range res.Series {
		p := s.Labels[0].Value.GetDoubleValue()
		seen[p] = len(s.Exemplars)
		for _, x := range s.Exemplars {
			// quantile_over_time reports a real span value, in seconds
			// for durations, not the quantile itself.
			if x.Value != 0.52 || x.Labels[0].Key != "trace:id" || x.Labels[0].Value.GetStringValue() != traceA {
				t.Errorf("p=%v exemplar = %+v", p, x)
			}
			if x.TimestampMs != bucket.Add(30*time.Second).UnixMilli() {
				t.Errorf("p=%v exemplar ts = %d", p, x.TimestampMs)
			}
		}
	}
	// 0.52 is closer to the p99 value (0.5) than to the p90 value (0.25).
	if seen[0.9] != 0 || seen[0.99] != 1 {
		t.Errorf("exemplars per quantile = %v", seen)
	}
	if apl := h.fake.find("q0 = percentile"); !strings.Contains(apl, "_exv = arg_max(toreal(duration / 1s), _exid, _exts)") {
		t.Errorf("quantile exemplar aggregation:\n%s", apl)
	}
}

// TestExemplarHistogramPerBucketSeries checks the RED duration panel: one
// exemplar per __bucket series, valued with that series' count so it
// lands on the heatmap where Tempo puts it.
func TestExemplarHistogramPerBucketSeries(t *testing.T) {
	bucket := t0.Truncate(time.Minute)
	h := exemplarHarness(t, bucket)
	q := url.QueryEscape(`{ nestedSetParent<0 } | histogram_over_time(duration) with(sample=true)`)
	var res tempopb.QueryRangeResponse
	h.getJSON(t, fmt.Sprintf("/api/metrics/query_range?q=%s&start=%d&end=%d&step=60", q, bucket.Unix(), bucket.Add(time.Minute).Unix()), &res)
	if len(res.Series) != 2 {
		t.Fatalf("series = %d", len(res.Series))
	}
	byTrace := map[string]tempopb.Exemplar{}
	for _, s := range res.Series {
		if s.Labels[0].Key != "__bucket" {
			t.Fatalf("labels = %v", s.Labels)
		}
		if len(s.Exemplars) != 1 {
			t.Fatalf("bucket series exemplars = %v", s.Exemplars)
		}
		byTrace[s.Exemplars[0].Labels[0].Value.GetStringValue()] = s.Exemplars[0]
	}
	if byTrace[traceA].Value != 7 || byTrace[traceB].Value != 2 {
		t.Errorf("histogram exemplar values = %v", byTrace)
	}
	if byTrace[traceA].TimestampMs != bucket.Add(5*time.Second).UnixMilli() {
		t.Errorf("histogram exemplar ts = %d", byTrace[traceA].TimestampMs)
	}
}

// TestExemplarMinOverTimeUsesArgMin checks that the exemplar of
// min_over_time is the span the series value reports.
func TestExemplarMinOverTimeUsesArgMin(t *testing.T) {
	bucket := t0.Truncate(time.Minute)
	h := exemplarHarness(t, bucket)
	q := url.QueryEscape(`{ nestedSetParent<0 } | min_over_time(duration)`)
	var res tempopb.QueryRangeResponse
	h.getJSON(t, fmt.Sprintf("/api/metrics/query_range?q=%s&start=%d&end=%d&step=60", q, bucket.Unix(), bucket.Add(time.Minute).Unix()), &res)
	if len(res.Series) != 1 || len(res.Series[0].Exemplars) != 1 {
		t.Fatalf("series = %v", res.Series)
	}
	x := res.Series[0].Exemplars[0]
	if x.Value != 0.005 || x.Labels[0].Value.GetStringValue() != traceB {
		t.Errorf("min exemplar = %+v", x)
	}
	if apl := h.fake.find("v = min(toreal(duration / 1s))"); !strings.Contains(apl, "_exv = arg_min(toreal(duration / 1s), _exid, _exts)") {
		t.Errorf("min exemplar aggregation:\n%s", apl)
	}
}

// TestExemplarRequestParameter covers the exemplars parameter and the
// with(exemplars=...) hint, which follow Tempo's normalisation.
func TestExemplarRequestParameter(t *testing.T) {
	bucket := t0.Truncate(time.Minute)
	base := func(extra string) string {
		q := url.QueryEscape(`{ nestedSetParent<0 } | rate()` + extra)
		return fmt.Sprintf("/api/metrics/query_range?q=%s&start=%d&end=%d&step=60", q, bucket.Unix(), bucket.Add(time.Minute).Unix())
	}
	cases := []struct {
		name    string
		path    string
		want    int
		queried bool
	}{
		// Grafana and Drilldown never send the parameter; the configured
		// default is what gives their panels dots.
		{"absent", base(""), 1, true},
		// Tempo treats an explicit 0 as "unspecified" too.
		{"zero", base("") + "&exemplars=0", 1, true},
		{"explicit", base("") + "&exemplars=5", 1, true},
		// A false hint switches them off, and then the query must not pay
		// for the extra aggregation either.
		{"hint false", base(" with(exemplars=false)"), 0, false},
		{"hint int", base(" with(exemplars=4)"), 1, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			h := exemplarHarness(t, bucket)
			var res tempopb.QueryRangeResponse
			h.getJSON(t, c.path, &res)
			if len(res.Series) != 1 {
				t.Fatalf("series = %v", res.Series)
			}
			if len(res.Series[0].Exemplars) != c.want {
				t.Errorf("exemplars = %v, want %d", res.Series[0].Exemplars, c.want)
			}
			apl := h.fake.find("summarize v = count()")
			if got := strings.Contains(apl, "arg_max"); got != c.queried {
				t.Errorf("exemplar aggregation present = %v:\n%s", got, apl)
			}
		})
	}
}

// TestExemplarsBoundedByRequestedCount checks that a request asking for
// fewer exemplars than there are buckets still gets a spread of them.
func TestExemplarsBoundedByRequestedCount(t *testing.T) {
	bucket := t0.Truncate(time.Minute)
	h := newHarness(t, func(apl string) ([]axiomField, [][]any) {
		if strings.Contains(apl, "summarize v = count()") {
			var rows [][]any
			for i := range 10 {
				at := bucket.Add(time.Duration(i) * time.Minute)
				rows = append(rows, []any{
					at.Format(time.RFC3339Nano), 60, 0.4, traceA,
					at.Add(time.Second).Format(time.RFC3339Nano),
				})
			}
			return fieldsOf("_bucket", "datetime", "v", "integer",
				"_exv", "float", "_exid", "string", "_exts", "datetime"), rows
		}
		return defaultRespond(apl)
	})
	q := url.QueryEscape(`{ nestedSetParent<0 } | rate()`)
	var res tempopb.QueryRangeResponse
	h.getJSON(t, fmt.Sprintf("/api/metrics/query_range?q=%s&start=%d&end=%d&step=60&exemplars=3", q, bucket.Unix(), bucket.Add(10*time.Minute).Unix()), &res)
	if len(res.Series) != 1 {
		t.Fatalf("series = %v", res.Series)
	}
	ex := res.Series[0].Exemplars
	if len(ex) != 3 {
		t.Fatalf("exemplars = %d, want 3", len(ex))
	}
	// Spread across the range rather than clustered at the start.
	if ex[0].TimestampMs != bucket.Add(time.Second).UnixMilli() ||
		ex[2].TimestampMs != bucket.Add(6*time.Minute+time.Second).UnixMilli() {
		t.Errorf("exemplars not spread: %v", ex)
	}
}

// TestExemplarsOmittedForInstant checks that instant queries carry none:
// InstantSeries has no exemplar field, and Tempo forces them off there.
func TestExemplarsOmittedForInstant(t *testing.T) {
	bucket := t0.Truncate(time.Hour)
	h := exemplarHarness(t, bucket)
	var inst tempopb.QueryInstantResponse
	h.getJSON(t, fmt.Sprintf("/api/metrics/query?q=%s&start=%d&end=%d", url.QueryEscape(`{} | rate()`), bucket.Unix(), bucket.Add(time.Hour).Unix()), &inst)
	if len(inst.Series) == 0 {
		t.Fatalf("instant series = %v", inst.Series)
	}
	if apl := h.fake.find("summarize v = count()"); strings.Contains(apl, "arg_max") {
		t.Errorf("instant query must not aggregate exemplars:\n%s", apl)
	}
}
