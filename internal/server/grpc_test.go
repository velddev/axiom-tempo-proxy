package server

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/grafana/tempo/pkg/tempopb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/velddev/axiom-tempo-proxy/internal/axiom"
	"github.com/velddev/axiom-tempo-proxy/internal/config"
)

// grpcClient dials the harness's port with cleartext HTTP/2, exactly as
// Grafana's Tempo datasource does when streaming is enabled.
func (h *harness) grpcClient(t *testing.T) tempopb.StreamingQuerierClient {
	t.Helper()
	conn, err := grpc.NewClient(strings.TrimPrefix(h.web.URL, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return tempopb.NewStreamingQuerierClient(conn)
}

type recver[T any] interface{ Recv() (T, error) }

// onlyMessage drains a stream and returns its single final message.
func onlyMessage[T any](t *testing.T, stream recver[T]) T {
	t.Helper()
	var last T
	n := 0
	for {
		msg, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("recv: %v", err)
		}
		last, n = msg, n+1
	}
	if n != 1 {
		t.Fatalf("expected exactly one message, got %d", n)
	}
	return last
}

// grpcRespond serves the query shapes the streaming tests exercise.
func grpcRespond(apl string) ([]axiom.Field, [][]any) {
	bucket := t0.Truncate(time.Minute)
	switch {
	case strings.Contains(apl, "by _bucket = bin(_time, 1m), g0 = ['service.name']"):
		// Exemplar columns come back from the same summarize, so the
		// streamed response carries exemplars too.
		return fieldsOf("_bucket", "datetime", "g0", "string", "v", "integer",
				"_exv", "float", "_exid", "string", "_exts", "datetime"), [][]any{
				{bucket.Format(time.RFC3339Nano), "frontend", 120, 0.12, traceA, bucket.Add(10 * time.Second).Format(time.RFC3339Nano)},
				{bucket.Add(time.Minute).Format(time.RFC3339Nano), "frontend", 240, 0.05, traceB, bucket.Add(70 * time.Second).Format(time.RFC3339Nano)},
			}
	case strings.Contains(apl, "summarize c = count() by v = ['service.name']"):
		return fieldsOf("v", "string", "c", "integer"), [][]any{{"frontend", 10}, {"backend", 3}}
	}
	return defaultRespond(apl)
}

// TestGRPCStreamingMatchesHTTP exercises the StreamingQuerier service over
// h2c on the same port as the HTTP API and asserts every response equals
// the one the corresponding HTTP endpoint returns.
func TestGRPCStreamingMatchesHTTP(t *testing.T) {
	h := newHarness(t, grpcRespond)
	client := h.grpcClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// --- Search ---
	query := `{ status = error }`
	start, end := t0.Add(-time.Hour).Unix(), t0.Add(time.Hour).Unix()
	stream, err := client.Search(ctx, &tempopb.SearchRequest{
		Query: query, Limit: 20, SpansPerSpanSet: 3,
		Start: uint32(start), End: uint32(end),
	})
	if err != nil {
		t.Fatal(err)
	}
	gotSearch := onlyMessage[*tempopb.SearchResponse](t, stream)
	if len(gotSearch.Traces) != 1 || gotSearch.Traces[0].TraceID != traceA {
		t.Fatalf("search traces = %v", gotSearch.Traces)
	}
	var wantSearch tempopb.SearchResponse
	h.getJSON(t, fmt.Sprintf("/api/search?q=%s&start=%d&end=%d&limit=20&spss=3", url.QueryEscape(query), start, end), &wantSearch)
	assertProtoEqual(t, "Search", gotSearch, &wantSearch)

	// --- SearchTagsV2 ---
	tagsStream, err := client.SearchTagsV2(ctx, &tempopb.SearchTagsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	gotTags := onlyMessage[*tempopb.SearchTagsV2Response](t, tagsStream)
	scopes := map[string][]string{}
	for _, sc := range gotTags.Scopes {
		scopes[sc.Name] = sc.Tags
	}
	// The event scope comes from the sampled events array, so streaming
	// sees the same tags the HTTP endpoint does.
	if !contains(scopes["resource"], "service.name") || !contains(scopes["span"], "http.method") ||
		!contains(scopes["event"], "exception.type") || !contains(scopes["intrinsic"], "status") {
		t.Fatalf("scopes = %v", scopes)
	}
	var wantTags tempopb.SearchTagsV2Response
	h.getJSON(t, "/api/v2/search/tags", &wantTags)
	assertProtoEqual(t, "SearchTagsV2", gotTags, &wantTags)

	// --- SearchTagValuesV2 ---
	valuesStream, err := client.SearchTagValuesV2(ctx, &tempopb.SearchTagValuesRequest{
		TagName: "resource.service.name", Query: query, MaxTagValues: 100,
	})
	if err != nil {
		t.Fatal(err)
	}
	gotValues := onlyMessage[*tempopb.SearchTagValuesV2Response](t, valuesStream)
	if len(gotValues.TagValues) != 2 || gotValues.TagValues[0].Value != "frontend" {
		t.Fatalf("tag values = %v", gotValues.TagValues)
	}
	var wantValues tempopb.SearchTagValuesV2Response
	h.getJSON(t, "/api/v2/search/tag/resource.service.name/values?q="+url.QueryEscape(query)+"&limit=100", &wantValues)
	assertProtoEqual(t, "SearchTagValuesV2", gotValues, &wantValues)

	// --- MetricsQueryRange ---
	bucket := t0.Truncate(time.Minute)
	mq := `{nestedSetParent<0 && true && resource.service.name != nil} | rate() by(resource.service.name)`
	metricsStream, err := client.MetricsQueryRange(ctx, &tempopb.QueryRangeRequest{
		Query: mq,
		Start: uint64(bucket.UnixNano()),
		End:   uint64(bucket.Add(3 * time.Minute).UnixNano()),
		Step:  uint64(time.Minute),
	})
	if err != nil {
		t.Fatal(err)
	}
	gotMetrics := onlyMessage[*tempopb.QueryRangeResponse](t, metricsStream)
	if len(gotMetrics.Series) != 1 || len(gotMetrics.Series[0].Samples) != 3 {
		t.Fatalf("series = %v", gotMetrics.Series)
	}
	if len(gotMetrics.Series[0].Exemplars) != 2 {
		t.Errorf("streamed exemplars = %v", gotMetrics.Series[0].Exemplars)
	}
	var wantMetrics tempopb.QueryRangeResponse
	h.getJSON(t, fmt.Sprintf("/api/metrics/query_range?q=%s&start=%d&end=%d&step=60",
		url.QueryEscape(mq), bucket.Unix(), bucket.Add(3*time.Minute).Unix()), &wantMetrics)
	assertProtoEqual(t, "MetricsQueryRange", gotMetrics, &wantMetrics)

	// The same port still speaks HTTP/1.1.
	if status, body := h.get(t, "/api/echo", ""); status != http.StatusOK || string(body) != "echo" {
		t.Errorf("HTTP/1.1 on the gRPC port: %d %q", status, body)
	}
}

func assertProtoEqual(t *testing.T, what string, got, want proto.Message) {
	t.Helper()
	if !proto.Equal(got, want) {
		t.Errorf("%s: gRPC response differs from HTTP\n grpc: %v\n http: %v", what, got, want)
	}
}

// TestGRPCDatasetSelection covers the two metadata keys that pick a
// dataset over gRPC, and the InvalidArgument returned when a call selects
// no dataset and none is configured.
func TestGRPCDatasetSelection(t *testing.T) {
	fake := newFakeAxiom(t, defaultRespond)
	cfg := config.Default()
	cfg.AxiomURL = fake.srv.URL
	cfg.AxiomToken = "test-token"
	cfg.Dataset = "" // no default
	cfg.AllowedDatasets = []string{"otel"}
	client, err := axiom.New(axiom.Config{BaseURL: cfg.AxiomURL, Token: cfg.AxiomToken})
	if err != nil {
		t.Fatal(err)
	}
	srv := New(cfg, client, slog.New(slog.NewTextHandler(io.Discard, nil)))
	web := httptest.NewServer(srv.Handler())
	defer web.Close()
	h := &harness{fake: fake, web: web}
	querier := h.grpcClient(t)

	search := func(ctx context.Context) error {
		stream, err := querier.Search(ctx, &tempopb.SearchRequest{Query: `{ status = error }`, Limit: 20})
		if err != nil {
			return err
		}
		for {
			if _, err := stream.Recv(); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// No dataset at all and no default configured.
	err = search(ctx)
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Fatalf("no dataset: code = %s (%v)", code, err)
	}
	if !strings.Contains(status.Convert(err).Message(), "no dataset") {
		t.Errorf("no dataset: message = %q", status.Convert(err).Message())
	}

	// The dataset header, forwarded by Grafana as gRPC metadata.
	if err := search(metadata.AppendToOutgoingContext(ctx, cfg.DatasetHeader, "otel")); err != nil {
		t.Errorf("header dataset: %v", err)
	}
	// The plain dataset metadata key.
	if err := search(metadata.AppendToOutgoingContext(ctx, "dataset", "otel")); err != nil {
		t.Errorf("dataset metadata: %v", err)
	}
	// A dataset outside the allow-list.
	err = search(metadata.AppendToOutgoingContext(ctx, "dataset", "secret"))
	if code := status.Code(err); code != codes.InvalidArgument {
		t.Errorf("disallowed dataset: code = %s (%v)", code, err)
	}
}

// TestGRPCCarriesPartialAndEventResults checks that dropped traces,
// Axiom's partial status and event attribute values reach streaming
// clients too, since both transports share the run* functions.
func TestGRPCCarriesPartialAndEventResults(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	t.Run("droppedTraces", func(t *testing.T) {
		const traces, spansPer = 6, 2
		h := newHarness(t, synthRespond(traces, spansPer), func(c *config.Config) {
			c.SearchBatchTraces = 2
			c.MaxSpansPerFetch = 8
		})
		stream, err := h.grpcClient(t).Search(ctx, &tempopb.SearchRequest{
			Query: "{}", Limit: 20, SpansPerSpanSet: 10,
			Start: uint32(t0.Add(-time.Hour).Unix()), End: uint32(t0.Add(time.Hour).Unix()),
		})
		if err != nil {
			t.Fatal(err)
		}
		res := onlyMessage[*tempopb.SearchResponse](t, stream)
		if len(res.Traces) != 4 {
			t.Errorf("traces = %v", traceIDs(res))
		}
		if n := res.Metrics.AdditionalMetrics[droppedTracesMetric]; n != 2 {
			t.Errorf("droppedTraces = %d, want 2", n)
		}
	})

	t.Run("partial metrics", func(t *testing.T) {
		bucket := t0.Truncate(time.Minute)
		h := newHarness(t, func(apl string) ([]axiom.Field, [][]any) {
			if strings.Contains(apl, "summarize v = count()") {
				return fieldsOf("_bucket", "datetime", "v", "integer"), [][]any{
					{bucket.Format(time.RFC3339Nano), 60},
				}
			}
			return defaultRespond(apl)
		})
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
		stream, err := h.grpcClient(t).MetricsQueryRange(ctx, &tempopb.QueryRangeRequest{
			Query: `{} | rate()`,
			Start: uint64(bucket.UnixNano()),
			End:   uint64(bucket.Add(time.Minute).UnixNano()),
			Step:  uint64(time.Minute),
		})
		if err != nil {
			t.Fatal(err)
		}
		res := onlyMessage[*tempopb.QueryRangeResponse](t, stream)
		if res.Status != tempopb.PartialStatus_PARTIAL {
			t.Errorf("status = %v, want PARTIAL", res.Status)
		}
		if !strings.Contains(res.Message, "query stopped early") {
			t.Errorf("message = %q", res.Message)
		}
	})

	t.Run("event tag values", func(t *testing.T) {
		h := newHarness(t, nil)
		client := h.grpcClient(t)
		stream, err := client.SearchTagValuesV2(ctx, &tempopb.SearchTagValuesRequest{
			TagName: "event.exception.message", MaxTagValues: 100,
		})
		if err != nil {
			t.Fatal(err)
		}
		res := onlyMessage[*tempopb.SearchTagValuesV2Response](t, stream)
		if len(res.TagValues) != 2 || res.TagValues[0].Value != "boom" {
			t.Errorf("event values = %v", res.TagValues)
		}
		if q := h.fake.find(`tostring(events['attributes']['exception.message'])`); !strings.Contains(q, "mv-expand events") {
			t.Errorf("event tag values query:\n%s", q)
		}
		// The event:name intrinsic, and the v1 method's scoped spelling.
		v1, err := client.SearchTagValues(ctx, &tempopb.SearchTagValuesRequest{TagName: "event:name"})
		if err != nil {
			t.Fatal(err)
		}
		if got := onlyMessage[*tempopb.SearchTagValuesResponse](t, v1); len(got.TagValues) != 2 || got.TagValues[0] != "exception" {
			t.Errorf("event:name values = %v", got.TagValues)
		}
	})
}
