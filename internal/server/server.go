// Package server exposes the Tempo query API over HTTP.
package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/grafana/tempo/pkg/tempopb"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/velddev/axiom-tempo-proxy/internal/apl"
	"github.com/velddev/axiom-tempo-proxy/internal/axiom"
	"github.com/velddev/axiom-tempo-proxy/internal/config"
	"github.com/velddev/axiom-tempo-proxy/internal/metrics"
	"github.com/velddev/axiom-tempo-proxy/internal/schema"
	"github.com/velddev/axiom-tempo-proxy/internal/translate"
)

// Version is reported by /api/status/buildinfo.
var Version = "dev"

// Server serves the Tempo API from Axiom.
type Server struct {
	cfg     config.Config
	client  *axiom.Client
	log     *slog.Logger
	schemas *schemaCache
	mux     *http.ServeMux
	grpc    *grpc.Server
}

// New wires a Server.
func New(cfg config.Config, client *axiom.Client, log *slog.Logger) *Server {
	s := &Server{
		cfg:    cfg,
		client: client,
		log:    log,
		schemas: &schemaCache{
			client:  client,
			log:     log,
			refresh: cfg.SchemaRefresh,
			sample:  cfg.TagSampleRows,
			entries: map[string]*datasetSchema{},
		},
	}
	s.routes()
	s.grpc = grpc.NewServer(grpc.StreamInterceptor(s.logStream))
	tempopb.RegisterStreamingQuerierServer(s.grpc, &streamingQuerier{s: s})
	return s
}

// Handler returns the handler for the single listening port. gRPC calls
// (HTTP/2 with an application/grpc content type) go to the StreamingQuerier
// service, everything else to the Tempo HTTP API. h2c accepts cleartext
// HTTP/2, both by prior knowledge (the connection preface Grafana's gRPC
// client sends) and by an HTTP/1.1 upgrade, so one port speaks both.
func (s *Server) Handler() http.Handler {
	httpHandler := s.logging(s.mux)
	mixed := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if isGRPCRequest(r) {
			s.grpc.ServeHTTP(w, r)
			return
		}
		httpHandler.ServeHTTP(w, r)
	})
	return h2c.NewHandler(mixed, &http2.Server{})
}

// isGRPCRequest reports whether a request is a gRPC call. Plain HTTP/1.1
// requests never match, so the HTTP API behaves exactly as before.
func isGRPCRequest(r *http.Request) bool {
	return r.ProtoMajor == 2 && strings.HasPrefix(r.Header.Get("Content-Type"), "application/grpc")
}

// routes registers every Tempo endpoint twice: at the root, where the
// dataset comes from a header, query parameter, or the configured
// default, and under a /{dataset} prefix so a Grafana datasource URL like
// http://proxy:3200/prod selects the dataset by itself.
func (s *Server) routes() {
	mux := http.NewServeMux()
	handle := func(pattern string, h http.HandlerFunc) {
		method, path, _ := strings.Cut(pattern, " ")
		mux.HandleFunc(method+" "+path, h)
		mux.HandleFunc(method+" /{dataset}"+path, h)
	}
	handle("GET /api/echo", s.handleEcho)
	handle("GET /ready", s.handleEcho)
	handle("GET /api/status/buildinfo", s.handleBuildInfo)
	handle("GET /api/traces/{traceID}", s.handleTraceByID)
	handle("GET /api/v2/traces/{traceID}", s.handleTraceByID)
	handle("GET /api/search", s.handleSearch)
	handle("GET /api/search/tags", s.handleSearchTags)
	handle("GET /api/v2/search/tags", s.handleSearchTags)
	handle("GET /api/search/tag/{tagName...}", s.handleSearchTagValues)
	handle("GET /api/v2/search/tag/{tagName...}", s.handleSearchTagValues)
	handle("GET /api/metrics/query_range", s.handleMetricsQueryRange)
	handle("GET /api/metrics/query", s.handleMetricsQueryInstant)
	s.mux = mux
}

// Warm discovers the default dataset's schema so the first request is
// fast. Failures are logged, not fatal.
func (s *Server) Warm(ctx context.Context) {
	if s.cfg.Dataset == "" {
		return
	}
	if _, err := s.schemas.get(ctx, s.cfg.Dataset); err != nil {
		s.log.Warn("schema discovery failed; falling back to guessed layout", "dataset", s.cfg.Dataset, "err", err)
	}
}

// dataset picks the dataset for an HTTP request: the URL prefix, then the
// dataset header, then the dataset query parameter, then the default.
func (s *Server) dataset(r *http.Request) (string, error) {
	candidates := []string{r.PathValue("dataset")}
	if s.cfg.DatasetHeader != "" {
		candidates = append(candidates, r.Header.Get(s.cfg.DatasetHeader))
	}
	candidates = append(candidates, r.URL.Query().Get("dataset"))
	return s.pickDataset(candidates, fmt.Sprintf("no dataset: use a /{dataset}/api/... URL prefix, the %s header, or a ?dataset= parameter", s.cfg.DatasetHeader))
}

// grpcDataset picks the dataset for a gRPC call. A URL path prefix does
// not survive the gRPC dial (Grafana dials host:port only), so the dataset
// comes from the dataset header forwarded as metadata, a plain "dataset"
// metadata key, or the configured default.
func (s *Server) grpcDataset(ctx context.Context) (string, error) {
	md, _ := metadata.FromIncomingContext(ctx)
	first := func(key string) string {
		if key == "" {
			return ""
		}
		if v := md.Get(key); len(v) > 0 {
			return v[0]
		}
		return ""
	}
	candidates := []string{first(s.cfg.DatasetHeader), first("dataset")}
	return s.pickDataset(candidates, fmt.Sprintf("no dataset: send the %s or dataset gRPC metadata key, or configure a default dataset (a URL path prefix does not reach gRPC)", s.cfg.DatasetHeader))
}

// pickDataset returns the first allowed candidate, else the default.
func (s *Server) pickDataset(candidates []string, missingMsg string) (string, error) {
	for _, v := range candidates {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if !s.cfg.DatasetAllowed(v) {
			return "", &statusError{status: http.StatusBadRequest, msg: fmt.Sprintf("dataset %q is not allowed", v)}
		}
		return v, nil
	}
	if s.cfg.Dataset == "" {
		return "", &statusError{status: http.StatusBadRequest, msg: missingMsg}
	}
	return s.cfg.Dataset, nil
}

// isV2 reports whether the request hit a /api/v2/ endpoint, allowing for a
// dataset prefix.
func isV2(r *http.Request) bool {
	return strings.Contains(r.URL.Path, "/api/v2/")
}

// datasetSchema is a cached mapping for one dataset.
type datasetSchema struct {
	name        string
	mapping     *schema.Mapping
	translator  *translate.Translator
	spanKeys    []string // sampled keys of the span custom map
	resKeys     []string // sampled keys of the resource custom map
	eventKeys   []string // sampled attribute keys inside the events array
	linkKeys    []string // sampled attribute keys inside the links array
	fetchedAt   time.Time
	discovered  bool
	refreshing  bool
	refreshLock sync.Mutex
}

// statusError is an error that already knows the HTTP status it maps to.
// Request validation returns it so the HTTP handlers and the gRPC methods
// can each render it in their own error space.
type statusError struct {
	status int
	msg    string
}

func (e *statusError) Error() string { return e.msg }

func badRequest(format string, args ...any) error {
	return &statusError{status: http.StatusBadRequest, msg: fmt.Sprintf(format, args...)}
}

// DatasetError reports a dataset that cannot be queried at all.
type DatasetError struct {
	Dataset string
	Err     error
}

func (e *DatasetError) Error() string {
	return fmt.Sprintf("dataset %q is not accessible: %v", e.Dataset, e.Err)
}

func (e *DatasetError) Unwrap() error { return e.Err }

type schemaCache struct {
	client  *axiom.Client
	log     *slog.Logger
	refresh time.Duration
	sample  int
	mu      sync.Mutex
	entries map[string]*datasetSchema
}

// get returns the schema for a dataset, discovering it on first use and
// refreshing it in the background when stale.
func (c *schemaCache) get(ctx context.Context, dataset string) (*datasetSchema, error) {
	c.mu.Lock()
	entry, ok := c.entries[dataset]
	c.mu.Unlock()
	if ok {
		if time.Since(entry.fetchedAt) > c.refresh {
			go c.tryRefresh(entry)
		}
		return entry, nil
	}
	entry, err := c.discover(ctx, dataset)
	if err != nil {
		// A rejected probe query means the dataset does not exist or the
		// token cannot read it: surface that instead of guessing.
		var apiErr *axiom.APIError
		if errors.As(err, &apiErr) && (apiErr.Status == http.StatusBadRequest || apiErr.Status == http.StatusForbidden || apiErr.Status == http.StatusNotFound) {
			return nil, &DatasetError{Dataset: dataset, Err: err}
		}
		// Otherwise serve a guessed layout so the proxy still works when
		// discovery is merely unavailable, and keep retrying.
		entry = &datasetSchema{name: dataset, fetchedAt: time.Now()}
		entry.mapping = schema.New(schema.DefaultConfig(), nil)
		entry.translator = translate.New(entry.mapping)
		c.mu.Lock()
		c.entries[dataset] = entry
		c.mu.Unlock()
		return entry, err
	}
	c.mu.Lock()
	c.entries[dataset] = entry
	c.mu.Unlock()
	return entry, nil
}

func (c *schemaCache) tryRefresh(old *datasetSchema) {
	old.refreshLock.Lock()
	if old.refreshing {
		old.refreshLock.Unlock()
		return
	}
	old.refreshing = true
	old.refreshLock.Unlock()
	defer func() {
		old.refreshLock.Lock()
		old.refreshing = false
		old.refreshLock.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	entry, err := c.discover(ctx, old.name)
	if err != nil {
		c.log.Warn("schema refresh failed", "dataset", old.name, "err", err)
		old.fetchedAt = time.Now()
		return
	}
	c.mu.Lock()
	c.entries[old.name] = entry
	c.mu.Unlock()
}

func (c *schemaCache) discover(ctx context.Context, dataset string) (*datasetSchema, error) {
	fields, err := c.client.ListFields(ctx, dataset)
	if err != nil {
		// Query-only tokens cannot read dataset metadata; a sample query
		// still returns every column with its type.
		probed, perr := c.fieldsFromQuery(ctx, dataset)
		if perr != nil {
			return nil, fmt.Errorf("fields endpoint: %v; probe query: %w", err, perr)
		}
		c.log.Info("fields endpoint unavailable; discovered schema from a sample query", "dataset", dataset, "err", err)
		fields = probed
	}
	m := schema.New(schema.DefaultConfig(), fields)
	entry := &datasetSchema{
		name:       dataset,
		mapping:    m,
		translator: translate.New(m),
		fetchedAt:  time.Now(),
		discovered: true,
	}
	cfg := m.Config()
	if m.HasField(cfg.SpanCustomMap) {
		entry.spanKeys = c.sampleKeys(ctx, dataset, cfg.SpanCustomMap)
	}
	if m.HasField(cfg.ResourceCustomMap) {
		entry.resKeys = c.sampleKeys(ctx, dataset, cfg.ResourceCustomMap)
	}
	if attrs, ok := m.ExpandedEventAttributes(); ok {
		entry.eventKeys = c.sampleArrayKeys(ctx, dataset, m, m.Events().Expr, attrs)
	}
	if attrs, ok := m.ExpandedLinkAttributes(); ok {
		entry.linkKeys = c.sampleArrayKeys(ctx, dataset, m, m.Links().Expr, attrs)
	}
	c.log.Info("discovered dataset schema", "dataset", dataset, "fields", len(fields),
		"span_map_keys", len(entry.spanKeys), "resource_map_keys", len(entry.resKeys),
		"event_attr_keys", len(entry.eventKeys), "link_attr_keys", len(entry.linkKeys))
	return entry, nil
}

// fieldsFromQuery derives the dataset schema from the columns of a
// one-row query, merging types from the response's field metadata.
func (c *schemaCache) fieldsFromQuery(ctx context.Context, dataset string) ([]axiom.DatasetField, error) {
	res, err := c.client.Query(ctx, fmt.Sprintf("['%s']\n| limit 1", dataset), axiom.QueryOptions{Start: time.Now().Add(-24 * time.Hour), End: time.Now()})
	if err != nil {
		return nil, err
	}
	t := res.FirstTable()
	if t == nil {
		return nil, fmt.Errorf("probe query returned no table")
	}
	meta := map[string]string{}
	for _, f := range res.FieldsMeta[dataset] {
		meta[f.Name] = f.Type
	}
	out := make([]axiom.DatasetField, 0, len(t.Fields))
	for _, f := range t.Fields {
		typ := f.Type
		if mt, ok := meta[f.Name]; ok && mt != "" && (typ == "" || typ == "unknown") {
			typ = mt
		}
		out = append(out, axiom.DatasetField{Name: f.Name, Type: typ})
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("probe query returned no columns")
	}
	return out, nil
}

// sampleKeys reads map keys from a sample of recent rows.
func (c *schemaCache) sampleKeys(ctx context.Context, dataset, field string) []string {
	q := fmt.Sprintf("['%s']\n| where isnotnull(['%s'])\n| take %d\n| project k = bag_keys(['%s'])", dataset, field, c.sample, field)
	return c.keysFromQuery(ctx, dataset, field, q)
}

// sampleArrayKeys reads the attribute keys carried inside an array column
// (events, links) from a sample of recent rows. The sample is taken before
// the expansion so the cost stays bounded by rows scanned, and mv-expand is
// written without an alias because APL rejects one.
func (c *schemaCache) sampleArrayKeys(ctx context.Context, dataset string, m *schema.Mapping, col, attrs string) []string {
	q := apl.NewQuery(dataset).
		Where(m.SpansOnly()).
		Where(apl.Call("isnotnull", col)).
		Raw(fmt.Sprintf("take %d", c.sample)).
		Raw("mv-expand " + col).
		Project("k = " + apl.Call("bag_keys", attrs))
	return c.keysFromQuery(ctx, dataset, col, q.String())
}

// keysFromQuery runs a key-sampling query and unions the k arrays it returns.
func (c *schemaCache) keysFromQuery(ctx context.Context, dataset, field, q string) []string {
	res, err := c.client.Query(ctx, q, axiom.QueryOptions{Start: time.Now().Add(-24 * time.Hour), End: time.Now()})
	if err != nil {
		c.log.Warn("key sampling failed", "dataset", dataset, "field", field, "err", err)
		return nil
	}
	t := res.FirstTable()
	if t == nil {
		return nil
	}
	seen := map[string]bool{}
	var keys []string
	for _, row := range t.Rows() {
		var ks []string
		if err := row.Unmarshal("k", &ks); err != nil {
			continue
		}
		for _, k := range ks {
			if !seen[k] {
				seen[k] = true
				keys = append(keys, k)
			}
		}
	}
	return keys
}

// --- response helpers ---

func wantsProto(r *http.Request) bool {
	return strings.Contains(r.Header.Get("Accept"), "application/protobuf")
}

// writeMessage encodes a Tempo protobuf message as protobuf or jsonpb
// depending on the Accept header. legacyBatches rewrites the first
// resourceSpans key to batches for the v1 trace endpoint.
func (s *Server) writeMessage(w http.ResponseWriter, r *http.Request, msg proto.Message, legacyBatches bool) {
	if wantsProto(r) {
		b, err := proto.Marshal(msg)
		if err != nil {
			s.writeError(w, http.StatusInternalServerError, "encode response: "+err.Error())
			return
		}
		w.Header().Set("Content-Type", "application/protobuf")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
		return
	}
	var buf bytes.Buffer
	marshaler := jsonpb.Marshaler{}
	if err := marshaler.Marshal(&buf, msg); err != nil {
		s.writeError(w, http.StatusInternalServerError, "encode response: "+err.Error())
		return
	}
	body := buf.Bytes()
	if legacyBatches {
		body = bytes.Replace(body, []byte(`"resourceSpans":`), []byte(`"batches":`), 1)
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (s *Server) writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = fmt.Fprintln(w, msg)
}

// queryErrorStatus maps an error from the query path to an HTTP status.
func queryErrorStatus(err error) int {
	var stErr *statusError
	var dsErr *DatasetError
	var apiErr *axiom.APIError
	var unsupported *metrics.UnsupportedError
	switch {
	case errors.As(err, &stErr):
		return stErr.status
	case errors.As(err, &dsErr):
		return http.StatusNotFound
	case errors.As(err, &unsupported):
		return http.StatusBadRequest
	case errors.Is(err, translate.ErrUnsupported):
		return http.StatusBadRequest
	case errors.As(err, &apiErr):
		switch apiErr.Status {
		case http.StatusBadRequest:
			return http.StatusBadRequest
		case http.StatusTooManyRequests:
			return http.StatusTooManyRequests
		}
		return http.StatusBadGateway
	case errors.Is(err, context.DeadlineExceeded):
		return http.StatusGatewayTimeout
	}
	return http.StatusInternalServerError
}

// writeQueryError maps errors from the query path to HTTP statuses.
func (s *Server) writeQueryError(w http.ResponseWriter, err error) {
	status := queryErrorStatus(err)
	if status == http.StatusGatewayTimeout {
		s.writeError(w, status, "query timed out")
		return
	}
	s.writeError(w, status, err.Error())
}

// logging is request logging middleware.
func (s *Server) logging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		next.ServeHTTP(rec, r)
		s.log.Info("request", "method", r.Method, "path", r.URL.Path, "query", r.URL.RawQuery,
			"status", rec.status, "duration", time.Since(start))
	})
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}
