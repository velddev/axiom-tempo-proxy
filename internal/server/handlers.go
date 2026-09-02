package server

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/grafana/tempo/pkg/tempopb"
	"github.com/grafana/tempo/pkg/traceql"

	"github.com/velddev/axiom-tempo-proxy/internal/apl"
	"github.com/velddev/axiom-tempo-proxy/internal/axiom"
	"github.com/velddev/axiom-tempo-proxy/internal/convert"
	"github.com/velddev/axiom-tempo-proxy/internal/fetch"
	"github.com/velddev/axiom-tempo-proxy/internal/metrics"
	"github.com/velddev/axiom-tempo-proxy/internal/spans"
	"github.com/velddev/axiom-tempo-proxy/internal/translate"
)

var hexRe = regexp.MustCompile(`^[0-9A-Fa-f]+$`)

// intrinsicTags is what Tempo lists under the intrinsic scope.
var intrinsicTags = []string{
	"duration", "event:name", "event:timeSinceStart", "instrumentation:name", "instrumentation:version",
	"kind", "name", "rootName", "rootServiceName", "span:duration", "span:kind", "span:name",
	"span:status", "span:statusMessage", "status", "statusMessage", "trace:duration",
	"trace:rootName", "trace:rootService", "traceDuration",
}

func (s *Server) handleEcho(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte("echo"))
}

func (s *Server) handleBuildInfo(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"version":"2.9.0","revision":"axiom-tempo-proxy-%s","branch":"main","buildDate":"","buildUser":"","goVersion":"%s"}`, Version, runtime.Version())
}

// requestContext sets up the per-request dataset, schema, and timeout.
func (s *Server) requestContext(w http.ResponseWriter, r *http.Request) (context.Context, context.CancelFunc, *datasetSchema, bool) {
	dataset, err := s.dataset(r)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return nil, nil, nil, false
	}
	ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
	ds, err := s.schemas.get(ctx, dataset)
	if ds == nil {
		cancel()
		var dsErr *DatasetError
		if errors.As(err, &dsErr) {
			s.writeError(w, http.StatusNotFound, err.Error())
		} else {
			s.writeQueryError(w, err)
		}
		return nil, nil, nil, false
	}
	if err != nil {
		s.log.Warn("using guessed schema", "dataset", dataset, "err", err)
	}
	return ctx, cancel, ds, true
}

func (s *Server) fetchOptions(ds *datasetSchema) fetch.Options {
	return fetch.Options{
		Dataset:         ds.name,
		MaxTraces:       s.cfg.MaxSearchTraces,
		MaxSpans:        s.cfg.MaxSpansPerFetch,
		DefaultLookback: s.cfg.DefaultLookback,
		TracePadding:    time.Hour,
		Log:             s.log,
		LogQueries:      s.cfg.LogQueries,
	}
}

// --- trace by id ---

func (s *Server) handleTraceByID(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("traceID")
	if !hexRe.MatchString(id) {
		s.writeError(w, http.StatusBadRequest, "invalid trace id: "+id)
		return
	}
	start, err := parseUnixSeconds(r, "start")
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	end, err := parseUnixSeconds(r, "end")
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !start.IsZero() && !end.IsZero() && !end.After(start) {
		s.writeError(w, http.StatusBadRequest, "http parameter start must be before end")
		return
	}
	if start.IsZero() || end.IsZero() {
		end = time.Now()
		start = end.Add(-s.cfg.TraceLookback)
	}

	ctx, cancel, ds, ok := s.requestContext(w, r)
	if !ok {
		return
	}
	defer cancel()

	trace, err := fetch.FetchTrace(ctx, s.client, ds.translator, s.fetchOptions(ds), id, start, end)
	if err != nil {
		s.writeQueryError(w, err)
		return
	}
	if trace == nil {
		s.writeError(w, http.StatusNotFound, "trace not found")
		return
	}
	otlp := convert.ToOTLP(trace)
	if isV2(r) {
		s.writeMessage(w, r, &tempopb.TraceByIDResponse{Trace: otlp, Metrics: &tempopb.TraceByIDMetrics{}}, false)
		return
	}
	s.writeMessage(w, r, otlp, true)
}

// --- search ---

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	query := q.Get("q")
	tags := q.Get("tags")
	if query != "" && tags != "" {
		s.writeError(w, http.StatusBadRequest, "can't specify tags and q in the same query")
		return
	}
	limit, err := parseUint(r, "limit", 20)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if limit == 0 {
		s.writeError(w, http.StatusBadRequest, "invalid limit: must be a positive number")
		return
	}
	spss, err := parseUint(r, "spss", 3)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	start, err := parseUnixSeconds(r, "start")
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	end, err := parseUnixSeconds(r, "end")
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if !start.IsZero() && !end.IsZero() && !end.After(start) {
		s.writeError(w, http.StatusBadRequest, fmt.Sprintf("http parameter start must be before end. received start=%d end=%d", start.Unix(), end.Unix()))
		return
	}

	if query == "" {
		// Legacy tag search, or bare key=value params.
		tagMap := map[string]string{}
		if tags != "" {
			tagMap, err = parseLogfmtTags(tags)
			if err != nil {
				s.writeError(w, http.StatusBadRequest, err.Error())
				return
			}
		} else if start.IsZero() && end.IsZero() {
			for k, vs := range q {
				switch k {
				case "limit", "spss", "minDuration", "maxDuration":
					continue
				}
				if len(vs) > 0 {
					tagMap[k] = vs[0]
				}
			}
		}
		query, err = legacyToTraceQL(tagMap, q.Get("minDuration"), q.Get("maxDuration"))
		if err != nil {
			s.writeError(w, http.StatusBadRequest, err.Error())
			return
		}
	}
	if translate.IsMetricsQuery(query) {
		s.writeError(w, http.StatusBadRequest, "metrics queries must use /api/metrics/query_range")
		return
	}

	ctx, cancel, ds, ok := s.requestContext(w, r)
	if !ok {
		return
	}
	defer cancel()

	opts := s.fetchOptions(ds)
	fetcher, err := fetch.NewSearchFetcher(s.client, ds.translator, query, opts)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	// Pull more candidates than the limit when the prefilter is only an
	// approximation, so the engine still has enough traces to fill it.
	want := int(limit)
	if !fetcher.Exact() {
		want = int(limit) * 3
	}
	if want > s.cfg.MaxSearchTraces {
		want = s.cfg.MaxSearchTraces
	}
	fetcher.SetMaxTraces(want)
	if s.cfg.LogQueries {
		s.log.Info("search plan", "query", query, "plan", fetcher.Plan())
	}

	req := &tempopb.SearchRequest{Query: query, Limit: limit, SpansPerSpanSet: spss}
	if !start.IsZero() {
		req.Start = uint32(start.Unix())
	}
	if !end.IsZero() {
		req.End = uint32(end.Unix())
	}
	res, err := traceql.NewEngine().ExecuteSearch(ctx, req, fetcher)
	if err != nil {
		s.writeQueryError(w, err)
		return
	}
	if res.Metrics == nil {
		res.Metrics = &tempopb.SearchMetrics{}
	}
	// Tempo's engine strips leading zeros from trace ids and its frontend
	// pads them back to 32 hex chars; do the same.
	for _, tr := range res.Traces {
		if n := len(tr.TraceID); n > 0 && n < 32 {
			tr.TraceID = strings.Repeat("0", 32-n) + tr.TraceID
		}
	}
	res.Metrics.CompletedJobs, res.Metrics.TotalJobs = 1, 1
	res.Metrics.InspectedTraces = uint32(fetcher.Stats().CandidateTraces)
	res.Metrics.InspectedSpans = uint64(fetcher.Stats().SpansFetched)
	s.writeMessage(w, r, res, false)
}

// legacyToTraceQL converts Tempo's tag-based search into TraceQL. Tempo
// matched tag values as case-insensitive substrings.
func legacyToTraceQL(tags map[string]string, minDuration, maxDuration string) (string, error) {
	keys := make([]string, 0, len(tags))
	for k := range tags {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var parts []string
	for _, k := range keys {
		v := tags[k]
		attr := k
		if !strings.HasPrefix(attr, ".") && !strings.HasPrefix(attr, "resource.") && !strings.HasPrefix(attr, "span.") {
			switch attr {
			case "name", "status", "kind", "duration", "statusMessage":
			default:
				attr = "." + attr
			}
		}
		parts = append(parts, fmt.Sprintf(`%s =~ "(?i).*%s.*"`, attr, regexp.QuoteMeta(v)))
	}
	if minDuration != "" {
		d, err := time.ParseDuration(minDuration)
		if err != nil {
			return "", fmt.Errorf("invalid minDuration: %s", minDuration)
		}
		parts = append(parts, fmt.Sprintf("duration >= %dms", d.Milliseconds()))
	}
	if maxDuration != "" {
		d, err := time.ParseDuration(maxDuration)
		if err != nil {
			return "", fmt.Errorf("invalid maxDuration: %s", maxDuration)
		}
		parts = append(parts, fmt.Sprintf("duration <= %dms", d.Milliseconds()))
	}
	if len(parts) == 0 {
		return "{}", nil
	}
	return "{ " + strings.Join(parts, " && ") + " }", nil
}

// --- tags ---

func (s *Server) handleSearchTags(w http.ResponseWriter, r *http.Request) {
	scopeParam := r.URL.Query().Get("scope")
	scope := traceql.AttributeScopeFromString(scopeParam)
	if scope == traceql.AttributeScopeUnknown {
		s.writeError(w, http.StatusBadRequest, "invalid scope: "+scopeParam)
		return
	}
	limit, err := parseUint(r, "limit", 0)
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	ctx, cancel, ds, ok := s.requestContext(w, r)
	if !ok {
		return
	}
	defer cancel()
	_ = ctx

	m := ds.mapping
	spanTags := uniqueSorted(append(m.TagNames(traceql.AttributeScopeSpan), ds.spanKeys...))
	resTags := uniqueSorted(append(m.TagNames(traceql.AttributeScopeResource), ds.resKeys...))
	if !m.Discovered() {
		resTags = uniqueSorted(append(resTags, "service.name"))
	}
	cap_ := func(tags []string) []string {
		if limit > 0 && len(tags) > int(limit) {
			return tags[:limit]
		}
		return tags
	}

	if isV2(r) {
		res := &tempopb.SearchTagsV2Response{Metrics: &tempopb.MetadataMetrics{}}
		add := func(name string, tags []string) {
			if scope != traceql.AttributeScopeNone && scope.String() != name {
				return
			}
			// A scope without tags is omitted, as Tempo does. jsonpb drops
			// empty lists, and Grafana's datasource calls tags.filter() on
			// every scope, which throws on a missing list.
			if len(tags) == 0 {
				return
			}
			res.Scopes = append(res.Scopes, &tempopb.SearchTagsV2Scope{Name: name, Tags: cap_(tags)})
		}
		add("resource", resTags)
		add("span", spanTags)
		add("intrinsic", intrinsicTags)
		s.writeMessage(w, r, res, false)
		return
	}

	var all []string
	if scope == traceql.AttributeScopeNone || scope == traceql.AttributeScopeResource {
		all = append(all, resTags...)
	}
	if scope == traceql.AttributeScopeNone || scope == traceql.AttributeScopeSpan {
		all = append(all, spanTags...)
	}
	res := &tempopb.SearchTagsResponse{TagNames: cap_(uniqueSorted(all)), Metrics: &tempopb.MetadataMetrics{}}
	s.writeMessage(w, r, res, false)
}

func uniqueSorted(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// --- tag values ---

func (s *Server) handleSearchTagValues(w http.ResponseWriter, r *http.Request) {
	rest := r.PathValue("tagName")
	if !strings.HasSuffix(rest, "/values") {
		http.NotFound(w, r)
		return
	}
	tagName := strings.TrimSuffix(rest, "/values")
	v2 := isV2(r)

	var attr traceql.Attribute
	if v2 {
		a, err := traceql.ParseIdentifier(tagName)
		if err != nil {
			s.writeError(w, http.StatusBadRequest, "invalid tag name: "+tagName)
			return
		}
		attr = a
	} else {
		attr = traceql.NewAttribute(tagName)
	}
	limit, err := parseUint(r, "limit", uint32(s.cfg.MaxTagValues))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if limit == 0 || int(limit) > s.cfg.MaxTagValues {
		limit = uint32(s.cfg.MaxTagValues)
	}
	start, _ := parseUnixSeconds(r, "start")
	end, _ := parseUnixSeconds(r, "end")
	if start.IsZero() || end.IsZero() {
		end = time.Now()
		start = end.Add(-s.cfg.DefaultLookback)
	}

	ctx, cancel, ds, ok := s.requestContext(w, r)
	if !ok {
		return
	}
	defer cancel()

	values, err := s.tagValues(ctx, ds, attr, r.URL.Query().Get("q"), int(limit), start, end)
	if err != nil {
		s.writeQueryError(w, err)
		return
	}
	if v2 {
		res := &tempopb.SearchTagValuesV2Response{Metrics: &tempopb.MetadataMetrics{}}
		for _, v := range values {
			res.TagValues = append(res.TagValues, &tempopb.TagValue{Type: staticTypeName(v), Value: v.EncodeToString(false)})
		}
		s.writeMessage(w, r, res, false)
		return
	}
	res := &tempopb.SearchTagValuesResponse{Metrics: &tempopb.MetadataMetrics{}}
	for _, v := range values {
		res.TagValues = append(res.TagValues, v.EncodeToString(false))
	}
	s.writeMessage(w, r, res, false)
}

// tagValues lists distinct values of an attribute, most frequent first.
// The q filter is applied when it parses and translates; otherwise the
// unfiltered list is returned, as Tempo does for incomplete queries.
func (s *Server) tagValues(ctx context.Context, ds *datasetSchema, attr traceql.Attribute, filter string, limit int, start, end time.Time) ([]traceql.Static, error) {
	m := ds.mapping
	col, ok := m.Resolve(attr)
	if !ok {
		switch attr.Intrinsic {
		case traceql.IntrinsicTraceRootService, traceql.ScopedIntrinsicTraceRootService:
			col = m.ServiceName()
			ok = true
		case traceql.IntrinsicTraceRootSpan, traceql.ScopedIntrinsicTraceRootName:
			col = m.Name()
			ok = true
		}
	}
	if !ok || col.Missing {
		return nil, nil
	}

	q := apl.NewQuery(ds.name).Where(m.SpansOnly())
	if strings.TrimSpace(filter) != "" {
		if expr, err := translate.ParseFilter(filter); err == nil {
			if f := ds.translator.Filter(expr); f.Where != "" {
				q.Where(f.Where)
			}
		} else if s.cfg.LogQueries {
			s.log.Debug("ignoring unparsable tag value filter", "q", filter, "err", err)
		}
	}
	q.Where(apl.Call("isnotnull", col.Expr)).
		Summarize([]string{"c = count()"}, []string{"v = " + col.Expr}).
		Top(limit, "c")
	query := q.String()
	if s.cfg.LogQueries {
		s.log.Info("axiom query", "apl", query)
	}
	res, err := s.client.Query(ctx, query, axiom.QueryOptions{Start: start, End: end})
	if err != nil {
		return nil, fmt.Errorf("axiom query failed: %w", err)
	}
	t := res.FirstTable()
	if t == nil {
		return nil, nil
	}
	seen := map[string]bool{}
	var out []traceql.Static
	for _, row := range t.Rows() {
		raw := row.Raw("v")
		if raw == nil {
			continue
		}
		st, ok := spans.StaticFromJSON(raw)
		if !ok {
			continue
		}
		st = normaliseIntrinsicValue(attr, st)
		key := st.EncodeToString(true)
		if seen[key] {
			continue
		}
		seen[key] = true
		out = append(out, st)
	}
	return out, nil
}

func normaliseIntrinsicValue(attr traceql.Attribute, st traceql.Static) traceql.Static {
	if st.Type != traceql.TypeString {
		return st
	}
	s := strings.ToLower(st.EncodeToString(false))
	switch attr.Intrinsic {
	case traceql.IntrinsicStatus, traceql.ScopedIntrinsicSpanStatus:
		switch strings.TrimPrefix(s, "status_code_") {
		case "error":
			return traceql.NewStaticStatus(traceql.StatusError)
		case "ok":
			return traceql.NewStaticStatus(traceql.StatusOk)
		}
		return traceql.NewStaticStatus(traceql.StatusUnset)
	case traceql.IntrinsicKind, traceql.ScopedIntrinsicSpanKind:
		k := strings.TrimPrefix(s, "span_kind_")
		for _, kind := range []traceql.Kind{traceql.KindServer, traceql.KindClient, traceql.KindProducer, traceql.KindConsumer, traceql.KindInternal} {
			if kind.String() == k {
				return traceql.NewStaticKind(kind)
			}
		}
		return traceql.NewStaticKind(traceql.KindUnspecified)
	}
	return st
}

func staticTypeName(st traceql.Static) string {
	switch st.Type {
	case traceql.TypeInt:
		return "int"
	case traceql.TypeFloat:
		return "float"
	case traceql.TypeBoolean:
		return "bool"
	case traceql.TypeDuration:
		return "duration"
	case traceql.TypeStatus:
		return "status"
	case traceql.TypeKind:
		return "kind"
	}
	return "string"
}

// --- metrics ---

func (s *Server) metricsRequest(w http.ResponseWriter, r *http.Request, instant bool) (metrics.Request, bool) {
	q := r.URL.Query()
	query := q.Get("query")
	if v := q.Get("q"); v != "" {
		query = v
	}
	if query == "" {
		s.writeError(w, http.StatusBadRequest, "missing q parameter")
		return metrics.Request{}, false
	}
	end, hasEnd, err := parseTimestamp(q.Get("end"))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return metrics.Request{}, false
	}
	start, hasStart, err := parseTimestamp(q.Get("start"))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return metrics.Request{}, false
	}
	since, err := parsePromDuration(q.Get("since"))
	if err != nil {
		s.writeError(w, http.StatusBadRequest, err.Error())
		return metrics.Request{}, false
	}
	if since == 0 {
		since = time.Hour
	}
	if !hasEnd {
		end = time.Now()
	}
	if !hasStart {
		start = end.Add(-since)
	}
	if !end.After(start) {
		s.writeError(w, http.StatusBadRequest, "http parameter start must be before end")
		return metrics.Request{}, false
	}
	req := metrics.Request{Query: query, StartNs: uint64(start.UnixNano()), EndNs: uint64(end.UnixNano())}
	if !instant {
		step, err := parseStep(q.Get("step"))
		if err != nil {
			s.writeError(w, http.StatusBadRequest, err.Error())
			return metrics.Request{}, false
		}
		if step == 0 {
			req.StepNs = metrics.DefaultStep(req.StartNs, req.EndNs)
		} else {
			req.StepNs = uint64(step)
		}
	}
	return req, true
}

func (s *Server) evaluator(ds *datasetSchema) *metrics.Evaluator {
	return metrics.New(s.client, ds.translator, metrics.Options{
		Dataset:    ds.name,
		Log:        s.log,
		LogQueries: s.cfg.LogQueries,
	})
}

func (s *Server) handleMetricsQueryRange(w http.ResponseWriter, r *http.Request) {
	req, ok := s.metricsRequest(w, r, false)
	if !ok {
		return
	}
	ctx, cancel, ds, ok := s.requestContext(w, r)
	if !ok {
		return
	}
	defer cancel()
	res, err := s.evaluator(ds).QueryRange(ctx, req)
	if err != nil {
		s.writeQueryError(w, err)
		return
	}
	s.writeMessage(w, r, res, false)
}

func (s *Server) handleMetricsQueryInstant(w http.ResponseWriter, r *http.Request) {
	req, ok := s.metricsRequest(w, r, true)
	if !ok {
		return
	}
	ctx, cancel, ds, ok := s.requestContext(w, r)
	if !ok {
		return
	}
	defer cancel()
	res, err := s.evaluator(ds).QueryInstant(ctx, req)
	if err != nil {
		s.writeQueryError(w, err)
		return
	}
	s.writeMessage(w, r, res, false)
}

var _ = axiom.DefaultBaseURL
