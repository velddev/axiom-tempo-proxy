package server

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/gogo/protobuf/proto"
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

// droppedTracesMetric is the SearchMetrics.AdditionalMetrics key naming
// candidate traces left out of a search because their spans could not be
// fetched completely.
const droppedTracesMetric = "droppedTraces"

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

// requestContext sets up the per-request dataset, schema, and timeout,
// writing the error response itself when anything fails.
func (s *Server) requestContext(w http.ResponseWriter, r *http.Request) (context.Context, context.CancelFunc, *datasetSchema, bool) {
	dataset, err := s.dataset(r)
	if err != nil {
		s.writeQueryError(w, err)
		return nil, nil, nil, false
	}
	ctx, cancel, ds, err := s.prepare(r.Context(), dataset)
	if err != nil {
		s.writeQueryError(w, err)
		return nil, nil, nil, false
	}
	return ctx, cancel, ds, true
}

// prepare resolves a dataset's schema and applies the query timeout.
func (s *Server) prepare(parent context.Context, dataset string) (context.Context, context.CancelFunc, *datasetSchema, error) {
	ctx, cancel := context.WithTimeout(parent, s.cfg.QueryTimeout)
	ds, err := s.schemas.get(ctx, dataset)
	if ds == nil {
		cancel()
		return nil, nil, nil, err
	}
	if err != nil {
		s.log.Warn("using guessed schema", "dataset", dataset, "err", err)
	}
	return ctx, cancel, ds, nil
}

func (s *Server) fetchOptions(ds *datasetSchema) fetch.Options {
	return fetch.Options{
		Dataset:          ds.name,
		MaxTraces:        s.cfg.MaxSearchTraces,
		MaxSpans:         s.cfg.MaxSpansPerFetch,
		BatchTraces:      s.cfg.SearchBatchTraces,
		DefaultLookback:  s.cfg.DefaultLookback,
		TracePadding:     time.Hour,
		NoPreferSelected: s.cfg.NoPreferSelected,
		Log:              s.log,
		LogQueries:       s.cfg.LogQueries,
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

	trace, st, err := fetch.FetchTrace(ctx, s.client, ds.translator, s.fetchOptions(ds), id, start, end)
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
		res := &tempopb.TraceByIDResponse{Trace: otlp, Metrics: &tempopb.TraceByIDMetrics{}}
		// v1 has no status field, so an incomplete trace can only be
		// reported on v2.
		if st.Partial {
			res.Status = tempopb.PartialStatus_PARTIAL
			res.Message = st.Message
		}
		s.writeMessage(w, r, res, false)
		return
	}
	if st.Partial {
		s.log.Warn("trace returned incomplete", "trace", id, "reason", st.Message)
	}
	s.writeMessage(w, r, otlp, true)
}

// --- search ---

func (s *Server) handleSearch(w http.ResponseWriter, r *http.Request) {
	req, err := searchRequestFromQuery(r)
	if err != nil {
		s.writeQueryError(w, err)
		return
	}
	ctx, cancel, ds, ok := s.requestContext(w, r)
	if !ok {
		return
	}
	defer cancel()
	res, err := s.runSearch(ctx, ds, req)
	if err != nil {
		s.writeQueryError(w, err)
		return
	}
	s.writeMessage(w, r, res, false)
}

// searchRequestFromQuery builds a SearchRequest from the HTTP query
// parameters of /api/search.
func searchRequestFromQuery(r *http.Request) (*tempopb.SearchRequest, error) {
	q := r.URL.Query()
	req := &tempopb.SearchRequest{Query: q.Get("q")}
	tags := q.Get("tags")
	limit, err := parseUint(r, "limit", 20)
	if err != nil {
		return nil, badRequest("%s", err.Error())
	}
	if limit == 0 {
		return nil, badRequest("invalid limit: must be a positive number")
	}
	req.Limit = limit
	if req.SpansPerSpanSet, err = parseUint(r, "spss", 3); err != nil {
		return nil, badRequest("%s", err.Error())
	}
	start, err := parseUnixSeconds(r, "start")
	if err != nil {
		return nil, badRequest("%s", err.Error())
	}
	end, err := parseUnixSeconds(r, "end")
	if err != nil {
		return nil, badRequest("%s", err.Error())
	}
	if !start.IsZero() {
		req.Start = uint32(start.Unix())
	}
	if !end.IsZero() {
		req.End = uint32(end.Unix())
	}
	if req.Query == "" {
		// Legacy tag search, or bare key=value params.
		if tags != "" {
			req.Tags, err = parseLogfmtTags(tags)
			if err != nil {
				return nil, badRequest("%s", err.Error())
			}
		} else if start.IsZero() && end.IsZero() {
			req.Tags = map[string]string{}
			for k, vs := range q {
				switch k {
				case "limit", "spss", "minDuration", "maxDuration", "dataset":
					continue
				}
				if len(vs) > 0 {
					req.Tags[k] = vs[0]
				}
			}
		}
		if v := q.Get("minDuration"); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, badRequest("invalid minDuration: %s", v)
			}
			req.MinDurationMs = uint32(d.Milliseconds())
		}
		if v := q.Get("maxDuration"); v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				return nil, badRequest("invalid maxDuration: %s", v)
			}
			req.MaxDurationMs = uint32(d.Milliseconds())
		}
	} else if tags != "" {
		return nil, badRequest("can't specify tags and q in the same query")
	}
	return req, nil
}

// normalizeSearchRequest validates a SearchRequest from either transport,
// fills in defaults, and rewrites a legacy tag search into TraceQL.
func normalizeSearchRequest(req *tempopb.SearchRequest) error {
	if req.Query != "" && len(req.Tags) > 0 {
		return badRequest("can't specify tags and q in the same query")
	}
	if req.Limit == 0 {
		req.Limit = 20
	}
	if req.SpansPerSpanSet == 0 {
		req.SpansPerSpanSet = 3
	}
	if req.Start != 0 && req.End != 0 && req.End <= req.Start {
		return badRequest("http parameter start must be before end. received start=%d end=%d", req.Start, req.End)
	}
	if req.Query == "" {
		req.Query = legacyToTraceQL(req.Tags,
			time.Duration(req.MinDurationMs)*time.Millisecond,
			time.Duration(req.MaxDurationMs)*time.Millisecond)
	}
	if translate.IsMetricsQuery(req.Query) {
		return badRequest("metrics queries must use /api/metrics/query_range")
	}
	return nil
}

// runSearch answers a search request: it is the shared body of the HTTP
// handler and the streaming Search method.
func (s *Server) runSearch(ctx context.Context, ds *datasetSchema, req *tempopb.SearchRequest) (*tempopb.SearchResponse, error) {
	if err := normalizeSearchRequest(req); err != nil {
		return nil, err
	}
	opts := s.fetchOptions(ds)
	fetcher, err := fetch.NewSearchFetcher(s.client, ds.translator, req.Query, opts)
	if err != nil {
		return nil, badRequest("%s", err.Error())
	}
	// Pull more candidates than the limit when the prefilter is only an
	// approximation, so the engine still has enough traces to fill it.
	want := int(req.Limit)
	if !fetcher.Exact() {
		want = int(req.Limit) * 3
	}
	if want > s.cfg.MaxSearchTraces {
		want = s.cfg.MaxSearchTraces
	}
	fetcher.SetMaxTraces(want)
	if s.cfg.LogQueries {
		s.log.Info("search plan", "query", req.Query, "plan", fetcher.Plan())
	}

	res, err := traceql.NewEngine().ExecuteSearch(ctx, req, fetcher)
	if err != nil {
		return nil, err
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
	stats := fetcher.Stats()
	res.Metrics.CompletedJobs, res.Metrics.TotalJobs = 1, 1
	res.Metrics.InspectedTraces = uint32(stats.CandidateTraces)
	res.Metrics.InspectedSpans = uint64(stats.SpansFetched)
	// SearchResponse has no status field, so a truncated span pull is
	// reported as an additional metric (and logged) instead of silently
	// returning fewer traces.
	if stats.DroppedTraces > 0 {
		if res.Metrics.AdditionalMetrics == nil {
			res.Metrics.AdditionalMetrics = map[string]int64{}
		}
		res.Metrics.AdditionalMetrics[droppedTracesMetric] = int64(stats.DroppedTraces)
		s.log.Warn("search result incomplete: span budget exhausted",
			"query", req.Query, "droppedTraces", stats.DroppedTraces,
			"candidateTraces", stats.CandidateTraces, "spansFetched", stats.SpansFetched,
			"maxSpansPerFetch", s.cfg.MaxSpansPerFetch)
	}
	return res, nil
}

// legacyToTraceQL converts Tempo's tag-based search into TraceQL. Tempo
// matched tag values as case-insensitive substrings.
func legacyToTraceQL(tags map[string]string, minDuration, maxDuration time.Duration) string {
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
	if minDuration > 0 {
		parts = append(parts, fmt.Sprintf("duration >= %dms", minDuration.Milliseconds()))
	}
	if maxDuration > 0 {
		parts = append(parts, fmt.Sprintf("duration <= %dms", maxDuration.Milliseconds()))
	}
	if len(parts) == 0 {
		return "{}"
	}
	return "{ " + strings.Join(parts, " && ") + " }"
}

// --- tags ---

func (s *Server) handleSearchTags(w http.ResponseWriter, r *http.Request) {
	scope := r.URL.Query().Get("scope")
	limit, err := parseUint(r, "limit", 0)
	if err != nil {
		s.writeQueryError(w, badRequest("%s", err.Error()))
		return
	}
	if _, err := parseTagScope(scope); err != nil {
		s.writeQueryError(w, err)
		return
	}

	_, cancel, ds, ok := s.requestContext(w, r)
	if !ok {
		return
	}
	defer cancel()

	var res proto.Message
	if isV2(r) {
		res, err = s.runSearchTagsV2(ds, scope, limit)
	} else {
		res, err = s.runSearchTags(ds, scope, limit)
	}
	if err != nil {
		s.writeQueryError(w, err)
		return
	}
	s.writeMessage(w, r, res, false)
}

func parseTagScope(scope string) (traceql.AttributeScope, error) {
	parsed := traceql.AttributeScopeFromString(scope)
	if parsed == traceql.AttributeScopeUnknown {
		return parsed, badRequest("invalid scope: %s", scope)
	}
	return parsed, nil
}

// tagLists returns the tag names of a dataset, by scope.
func (s *Server) tagLists(ds *datasetSchema) (resTags, spanTags, eventTags, linkTags []string) {
	m := ds.mapping
	spanTags = uniqueSorted(append(m.TagNames(traceql.AttributeScopeSpan), ds.spanKeys...))
	resTags = uniqueSorted(append(m.TagNames(traceql.AttributeScopeResource), ds.resKeys...))
	if !m.Discovered() {
		resTags = uniqueSorted(append(resTags, "service.name"))
	}
	// Event and link attributes are not columns: their keys are sampled
	// out of the arrays at schema discovery, and stay empty for a dataset
	// that has no such array at all.
	eventTags = uniqueSorted(ds.eventKeys)
	linkTags = uniqueSorted(ds.linkKeys)
	return resTags, spanTags, eventTags, linkTags
}

func capTags(tags []string, limit uint32) []string {
	if limit > 0 && len(tags) > int(limit) {
		return tags[:limit]
	}
	return tags
}

// runSearchTagsV2 is the shared body of GET /api/v2/search/tags and the
// streaming SearchTagsV2 method.
func (s *Server) runSearchTagsV2(ds *datasetSchema, scopeParam string, limit uint32) (*tempopb.SearchTagsV2Response, error) {
	scope, err := parseTagScope(scopeParam)
	if err != nil {
		return nil, err
	}
	resTags, spanTags, eventTags, linkTags := s.tagLists(ds)
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
		res.Scopes = append(res.Scopes, &tempopb.SearchTagsV2Scope{Name: name, Tags: capTags(tags, limit)})
	}
	add("resource", resTags)
	add("span", spanTags)
	add("event", eventTags)
	add("link", linkTags)
	add("intrinsic", intrinsicTags)
	return res, nil
}

// runSearchTags is the shared body of GET /api/search/tags and the
// streaming SearchTags method.
func (s *Server) runSearchTags(ds *datasetSchema, scopeParam string, limit uint32) (*tempopb.SearchTagsResponse, error) {
	scope, err := parseTagScope(scopeParam)
	if err != nil {
		return nil, err
	}
	resTags, spanTags, _, _ := s.tagLists(ds)
	var all []string
	if scope == traceql.AttributeScopeNone || scope == traceql.AttributeScopeResource {
		all = append(all, resTags...)
	}
	if scope == traceql.AttributeScopeNone || scope == traceql.AttributeScopeSpan {
		all = append(all, spanTags...)
	}
	return &tempopb.SearchTagsResponse{TagNames: capTags(uniqueSorted(all), limit), Metrics: &tempopb.MetadataMetrics{}}, nil
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

	limit, err := parseUint(r, "limit", 0)
	if err != nil {
		s.writeQueryError(w, badRequest("%s", err.Error()))
		return
	}
	start, _ := parseUnixSeconds(r, "start")
	end, _ := parseUnixSeconds(r, "end")

	ctx, cancel, ds, ok := s.requestContext(w, r)
	if !ok {
		return
	}
	defer cancel()

	filter := r.URL.Query().Get("q")
	var res proto.Message
	if v2 {
		res, err = s.runSearchTagValuesV2(ctx, ds, tagName, filter, limit, start, end)
	} else {
		res, err = s.runSearchTagValues(ctx, ds, tagName, filter, limit, start, end)
	}
	if err != nil {
		s.writeQueryError(w, err)
		return
	}
	s.writeMessage(w, r, res, false)
}

// tagValueAttribute parses a tag name. v2 accepts a scoped identifier;
// v1 takes the name as-is, except for event attributes, which have no
// unscoped spelling, so the scoped one (event.exception.message,
// event:name) is accepted on v1 too.
func tagValueAttribute(tagName string, v2 bool) (traceql.Attribute, error) {
	if v2 {
		a, err := traceql.ParseIdentifier(tagName)
		if err != nil {
			return a, badRequest("invalid tag name: %s", tagName)
		}
		return a, nil
	}
	if a, err := traceql.ParseIdentifier(tagName); err == nil {
		if _, isEvent := eventAttribute(a); isEvent {
			return a, nil
		}
	}
	return traceql.NewAttribute(tagName), nil
}

// tagValueBounds applies the configured caps and default window.
func (s *Server) tagValueBounds(limit uint32, start, end time.Time) (int, time.Time, time.Time) {
	if limit == 0 || int(limit) > s.cfg.MaxTagValues {
		limit = uint32(s.cfg.MaxTagValues)
	}
	if start.IsZero() || end.IsZero() {
		end = time.Now()
		start = end.Add(-s.cfg.DefaultLookback)
	}
	return int(limit), start, end
}

// runSearchTagValuesV2 is the shared body of the v2 tag values endpoint
// and the streaming SearchTagValuesV2 method.
func (s *Server) runSearchTagValuesV2(ctx context.Context, ds *datasetSchema, tagName, filter string, limit uint32, start, end time.Time) (*tempopb.SearchTagValuesV2Response, error) {
	attr, err := tagValueAttribute(tagName, true)
	if err != nil {
		return nil, err
	}
	n, start, end := s.tagValueBounds(limit, start, end)
	values, err := s.tagValues(ctx, ds, attr, filter, n, start, end)
	if err != nil {
		return nil, err
	}
	res := &tempopb.SearchTagValuesV2Response{Metrics: &tempopb.MetadataMetrics{}}
	for _, v := range values {
		res.TagValues = append(res.TagValues, &tempopb.TagValue{Type: staticTypeName(v), Value: v.EncodeToString(false)})
	}
	return res, nil
}

// runSearchTagValues is the shared body of the v1 tag values endpoint and
// the streaming SearchTagValues method.
func (s *Server) runSearchTagValues(ctx context.Context, ds *datasetSchema, tagName, filter string, limit uint32, start, end time.Time) (*tempopb.SearchTagValuesResponse, error) {
	attr, _ := tagValueAttribute(tagName, false)
	n, start, end := s.tagValueBounds(limit, start, end)
	values, err := s.tagValues(ctx, ds, attr, filter, n, start, end)
	if err != nil {
		return nil, err
	}
	res := &tempopb.SearchTagValuesResponse{Metrics: &tempopb.MetadataMetrics{}}
	for _, v := range values {
		res.TagValues = append(res.TagValues, v.EncodeToString(false))
	}
	return res, nil
}

// tagValues lists distinct values of an attribute, most frequent first.
// The q filter is applied when it parses and translates; otherwise the
// unfiltered list is returned, as Tempo does for incomplete queries.
func (s *Server) tagValues(ctx context.Context, ds *datasetSchema, attr traceql.Attribute, filter string, limit int, start, end time.Time) ([]traceql.Static, error) {
	m := ds.mapping

	// Event attributes are not per-span columns: their values live inside
	// the events array, so those rows are expanded and grouped instead.
	eventKey, isEvent := eventAttribute(attr)
	var valueExpr string
	if isEvent {
		expr, ok := m.ExpandedEvent(eventKey)
		if !ok {
			return nil, nil
		}
		// APL refuses to group by a value of unknown type, which is what
		// indexing into a dynamic yields, so every event value is a string.
		valueExpr = apl.Call("tostring", expr)
	} else {
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
		valueExpr = col.Expr
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
	if isEvent {
		// The q filter is pushed down ahead of the expansion, where the
		// per-slot event expressions it may carry are still meaningful.
		events := m.Events().Expr
		q.Where(apl.Call("isnotnull", events)).
			Raw("mv-expand "+events).
			Extend("v = "+valueExpr).
			Where(apl.Call("isnotempty", "v")).
			Summarize([]string{"c = count()"}, []string{"v"})
	} else {
		q.Where(apl.Call("isnotnull", valueExpr)).
			Summarize([]string{"c = count()"}, []string{"v = " + valueExpr})
	}
	q.Top(limit, "c")
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

// eventAttribute reports whether an attribute addresses span event data,
// returning the event attribute key, empty for the event's own name
// (event:name). Event intrinsics other than the name, such as
// event:timeSinceStart, are not event attributes here: they are not
// derivable from the stored elements.
func eventAttribute(a traceql.Attribute) (string, bool) {
	if a.Parent {
		return "", false
	}
	if a.Intrinsic == traceql.IntrinsicEventName {
		return "", true
	}
	if a.Intrinsic == traceql.IntrinsicNone && a.Scope == traceql.AttributeScopeEvent {
		return a.Name, true
	}
	return "", false
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

// metricsRequestFromQuery builds a metrics request from the HTTP query
// parameters of /api/metrics/query_range and /api/metrics/query.
func metricsRequestFromQuery(q url.Values, instant bool) (metrics.Request, error) {
	query := q.Get("query")
	if v := q.Get("q"); v != "" {
		query = v
	}
	if query == "" {
		return metrics.Request{}, badRequest("missing q parameter")
	}
	end, hasEnd, err := parseTimestamp(q.Get("end"))
	if err != nil {
		return metrics.Request{}, badRequest("%s", err.Error())
	}
	start, hasStart, err := parseTimestamp(q.Get("start"))
	if err != nil {
		return metrics.Request{}, badRequest("%s", err.Error())
	}
	since, err := parsePromDuration(q.Get("since"))
	if err != nil {
		return metrics.Request{}, badRequest("%s", err.Error())
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
	// `exemplars` is how many exemplars the caller wants per series.
	// Tempo treats an absent or unparsable value as 0 and then, unless
	// the query carries a with(exemplars=...) hint, replaces 0 with its
	// configured max_exemplars; the evaluator does that second step.
	// Neither Grafana's Tempo datasource nor Traces Drilldown sends the
	// parameter, so the default is what puts dots on their panels.
	exemplars, err := parseUintValue(q, "exemplars", 0)
	if err != nil {
		return metrics.Request{}, badRequest("%s", err.Error())
	}
	req := metrics.Request{
		Query:     query,
		StartNs:   uint64(start.UnixNano()),
		EndNs:     uint64(end.UnixNano()),
		Exemplars: int(exemplars),
	}
	if !instant {
		step, err := parseStep(q.Get("step"))
		if err != nil {
			return metrics.Request{}, badRequest("%s", err.Error())
		}
		req.StepNs = uint64(step)
	}
	return req, normalizeMetricsRequest(&req, instant)
}

// normalizeMetricsRequest validates a metrics request from either
// transport and fills in the default window and step.
func normalizeMetricsRequest(req *metrics.Request, instant bool) error {
	if req.Query == "" {
		return badRequest("missing q parameter")
	}
	if req.EndNs == 0 {
		req.EndNs = uint64(time.Now().UnixNano())
	}
	if req.StartNs == 0 {
		req.StartNs = req.EndNs - uint64(time.Hour)
	}
	if req.EndNs <= req.StartNs {
		return badRequest("http parameter start must be before end")
	}
	if instant {
		req.StepNs = 0
		return nil
	}
	if req.StepNs == 0 {
		req.StepNs = metrics.DefaultStep(req.StartNs, req.EndNs)
	}
	return nil
}

func (s *Server) evaluator(ds *datasetSchema) *metrics.Evaluator {
	return metrics.New(s.client, ds.translator, metrics.Options{
		Dataset:          ds.name,
		DefaultExemplars: s.cfg.DefaultExemplars,
		MaxExemplars:     s.cfg.MaxExemplars,
		Log:              s.log,
		LogQueries:       s.cfg.LogQueries,
	})
}

func (s *Server) handleMetricsQueryRange(w http.ResponseWriter, r *http.Request) {
	req, err := metricsRequestFromQuery(r.URL.Query(), false)
	if err != nil {
		s.writeQueryError(w, err)
		return
	}
	ctx, cancel, ds, ok := s.requestContext(w, r)
	if !ok {
		return
	}
	defer cancel()
	res, err := s.runMetricsQueryRange(ctx, ds, req)
	if err != nil {
		s.writeQueryError(w, err)
		return
	}
	s.writeMessage(w, r, res, false)
}

func (s *Server) handleMetricsQueryInstant(w http.ResponseWriter, r *http.Request) {
	req, err := metricsRequestFromQuery(r.URL.Query(), true)
	if err != nil {
		s.writeQueryError(w, err)
		return
	}
	ctx, cancel, ds, ok := s.requestContext(w, r)
	if !ok {
		return
	}
	defer cancel()
	res, err := s.runMetricsQueryInstant(ctx, ds, req)
	if err != nil {
		s.writeQueryError(w, err)
		return
	}
	s.writeMessage(w, r, res, false)
}

// runMetricsQueryRange is the shared body of GET /api/metrics/query_range
// and the streaming MetricsQueryRange method.
func (s *Server) runMetricsQueryRange(ctx context.Context, ds *datasetSchema, req metrics.Request) (*tempopb.QueryRangeResponse, error) {
	if err := normalizeMetricsRequest(&req, false); err != nil {
		return nil, err
	}
	return s.evaluator(ds).QueryRange(ctx, req)
}

// runMetricsQueryInstant is the shared body of GET /api/metrics/query and
// the streaming MetricsQueryInstant method.
func (s *Server) runMetricsQueryInstant(ctx context.Context, ds *datasetSchema, req metrics.Request) (*tempopb.QueryInstantResponse, error) {
	if err := normalizeMetricsRequest(&req, true); err != nil {
		return nil, err
	}
	return s.evaluator(ds).QueryInstant(ctx, req)
}

var _ = axiom.DefaultBaseURL
