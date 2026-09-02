# Tempo Query-Frontend HTTP API — Implementation Reference

Research compiled 2026-09-02 by reading Tempo, grafana-tempo-datasource, and traces-drilldown sources directly, plus the public docs at https://grafana.com/docs/tempo/latest/api_docs/, https://grafana.com/docs/tempo/latest/traceql/construct-traceql-queries/, https://grafana.com/docs/tempo/latest/metrics-from-traces/metrics-queries/functions/.

Note: the Grafana Tempo datasource no longer lives in grafana/grafana; it was extracted to https://github.com/grafana/grafana-tempo-datasource (v13.1.5, bundled with Grafana >= 12.3).

## 0. JSON encoding rules that apply to every endpoint

All JSON responses are produced by gogo `jsonpb.Marshaler{}` with default options. A compatible server must replicate:

| Proto type | JSON |
|---|---|
| field names | lowerCamelCase (`start_time_unix_nano` -> `startTimeUnixNano`, `timestamp_ms` -> `timestampMs`, `trace_id` -> `traceId`) |
| zero values | omitted (EmitDefaults=false): empty lists, `0`, `""`, `false`, enum 0, `status: COMPLETE` disappear |
| `uint64`/`int64`/`fixed64` | decimal strings (`"startTimeUnixNano": "1684778327699392724"`, `"durationNanos": "446979497"`, `"timestampMs": "1715000000000"`, `"intValue": "200"`) |
| `uint32`/`int32`/`double`/`bool` | JSON numbers / booleans (`"durationMs": 557`, `"matched": 1`) |
| enums | name strings (`"kind": "SPAN_KIND_SERVER"`, `"code": "STATUS_CODE_ERROR"`, `"status": "PARTIAL"`) |
| `bytes` | base64 — this is what happens to OTLP `traceId`/`spanId`/`parentSpanId` in `/api/traces` JSON |
| `AnyValue` oneof | `{"stringValue": "x"}`, `{"intValue": "42"}`, `{"doubleValue": 1.5}`, `{"boolValue": true}`, `{"arrayValue": {"values": [...]}}`, `{"kvlistValue": {"values": [{"key","value"}]}}`, `{"bytesValue": "base64"}` |
| maps | JSON objects |

## 1. Endpoints actually called by Grafana and Traces Drilldown

### 1.0 Headers

- `Accept`: `application/protobuf` -> proto; `application/json` (or absent) -> JSON; `application/vnd.grafana.llm` -> LLM JSON. Substring match in the order protobuf, json, llm.
- Grafana's backend sends **`Accept: application/protobuf` for trace-by-ID** and **`Accept: application/json` for search and metrics**.
- `X-Scope-OrgID`: tenant header, only in multitenant mode. Grafana forwards it only if configured as a custom header.
- Grafana health check: `GET api/echo`.

### 1.1 `GET /api/echo`
Returns 200, body `echo`. Used by Grafana "Save & test".

### 1.2 `GET /api/status/buildinfo`
JSON with `version`, `revision`, `branch`, `buildDate`, `buildUser`, `goVersion`. Neither the datasource nor Drilldown calls it today; streaming is gated on datasource settings, not a version probe. Serve it anyway with a plausible `version` (e.g. `"2.9.0"`).

### 1.3 `GET /api/traces/{traceID}` (v1) and `GET /api/v2/traces/{traceID}` (v2)

Path var: hex trace ID. Grafana validates `^[0-9A-Fa-f]+$`.

Query params:

| param | type | semantics |
|---|---|---|
| `start` | int64 unix seconds | optional |
| `end` | int64 unix seconds | optional; if both given, `end` must be > start else 400 `http parameter start must be before end` |
| `mode`, `blockStart`, `blockEnd` | | internal |

v2-only: `q` (single spanset filter), `keep_hierarchy` (bool), `match_depth` (int), `ancestor_depth` (int), `span_pruning*`.

**What Grafana sends**: tries `api/v2/traces/{id}?start=&end=` first with `Accept: application/protobuf`; on **404** retries `api/traces/{id}`. `start`/`end` are the panel range widened by `spanStartTimeShift`/`spanEndTimeShift` (default 30m each). So the server **must accept start/end on trace-by-ID and must serve protobuf**. Drilldown opens a trace by issuing a Tempo query with `query: <traceId>, queryType: 'traceql'`, same path.

Responses:

- v2 = `tempopb.TraceByIDResponse`:
  ```json
  {
    "trace": { "resourceSpans": [ ...OTLP ResourceSpans... ] },
    "metrics": { "inspectedBytes": "12345" },
    "status": "PARTIAL",
    "message": "..."
  }
  ```
  `status` is `PartialStatus {COMPLETE=0, PARTIAL=1}`, omitted when COMPLETE. Grafana reads `trace.resourceSpans`. 404 when not found (Grafana then falls back to v1; a v1 404 yields "trace not found").
- v1 proto = `tempopb.Trace{ resourceSpans }`; v1 JSON = same but the first `"resourceSpans":` key is rewritten to `"batches":`.

### 1.4 `GET /api/search`

| param | type | default | notes |
|---|---|---|---|
| `q` | TraceQL | — | mutually exclusive with `tags` (400 `can't specify tags and q in the same query`) |
| `tags` | logfmt `k=v k2="v 2"` | — | legacy; case-insensitive substring match; duplicate key -> 400 |
| `minDuration`,`maxDuration` | Go duration | — | legacy; max < min -> 400 |
| `limit` | uint32 | 20 | `0` -> 400 |
| `spss` | uint32 | 3 | spans per spanset; `0` = unlimited |
| `start`,`end` | uint32 unix seconds | 0/0 | both 0 = recent data only; otherwise `end <= start` -> 400 `http parameter start must be before end. received start=.. end=..` |

If none of `q`/`tags`/`start`/`end` are present, other query params are treated as legacy `key=value` tags.

**What Grafana sends**: `q`, `limit`, `spss`, `start`, `end` (unix seconds, always both), `Accept: application/json`. Defaults `limit=20`, `spss=3`.

### 1.5 `GET /api/search/tags` (v1) and `GET /api/v2/search/tags`

| param | notes |
|---|---|
| `scope` | `resource`|`span`|`intrinsic`|`event`|`link`|`instrumentation`|`trace`|`""`/`none` (= all). Unknown -> 400 `invalid scope: x` |
| `q` | TraceQL filter (single spanset) |
| `start`,`end` | int32 unix seconds |
| `limit` | max tag names per scope |
| `maxStaleValues` | early-stop threshold |

v1 response: `{"tagNames": ["host.name", ...], "metrics": {"inspectedBytes": "630188"}}`.

v2 response:
```json
{
  "scopes": [
    { "name": "link", "tags": ["link-type"] },
    { "name": "resource", "tags": ["k6", "service.name"] },
    { "name": "span", "tags": ["article.count", "http.method", "http.status_code", "http.url", "net.peer.name"] },
    { "name": "intrinsic", "tags": ["duration","event:name","event:timeSinceStart","instrumentation:name","instrumentation:version","kind","name","rootName","rootServiceName","span:duration","span:kind","span:name","span:status","span:statusMessage","status","statusMessage","trace:duration","trace:rootName","trace:rootService","traceDuration"] },
    { "name": "event", "tags": ["exception.escape","exception.message","exception.stacktrace","exception.type"] }
  ],
  "metrics": { "inspectedBytes": "377046" }
}
```
Tag names are unscoped inside each scope entry; Grafana re-prefixes as `<scope>.<tag>` except `intrinsic`.

**What Grafana sends**: only v2. Params: `limit=<tagLimit || 5000>`, optionally `start`/`end`. No `scope` param.

### 1.6 `GET /api/search/tag/{tag}/values` (v1) and `GET /api/v2/search/tag/{tag}/values`

Tag path var may contain slashes/dots. v2 requires the name to parse as a TraceQL identifier: `resource.service.name`, `span.http.status_code`, `.service.name`, `status`, `span:kind`, `event.exception.type`. Params: `q`, `start`, `end`, `limit`, `maxStaleValues`.

v1 response: `{"tagValues": ["article-service", ...], "metrics": {...}}`.

v2 response:
```json
{
  "tagValues": [
    { "type": "string", "value": "article-service" },
    { "type": "string", "value": "postgres" }
  ],
  "metrics": { "inspectedBytes": "502756" }
}
```
`type` values: `string`, `int`, `float`, `bool`, `duration`, `status`, `kind`. `q` may be **incomplete** (`{ resource.cluster = }`); a parse failure must degrade to the unfiltered list, never a 4xx.

**What Grafana/Drilldown send**: only v2, `q=<current filters as full {...} query>&limit=5000[&start&end]`.

### 1.7 `GET /api/metrics/query_range` and `GET /api/metrics/query`

| param | type | notes |
|---|---|---|
| `q` (also `query`) | TraceQL metrics | |
| `start`,`end` | unix seconds (<=10 digits), unix nanoseconds (>10 digits), fractional seconds, or RFC3339Nano | Defaults: `end=now`, `start=end-since` |
| `since` | Prometheus duration | default 1h |
| `step` | float seconds or Go duration | absent -> `DefaultQueryRangeStep`; negative -> 400. query_range only |
| `exemplars` | uint32 | max exemplars per series. Absent or `0` means *unspecified*, not none: `normalizeRequestExemplars` (`modules/frontend/metrics_query_range_handler.go`) then substitutes `max_exemplars` (config default 100) and caps at it. An integer `with(exemplars=N)` hint overrides the parameter, `with(exemplars=false)` forces 0, `with(exemplars=true)` is a no-op. Instant queries force 0. Grafana's datasource omits the parameter entirely (its UI input is commented out) and Drilldown never sets it, so both rely on that default. |
| `maxSeries` | uint32 | cap |
| others | | internal sharding; ignore |

`DefaultQueryRangeStep(start,end)`: `baseline = (end-start)/240`; if window < 1 min, step = baseline rounded down to a 50 ms multiple (min 50 ms); otherwise the first of `[1h, 5m, 1m, 15s, 5s, 1s]` smaller than baseline, rounded down to a multiple of it; minimum 1 s.

**What Grafana sends**: `q`, `start=<From.Unix()>`, `end=<To.Unix()>`, `step=<verbatim, e.g. "30s">` if set, `exemplars=<n>` if set, `Accept: application/json`. Metrics detected client-side by regex `\|\s*(rate|count_over_time|avg_over_time|max_over_time|min_over_time|sum_over_time|quantile_over_time|histogram_over_time|compare)\s*\(`.

### 1.8 Not used
- `/api/metrics/summary`: deprecated, not called.
- Service graph / span metrics: Grafana queries the **Prometheus** datasource configured on the Tempo DS. Tempo serves nothing for this.
- `POST /api/v2/traces/diff`, `/api/mcp`, `/api/overrides`: not called.

## 2. `/api/traces/{id}` JSON (OTLP-style)

```json
{
  "batches": [
    {
      "resource": {
        "attributes": [ { "key": "service.name", "value": { "stringValue": "shop-backend" } } ]
      },
      "scopeSpans": [
        {
          "scope": { "name": "my.library", "version": "1.0.0" },
          "spans": [
            {
              "traceId": "W47/95gDgQPSabYzgT/GDA==",
              "spanId": "7uGbfsPBsXQ=",
              "parentSpanId": "7uGbfsPBsXM=",
              "name": "HTTP GET /cart",
              "kind": "SPAN_KIND_SERVER",
              "startTimeUnixNano": "1544712660000000000",
              "endTimeUnixNano":   "1544712661000000000",
              "attributes": [
                { "key": "http.status_code", "value": { "intValue": "200" } },
                { "key": "http.method",      "value": { "stringValue": "GET" } },
                { "key": "sampled",          "value": { "boolValue": true } },
                { "key": "ratio",            "value": { "doubleValue": 0.5 } }
              ],
              "events": [
                { "timeUnixNano": "1544712660500000000", "name": "exception",
                  "attributes": [ { "key": "exception.message", "value": { "stringValue": "boom" } } ] }
              ],
              "links": [
                { "traceId": "...base64...", "spanId": "...base64...", "attributes": [] }
              ],
              "status": { "message": "Forbidden", "code": "STATUS_CODE_ERROR" }
            }
          ]
        }
      ]
    }
  ]
}
```

Enums: `SpanKind {0 UNSPECIFIED, 1 INTERNAL, 2 SERVER, 3 CLIENT, 4 PRODUCER, 5 CONSUMER}`; `StatusCode {0 UNSET, 1 OK, 2 ERROR}`.

Grafana's Go backend requests protobuf and hex-encodes ids itself. Grafana's frame: `traceID, spanID, parentSpanID, operationName, serviceName, serviceNamespace, kind ("server"/...), statusCode, statusMessage, instrumentationLibraryName/Version, traceState, serviceTags, startTime (ms), duration (ms), logs, references, tags`.

## 3. `/api/search` JSON

```protobuf
message SearchResponse { repeated TraceSearchMetadata traces = 1; SearchMetrics metrics = 2; }
message TraceSearchMetadata {
  string traceID = 1; string rootServiceName = 2; string rootTraceName = 3;
  uint64 startTimeUnixNano = 4; uint32 durationMs = 5;
  SpanSet spanSet = 6;            // deprecated, still populated with the first spanset
  repeated SpanSet spanSets = 7;
  map<string, ServiceStats> serviceStats = 8;   // {spanCount uint32, errorCount uint32}
}
message SpanSet { repeated Span spans = 1; uint32 matched = 2; repeated KeyValue attributes = 3; }
message Span { string spanID = 1; string name = 2; uint64 startTimeUnixNano = 3; uint64 durationNanos = 4; repeated KeyValue attributes = 5; }
message SearchMetrics { uint32 inspectedTraces=1; uint64 inspectedBytes=2; uint32 totalBlocks=3; uint32 completedJobs=4; uint32 totalJobs=5; uint64 totalBlockBytes=6; uint64 inspectedSpans=7; uint64 backendReads=8; uint64 backendBytes=9; map<string,int64> additionalMetrics=10; }
```

Docs example:
```json
{
  "traces": [
    {
      "traceID": "2f3e0cee77ae5dc9c17ade3689eb2e54",
      "rootServiceName": "shop-backend",
      "rootTraceName": "update-billing",
      "startTimeUnixNano": "1684778327699392724",
      "durationMs": 557,
      "spanSets": [
        {
          "spans": [
            {
              "spanID": "563d623c76514f8e",
              "startTimeUnixNano": "1684778327735077898",
              "durationNanos": "446979497",
              "attributes": [ { "key": "status", "value": { "stringValue": "error" } } ]
            }
          ],
          "matched": 1
        }
      ]
    }
  ],
  "metrics": { "totalBlocks": 13 }
}
```
`traceID`/`spanID` are hex strings. `spanSet.attributes` carries spanset-level values (`by()` keys, `count()`); `span.attributes` carries `select()`ed / matched fields. Grafana sorts traces by `startTimeUnixNano` desc, uses `spanSets` (falls back to `spanSet`), merges `spanSet.attributes` + `span.attributes`, reads values from `boolValue|doubleValue|intValue|stringValue`.

## 4. `/api/metrics/query_range` and `/api/metrics/query` JSON

```protobuf
message QueryRangeResponse { repeated TimeSeries series = 1; SearchMetrics metrics = 2; PartialStatus status = 3; string message = 4; }
message TimeSeries { repeated KeyValue labels = 1; repeated Sample samples = 2; repeated Exemplar exemplars = 4; }
message Sample   { int64 timestamp_ms = 2; double value = 1; }
message Exemplar { repeated KeyValue labels = 1; double value = 2; int64 timestamp_ms = 3; }
message QueryInstantResponse { repeated InstantSeries series = 1; SearchMetrics metrics = 2; PartialStatus status = 3; string message = 4; }
message InstantSeries { repeated KeyValue labels = 1; double value = 2; }
```

```json
{
  "series": [
    {
      "labels": [
        { "key": "resource.service.name", "value": { "stringValue": "frontend" } },
        { "key": "p", "value": { "doubleValue": 0.9 } }
      ],
      "samples": [
        { "timestampMs": "1715000000000", "value": 12.5 },
        { "timestampMs": "1715000030000", "value": 13 }
      ],
      "exemplars": [
        { "labels": [ { "key": "trace:id", "value": { "stringValue": "2f3e0cee77ae5dc9c17ade3689eb2e54" } } ],
          "value": 0.412, "timestampMs": "1715000012345" }
      ]
    }
  ],
  "metrics": { "inspectedBytes": "1024" }
}
```
Instant: `"series": [ { "labels": [...], "value": 42 } ]`. Sample timestamps are the start of each step bucket, aligned to step, in ms. Emit `"value":0` explicitly (harmless).

Special labels: `p` (float) per quantile from `quantile_over_time`; `__bucket` for `histogram_over_time` (bucket upper bound); `__meta_type` in `baseline|selection|baseline_total|selection_total` and `__meta_error="__too_many_values__"` for `compare()`.

Grafana frames: one per series, fields `time` and value named by series name; name = single label's value if one label, else `{k1=v1, k2="v2"}` sorted. `histogram_over_time` gets heatmap visualization. Exemplars -> frame named `exemplar` on `DataTopic.Annotations` with `Time`, `Value`, `traceId` (from label `trace:id`, surrounding quotes stripped) plus one string column per series label; exemplars with `value == 0` or `timestampMs <= 0` are dropped (NaN survives). Traces Drilldown's `exemplarsTransformations` finds the `traceId` field and hangs a "View trace" data link on it.

Exemplar semantics in Tempo (`pkg/traceql/ast_metrics.go`, `engine_metrics.go`): `timestampMs` is the span's **start time**, not the bucket start. Values are per-function: `rate()`, `count_over_time()`, and `histogram_over_time()` produce NaN placeholders that `combiner.attachExemplars` replaces with the series' own sample value in the matching interval; `min/max/avg/sum_over_time(attr)` and `quantile_over_time(attr, ...)` carry the span's real value, in seconds for `duration`. A `quantile_over_time` exemplar is attached to exactly one quantile series, the one closest to it by value. An exemplar is dropped if the series value at its interval is NaN. Labels are all the span's materialized attributes; `trace:id` is there because `ExemplarMetaConditions` selects it.

## 5. TraceQL grammar

Lexer tokens: keywords `true false nil ok error unset unspecified internal server client producer consumer`; intrinsics `duration childCount name status statusMessage kind rootName rootServiceName rootService traceDuration nestedSetLeft nestedSetRight nestedSetParent id traceID spanID parentID timeSinceStart version parent`; scope prefixes `parent. resource. span. event. link. instrumentation.` and `trace: span: event: link: instrumentation:`; functions `count avg max min sum by coalesce select rate count_over_time min_over_time max_over_time avg_over_time sum_over_time quantile_over_time histogram_over_time compare topk bottomk with`; operators `= != > >= < <= =~ !~ && || | + - * / % ^ !` and structural `> < >> << ~ !> !< !>> !<< !~ &> &< &>> &<< &~`. Precedence (low to high): `|` < `&& ||` < comparisons/structural < `+ -` < `!` < `* / %` < `^` < unary minus.

Literals: strings Go-quoted (`strconv.Unquote`); ints; floats; durations = number immediately followed by unit, parsed first as Prometheus duration (`ms s m h d w y`) then Go `time.ParseDuration` (`ns us µs ms s m h`); status `ok|error|unset`; kind `unspecified|internal|server|client|producer|consumer`; `nil` only in `= nil` / `!= nil` (existence tests). Regex is RE2, fully anchored. Array attributes: `=`/`=~` match if any element matches; `!=`/`!~` only if none match.

Attributes: `.foo` (unscoped -> span or resource), `resource.foo`, `span.foo`, `event.foo`, `link.foo`, `instrumentation.foo`, `parent.foo`, `parent.resource.foo`, `parent.span.foo`. Names run until whitespace or one of `{ } ( ) = ~ ! < > & | ^ ,` and may embed `"quoted parts"`, e.g. `span."attr with space"`.

Intrinsics: unscoped `duration name status statusMessage kind parent rootName rootServiceName traceDuration nestedSetLeft nestedSetRight nestedSetParent`; scoped `trace:duration trace:rootName trace:rootService trace:id`, `span:duration span:name span:kind span:status span:statusMessage span:id span:parentID span:childCount`, `event:name event:timeSinceStart`, `link:traceID link:spanID`, `instrumentation:name instrumentation:version`. Root has `nestedSetParent < 0` (Drilldown relies on this).

Grammar (abridged):

```
root        := spansetPipeline
             | spansetPipelineExpression
             | scalarPipelineExpressionFilter
             | spansetPipeline '|' metricsAggregation [metricsSecondStagePipeline]
             | metricsExpression [metricsSecondStagePipeline]
             | root hints                                  // trailing  with(k=v, ...)
spansetPipeline := spansetExpression | scalarFilter | groupOperation | selectOperation
             | spansetPipeline '|' (spansetExpression | scalarFilter | groupOperation | coalesceOperation | selectOperation)
spansetExpression := '(' spansetExpression ')' | spansetFilter
             | spansetExpression OP spansetExpression      // OP in && || > < >> << ~ !> !< !>> !<< !~ &> &< &>> &<< &~
spansetFilter := '{' '}' | '{' fieldExpression '}'
fieldExpression := '(' fe ')' | fe (+ - * / % ^ = != < <= > >= =~ !~ && ||) fe
             | fe != nil | fe = nil | '-' fe | '!' fe | static | intrinsic | attribute
scalarFilter := scalarExpression (= != < <= > >=) (scalarExpression | scalar) | scalar CMP scalarExpression
scalarExpression := aggregate | scalarExpression (+ - * / % ^) (scalarExpression|scalar) | '(' ... ')'
aggregate   := count() | max(fe) | min(fe) | avg(fe) | sum(fe)
groupOperation := by '(' fieldExpression ')'
coalesceOperation := coalesce '(' ')'
selectOperation := select '(' attribute {, attribute} ')'
metricsAggregation :=
      rate() [by(attrList)] | count_over_time() [by(...)]
    | (min_over_time|max_over_time|sum_over_time|avg_over_time|histogram_over_time) '(' attribute ')' [by(...)]
    | quantile_over_time '(' attribute ',' numericList ')' [by(...)]
    | compare '(' spansetFilter [',' INT topN [',' INT startNs ',' INT endNs]] ')'   // topN default 10
metricsSecondStagePipeline := ('|' (topk(INT) | bottomk(INT)) | CMP scalar)+
metricsExpression := '(' spansetPipeline '|' metricsAggregation [secondStage] ')' | me (+ - * /) me | me OP scalar | scalar OP me
hints       := with '(' IDENTIFIER '=' static {, ...} ')'
```
Semantics: `{A} >> {B}` returns B-side spans that are descendants of A; `<<` ancestors; `>`/`<` direct child/parent; `~` sibling; `!`-forms negate; `&`-forms return the union of both sides. Safe hints: `sample`, `trace_sample`, `span_sample`, `exemplars`, `most_recent`, `extrapolate`; others ignored. Scalar filter operands must contain an aggregate.

Metrics functions: `rate()` spans/sec per step; `count_over_time()`; `sum/min/max/avg_over_time(attr)`; `quantile_over_time(attr, q...)` (one series per q, label `p`); `histogram_over_time(attr)` (label `__bucket`); `compare({filter}[, topN[, startNs, endNs]])`; `topk(n)`/`bottomk(n)`; comparisons on results; `by (a, b)`; hints `with(sample=true)`, `with(exemplars=true)`, `with(most_recent=true)`.

## 6. What Traces Drilldown sends

Building blocks:
- `VAR_FILTERS_EXPR = '${primarySignal} && ${filters}'`.
- Primary signals: Root spans `nestedSetParent<0` (default), All spans `true`, Server spans `kind=server`, Consumer spans `kind=consumer`, Database calls `span.db.system.name!=""`.
- `${filters}` = ad-hoc filters rendered as `key op value` joined by ` && `, or literally `true` when empty. String values double-quoted with `\\ \" \n \r \t` escaping; numeric values and values for keys `status kind span:status span:kind duration span:duration trace:duration event:timeSinceStart` are unquoted.
- Every query: `{refId:'A', query, queryType:'traceql', tableType:'spans'|'raw', limit, spss, step?, filters:[]}`.

Concrete queries (Root spans + no filters):

| View | Query | limit/spss/step |
|---|---|---|
| RED rate | `{nestedSetParent<0 && true} \| rate()  with(sample=true)` | step = `${floor(rangeSec/maxDataPoints)||1}s` |
| RED errors | `{nestedSetParent<0 && true && status=error} \| rate()  with(sample=true)` | same |
| RED duration | `{nestedSetParent<0 && true} \| histogram_over_time(duration) with(sample=true)` | limit 1000, spss 10 |
| Breakdown by attribute | rate: `{nestedSetParent<0 && true && resource.service.name != nil} \| rate() by(resource.service.name)`; errors: `{... && status=error && A != nil} \| rate() by(A)`; duration: `{... && A != nil} \| quantile_over_time(duration, 0.9) by(A)` | maxDataPoints 64 -> step; limit 100, spss 10 |
| Breakdown tile | `{nestedSetParent<0 && true && resource.service.name="frontend"} \| rate() by(resource.service.name)` | |
| Comparison | `{nestedSetParent<0 && true} \| compare({status = error}, 10)` or `\| compare({duration >= 1.2s && duration <= 3s}, 10, <fromNs>, <toNs>)` | step = `${to-from}s` (single bucket), limit 100, spss 10 |
| Trace/span list | `{nestedSetParent<0 && true}`; errors `{... && status = error}`; duration `{... && duration > ${latencyThreshold}}`; optional ` \| select(col1, col2)` | limit 200, spss 10 |
| Structure tab | `({nestedSetParent<0 && true  && status = error} &>> { status = error }) \|\| ({nestedSetParent<0 && true  && status = error}) \| select(status, resource.service.name, name, nestedSetParent, nestedSetLeft, nestedSetRight)` | tableType raw, limit 200, spss 20 |
| Exceptions tab | `{nestedSetParent<0 && true && status = error} \| select(resource.service.name, event.exception.message,event.exception.stacktrace,event.exception.type) with(most_recent=true)` | limit 400 / 100, spss 10 |
| Time seeker | `{...} \| rate()  with(sample=true)` in 24h batches | step `30s` |
| TraceQL issue detector | `{} \| rate()` — if error contains `localblocks processor not found` it shows a "metrics not configured" banner | |
| Trace view | `query: <hex traceId>` -> `/api/v2/traces/{id}?start&end` | |

On load: `GET api/v2/search/tags?limit=5000` (all scopes); keys shown as `resource.x`, `span.x`, `event.x`, intrinsics bare. Drilldown hides `duration event:name nestedSetLeft nestedSetParent nestedSetRight span:duration span:id trace:duration trace:id traceDuration`. Value pickers -> `GET api/v2/search/tag/<key>/values?q={<current filters>}&limit=5000`. Exemplars: Drilldown expects the `exemplar` frame with a `traceId` field. No buildinfo detection.

## 7. Time handling summary

| Where | Unit |
|---|---|
| `/api/search`, tags, tag values, `/api/traces*` `start`/`end` | unix seconds, integer |
| `/api/metrics/query{,_range}` `start`/`end` | seconds if <=10 digits, else nanoseconds; floats and RFC3339Nano also accepted; `since` alternative; default `[now-1h, now]` |
| `step` | seconds (float) or Go duration; default per `DefaultQueryRangeStep` |
| Search response | `startTimeUnixNano` (string ns), `durationMs` (uint32), span `durationNanos` (string ns) |
| Trace response | `startTimeUnixNano`/`endTimeUnixNano`/`timeUnixNano` (string ns) |
| Metrics response | `timestampMs` (string ms) |
| Grafana | seconds everywhere over HTTP; trace-by-ID range widened +/-30m by default |
| Default lookback when omitted | search/tags: recent data only; metrics: last 1h; trace-by-ID: all |

Errors: parameter problems return 400 with a plain-text message (`http parameter start must be before end. received start=X end=Y`, `invalid limit: ...`, `invalid scope: x`); trace not found returns 404; Grafana surfaces 4xx bodies verbatim.
