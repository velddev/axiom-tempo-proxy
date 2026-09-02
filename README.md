# axiom-tempo-proxy

A proxy that speaks the [Grafana Tempo](https://grafana.com/docs/tempo/latest/api_docs/)
query API and answers from an [Axiom](https://axiom.co) dataset of
OpenTelemetry spans by translating requests into APL.

Point Grafana's Tempo datasource (and Grafana Traces Drilldown) at this
proxy and your traces stored in Axiom show up in the trace view, TraceQL
search, tag autocomplete, and the RED metrics panels.

## What it supports

| Tempo endpoint | Status |
|---|---|
| `GET /api/echo`, `GET /api/status/buildinfo` | yes |
| `GET /api/traces/{id}`, `GET /api/v2/traces/{id}` | yes, protobuf and JSON |
| `GET /api/search` (TraceQL `q`, legacy `tags`) | yes, full TraceQL via Tempo's engine |
| `GET /api/search/tags`, `GET /api/v2/search/tags` | yes |
| `GET /api/search/tag/{tag}/values`, v2 | yes, with `q` filter pushdown |
| `GET /api/metrics/query_range`, `GET /api/metrics/query` | yes: `rate`, `count_over_time`, `min/max/avg/sum_over_time`, `quantile_over_time`, `histogram_over_time`, `compare`, `topk`/`bottomk`, comparisons, `by()`, arithmetic between queries |
| Service graph / span metrics | no (Grafana reads those from Prometheus) |
| gRPC streaming (`tempopb.StreamingQuerier`) | yes, on the same port (see below) |

## How it works

- **Trace by id** is one APL query on `trace_id`, converted to OTLP. If
  the query hits the span cap the trace is still returned, but the v2
  response is marked `PARTIAL` with a message (v1 has no status field).
- **Search** pushes the query's spanset filters down to APL to find
  candidate traces, pulls the spans of those traces, and runs Tempo's own
  TraceQL engine in-process over them. Structural operators, aggregates,
  `select`, `by`, and `coalesce` all work exactly as in Tempo. The span
  pull `project`s only the columns the query needs — the intrinsics, the
  resource identity, and the columns backing the attributes the query
  filters on or `select`s — so a 183-column dataset moves a fraction of
  the bytes. Trace by id still fetches every column.
- **Partial results are explicit.** The span pull runs in batches of
  `PROXY_SEARCH_BATCH_TRACES` candidate traces (one query when they all
  fit) and stops once the `PROXY_MAX_SPANS_PER_FETCH` budget is spent, so
  a trace is either returned whole or not at all. Candidates left out are
  counted in `metrics.additionalMetrics.droppedTraces` on the search
  response and logged with the query. Metrics responses set
  `status: PARTIAL` with a message when Axiom reports a partial result,
  and pass Axiom's status messages through.
- **Search ranking**: a search returns at most `limit` traces, newest
  first. When the query `select()`s attributes (Drilldown's Exceptions tab
  selects `event.exception.*`), traces that actually carry those
  attributes are ranked ahead of traces that don't, so the bounded result
  shows the data the caller asked for instead of being dominated by, say,
  exception-free errors. Every returned trace still matches the filter.
  Disable with `PROXY_NO_PREFER_SELECTED=true`.
- **Event attributes** (`event.exception.type = "X"`, `event:name`) are
  pushed down too: the `events` array has no per-row "any element
  matches" construct in APL, so the test is repeated over the first
  `MaxEventsPerSpan` (8) event slots and or-ed. Spans with more events
  than that are only matched on their first eight. The tag endpoints
  cover them too: `/api/v2/search/tags` lists an `event` scope of the
  attribute keys found in the data, and tag values for
  `event.<key>`/`event:name` expand the array (`mv-expand`) after the
  `q` filter has been pushed down, so they come back as strings.
- **Streaming** serves Tempo's `StreamingQuerier` gRPC service on the same
  port as the HTTP API. Requests arriving as cleartext HTTP/2 (h2c) with a
  `application/grpc` content type are routed to the gRPC server, everything
  else to the HTTP mux, so HTTP/1.1 behaviour is unchanged. Each streaming
  method does the same work as its HTTP handler and sends exactly one final
  message with the complete result; Tempo sends progressive partial results
  instead, and Grafana keeps the last message either way. Streamed
  responses carry the same `droppedTraces` metric, `PARTIAL` status and
  exemplars as the HTTP ones.
- **Metrics** are translated to native APL `summarize ... by bin(_time, step)`
  aggregations, so they scale with Axiom rather than with the proxy.
- **Exemplars** (the clickable trace dots on RED and breakdown panels)
  come out of that same `summarize`: an `arg_max`/`arg_min` picks one
  span per bucket per series, so no extra query is issued. The exemplar
  carries a `trace:id` label, the span's own start time, and the value
  Tempo would report — the span's value in seconds for `min/max/avg/
  sum/quantile_over_time(duration)`, and the bucket's own value for
  `rate`, `count_over_time`, and `histogram_over_time`. `compare()` and
  instant queries carry none, as in Tempo. Neither Grafana nor Traces
  Drilldown sends the `exemplars` parameter, so the count falls back to
  `PROXY_DEFAULT_EXEMPLARS`; `with(exemplars=N)` and
  `with(exemplars=false)` in the query override it.
- **Trace-level intrinsics in metrics** (`traceDuration`/`trace:duration`,
  `rootName`/`trace:rootName`, `rootServiceName`/`trace:rootService`) are
  properties of a whole trace, so no single row can be tested for them.
  The filter is split at its top-level `&&`: the trace-level part is
  resolved first by a per-trace aggregation (`summarize … by trace_id`),
  and the qualifying trace ids are inlined into the metrics query as
  `trace_id in (…)`. `{ traceDuration > 2s } | rate()`,
  `{ rootServiceName = "web" && status = error } | rate() by (name)` and
  `{ trace:rootName = "GET /x" } | quantile_over_time(duration, .9)` all
  work. Limits, and when a query is refused with a 400 instead of
  returning partial numbers, are in
  [docs/DESIGN.md](docs/DESIGN.md#trace-level-intrinsics-in-metrics).
- The dataset's field list is discovered at startup (and refreshed) so
  attributes that Axiom flattens (`attributes.http.method`) and attributes
  kept in the `attributes.custom` map are both addressed correctly.

See [docs/DESIGN.md](docs/DESIGN.md) for details.

## Running

```bash
export AXIOM_TOKEN=xaat-...
export AXIOM_DATASET=otel-traces-prod
go run ./cmd/axiom-tempo-proxy -listen :3200
```

Then add a Tempo datasource in Grafana with URL `http://localhost:3200`.

### Selecting the dataset

Datasets are per environment. A request picks its dataset in this order:

1. **URL prefix**: `http://localhost:3200/prod/api/...`. Set the Grafana
   datasource URL to `http://localhost:3200/prod` and every call carries
   the prefix. One proxy serves any number of datasources this way.
2. **Header** `X-Axiom-Dataset` (configurable), for example as a custom
   header on the datasource.
3. **Query parameter** `?dataset=prod`.
4. The configured default (`AXIOM_DATASET`), if any.

`AXIOM_DATASET` is optional. Restrict which datasets a request may select
with `PROXY_ALLOWED_DATASETS=prod,staging`; without it any dataset the
token can read is accepted.

### Streaming

Grafana's Tempo datasource uses gRPC streaming for search and metrics when
**Streaming** is enabled on the datasource. The proxy serves
`tempopb.StreamingQuerier` (`Search`, `SearchTags`, `SearchTagsV2`,
`SearchTagValues`, `SearchTagValuesV2`, `MetricsQueryRange`,
`MetricsQueryInstant`) over h2c on the listen address, so no extra port or
configuration is needed.

**The URL prefix does not work for streaming.** Grafana appends `/api/...`
to the datasource URL for HTTP calls, but for gRPC it only dials the URL's
host and port: a datasource URL like `http://proxy:3200/prod` cannot carry
the `prod` prefix over gRPC. Streaming users must therefore either

- set a custom datasource header `X-Axiom-Dataset: prod` (Grafana forwards
  custom headers as gRPC metadata, and the proxy also accepts a plain
  `dataset` metadata key), or
- run a proxy with `AXIOM_DATASET` set to that dataset,

otherwise streaming calls fail with `InvalidArgument: no dataset`. HTTP
calls keep accepting the URL prefix and the `?dataset=` parameter.

### Configuration

| Env | Flag | Default | Meaning |
|---|---|---|---|
| `AXIOM_TOKEN` | | | Axiom API token (required) |
| `AXIOM_DATASET` | `-dataset` | | default dataset (optional, see above) |
| `AXIOM_URL` | `-axiom-url` | `https://api.axiom.co` | API base URL |
| `AXIOM_ORG_ID` | `-axiom-org-id` | | org id, needed for personal tokens |
| `PROXY_LISTEN_ADDR` | `-listen` | `:3200` | listen address |
| `PROXY_DATASET_HEADER` | `-dataset-header` | `X-Axiom-Dataset` | header that selects the dataset |
| `PROXY_ALLOWED_DATASETS` | | any | comma separated allow-list for the header |
| `PROXY_DEFAULT_LOOKBACK` | `-default-lookback` | `1h` | search window when none is given |
| `PROXY_TRACE_LOOKBACK` | `-trace-lookback` | `24h` | trace-by-id window when none is given |
| `PROXY_MAX_SEARCH_TRACES` | `-max-search-traces` | `500` | cap on candidate traces per search |
| `PROXY_MAX_SPANS_PER_FETCH` | `-max-spans-per-fetch` | `50000` | span budget for one search (across its batches) or one trace |
| `PROXY_SEARCH_BATCH_TRACES` | `-search-batch-traces` | `400` | candidate traces per span-pull query during a search |
| `PROXY_MAX_TAG_VALUES` | `-max-tag-values` | `5000` | cap on tag values |
| `PROXY_DEFAULT_EXEMPLARS` | `-default-exemplars` | `100` | exemplars per series when the request names no number (`0` disables) |
| `PROXY_MAX_EXEMPLARS` | `-max-exemplars` | `1000` | cap on exemplars per series |
| `PROXY_MAX_TRACE_INTRINSIC_TRACES` | `-max-trace-intrinsic-traces` | `5000` | cap on traces a trace-level metrics filter may resolve to |
| `PROXY_SCHEMA_REFRESH` | `-schema-refresh` | `5m` | schema re-discovery interval |
| `PROXY_QUERY_TIMEOUT` | `-query-timeout` | `60s` | per-request timeout |
| `PROXY_LOG_QUERIES` | `-log-queries` | `false` | log every generated APL query |
| `PROXY_LOG_LEVEL` | `-log-level` | `info` | log level |

## Development

```bash
go test ./...
```
