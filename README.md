# axiom-tempo-proxy

axiom-tempo-proxy serves the [Grafana Tempo](https://grafana.com/docs/tempo/latest/api_docs/)
query API on top of an [Axiom](https://axiom.co) dataset of OpenTelemetry
spans. It translates Tempo requests into APL, runs them against Axiom, and
encodes the answers the way Tempo does. Point Grafana's Tempo datasource
or Grafana Traces Drilldown at it and traces stored in Axiom show up in
the trace view, TraceQL search, tag autocomplete and the RED metrics
panels. No Tempo installation is involved.

## Supported endpoints

| Tempo endpoint | Supported |
|---|---|
| `GET /api/echo`, `GET /api/status/buildinfo` | yes |
| `GET /api/traces/{id}`, `GET /api/v2/traces/{id}` | yes, protobuf and JSON |
| `GET /api/search` (TraceQL `q`, legacy `tags`) | yes, full TraceQL through Tempo's own engine |
| `GET /api/search/tags`, `GET /api/v2/search/tags` | yes |
| `GET /api/search/tag/{tag}/values`, v2 | yes, with `q` filter pushdown |
| `GET /api/metrics/query_range`, `GET /api/metrics/query` | yes: `rate`, `count_over_time`, `min/max/avg/sum_over_time`, `quantile_over_time`, `histogram_over_time`, `compare`, `topk`/`bottomk`, comparisons, `by()`, arithmetic between queries |
| gRPC streaming (`tempopb.StreamingQuerier`) | yes, on the same port |
| Service graph and span metrics | no, Grafana reads those from Prometheus |

## How it works

**Trace by id** is a single APL query on `trace_id`, converted to OTLP.

**Search** translates the TraceQL spanset filter into an APL prefilter,
finds candidate trace ids with it, pulls the spans of those traces, and
runs Tempo's own TraceQL engine in process over the result. Structural
operators, aggregates, `select`, `by` and `coalesce` behave as they do in
Tempo. The span pull projects only the columns the query needs, so a wide
dataset moves a fraction of its bytes. Trace by id fetches every column.

**Metrics** become native APL `summarize ... by bin(_time, step)`
aggregations, so they scale with Axiom rather than with the proxy.
Trace-level intrinsics work too: `traceDuration`, `rootName` and
`rootServiceName` are resolved by a separate per-trace aggregation whose
trace ids are inlined into the metrics query, so
`{ rootServiceName = "web" && status = error } | rate() by (name)` and
`{ traceDuration > 2s } | rate()` both return exact numbers.

**Exemplars**, the clickable trace dots on RED and breakdown panels, ride
along on that same metrics query through an `arg_max`/`arg_min`, so no
second query is issued. Each carries a `trace:id` label, the span's own
start time, and the value Tempo would report. `compare()` and instant
queries carry none, as in Tempo. Grafana and Traces Drilldown never send
the `exemplars` parameter, so the count falls back to
`PROXY_DEFAULT_EXEMPLARS`; `with(exemplars=N)` and `with(exemplars=false)`
in the query override it.

**Tag names and values** come from the dataset field list plus keys
sampled out of Axiom's map, event and link fields, with the `q` filter
pushed down. `/api/v2/search/tags` reports an `event` scope, and tag
values for `event.<key>` and `event:name` expand the `events` array after
the filter has been applied to whole spans.

**Streaming** serves Tempo's `StreamingQuerier` gRPC service on the listen
port. Cleartext HTTP/2 requests with an `application/grpc` content type go
to the gRPC server and everything else to the HTTP mux, so HTTP/1.1
behaviour is unchanged. Each streaming method does the same work as its
HTTP handler and sends one final message with the complete result. Tempo
instead sends progressive partial results, and Grafana keeps the last
message either way. Streamed responses carry the same `droppedTraces`
metric, `PARTIAL` status and exemplars as the HTTP ones.

The dataset's field list is discovered at startup and refreshed
periodically, so attributes Axiom flattens (`attributes.http.method`) and
attributes kept in the `attributes.custom` map are both addressed
correctly.

[docs/DESIGN.md](docs/DESIGN.md) covers the query strategies in detail,
with the generated APL.

## Quick start

```bash
export AXIOM_TOKEN=xaat-...
export AXIOM_DATASET=otel-traces-prod
go run ./cmd/axiom-tempo-proxy -listen :3200
```

Then add a Tempo datasource in Grafana with URL `http://localhost:3200`.

With Docker:

```bash
docker build -t axiom-tempo-proxy .
docker run --rm -p 3200:3200 \
  -e AXIOM_TOKEN=xaat-... \
  -e AXIOM_DATASET=otel-traces-prod \
  axiom-tempo-proxy
```

`cmd/schema-check` inspects a dataset and reports which of the APL
constructs the proxy relies on work against it:

```bash
AXIOM_TOKEN=xaat-... go run ./cmd/schema-check -dataset otel-traces-prod
```

### Selecting the dataset

A request picks its dataset in this order:

1. URL prefix: `http://localhost:3200/otel-traces-prod/api/...`. Set the
   Grafana datasource URL to `http://localhost:3200/otel-traces-prod` and
   every call carries the prefix. One proxy can back any number of
   datasources this way.
2. The `X-Axiom-Dataset` header, for example as a custom header on the
   datasource. The header name is configurable.
3. The `?dataset=` query parameter.
4. The configured default, `AXIOM_DATASET`.

`AXIOM_DATASET` is optional. `PROXY_ALLOWED_DATASETS` restricts which
datasets a request may select. Without it, any dataset the token can read
is accepted.

### Streaming

Grafana's Tempo datasource uses gRPC streaming for search and metrics when
**Streaming** is enabled on the datasource. The proxy serves `Search`,
`SearchTags`, `SearchTagsV2`, `SearchTagValues`, `SearchTagValuesV2`,
`MetricsQueryRange` and `MetricsQueryInstant` over h2c on the listen
address, so no extra port or configuration is needed.

The URL prefix does not work for streaming. Grafana appends `/api/...` to
the datasource URL for HTTP calls, but for gRPC it dials only the URL's
host and port, so a datasource URL like
`http://proxy:3200/otel-traces-prod` cannot carry the dataset over gRPC.
Streaming clients must instead either set a custom datasource header
`X-Axiom-Dataset: otel-traces-prod` (Grafana forwards custom headers as
gRPC metadata, and the proxy also accepts a plain `dataset` metadata key)
or talk to a proxy that has `AXIOM_DATASET` set. Otherwise streaming calls
fail with `InvalidArgument: no dataset`. HTTP calls keep accepting the URL
prefix and the `?dataset=` parameter.

## Configuration

Flags win over environment variables, which win over the defaults.

| Env | Flag | Default | Meaning |
|---|---|---|---|
| `AXIOM_TOKEN` | | | Axiom API token (required) |
| `AXIOM_DATASET` | `-dataset` | | default dataset (optional, see above) |
| `AXIOM_URL` | `-axiom-url` | `https://api.axiom.co` | API base URL |
| `AXIOM_ORG_ID` | `-axiom-org-id` | | org id, needed for personal tokens |
| `AXIOM_QUERY_PATH` | | `/v1/datasets/_apl` | APL query endpoint path |
| `PROXY_LISTEN_ADDR` | `-listen` | `:3200` | listen address |
| `PROXY_DATASET_HEADER` | `-dataset-header` | `X-Axiom-Dataset` | header that selects the dataset |
| `PROXY_ALLOWED_DATASETS` | | any | comma separated allow-list for prefix, header and parameter |
| `PROXY_DEFAULT_LOOKBACK` | `-default-lookback` | `1h` | search window when none is given |
| `PROXY_TRACE_LOOKBACK` | `-trace-lookback` | `24h` | trace-by-id window when none is given |
| `PROXY_MAX_SEARCH_TRACES` | `-max-search-traces` | `500` | cap on candidate traces per search |
| `PROXY_MAX_SPANS_PER_FETCH` | `-max-spans-per-fetch` | `50000` | span budget for one search (across its batches) or one trace |
| `PROXY_SEARCH_BATCH_TRACES` | `-search-batch-traces` | `400` | candidate traces per span-pull query during a search |
| `PROXY_MAX_TAG_VALUES` | `-max-tag-values` | `5000` | cap on tag values |
| `PROXY_TAG_SAMPLE_ROWS` | `-tag-sample-rows` | `2000` | rows sampled to discover map, event and link keys |
| `PROXY_DEFAULT_EXEMPLARS` | `-default-exemplars` | `100` | exemplars per series when the request names no number (`0` disables) |
| `PROXY_MAX_EXEMPLARS` | `-max-exemplars` | `1000` | cap on exemplars per series |
| `PROXY_MAX_TRACE_INTRINSIC_TRACES` | `-max-trace-intrinsic-traces` | `5000` | cap on traces a trace-level metrics filter may resolve to |
| `PROXY_NO_PREFER_SELECTED` | `-no-prefer-selected` | `false` | stop ranking search candidates that carry `select()`ed attributes first |
| `PROXY_SCHEMA_REFRESH` | `-schema-refresh` | `5m` | schema re-discovery interval |
| `PROXY_QUERY_TIMEOUT` | `-query-timeout` | `60s` | per-query timeout |
| `PROXY_LOG_QUERIES` | `-log-queries` | `false` | log every generated APL query |
| `PROXY_LOG_LEVEL` | `-log-level` | `info` | log level |

## Deploying

The `Dockerfile` builds a static binary into a distroless image that
listens on `:3200`.

[`deploy/`](deploy/) holds a helper that runs the proxy as a systemd
service on a Google Compute Engine instance. It takes the instance name
and zone as arguments, cross-compiles for the instance's architecture,
copies the binary and unit file across, and writes the Axiom token to a
mode 600 environment file on the host:

```bash
AXIOM_TOKEN=xaat-... AXIOM_DATASET=otel-traces-prod \
  deploy/deploy.sh my-instance my-zone
```

## Development

```bash
go test ./...
gofmt -l .
go vet ./...
```

CI runs those three checks on every push and pull request.

The tests are hermetic. They stand up an `httptest` Axiom stub and assert
on the generated APL, so no token or dataset is needed to run them.

## Limitations

- **Partial results are explicit, never silent.** The span pull runs in
  batches and stops once the `PROXY_MAX_SPANS_PER_FETCH` budget is spent,
  so a trace is either returned whole or not at all. Candidates left out
  are counted in `metrics.additionalMetrics.droppedTraces` on the search
  response and logged with the query. Metrics responses and trace-by-id v2
  set `status: PARTIAL` with a message, and pass Axiom's own status
  messages through. `SearchResponse` has no status field, which is why a
  search reports a count instead.
- **A search returns at most `limit` traces, newest first.** When the
  query `select()`s attributes, traces that carry those attributes are
  ranked ahead of traces that do not, so a bounded result shows the data
  the caller asked for. Every returned trace still matches the filter. Set
  `PROXY_NO_PREFER_SELECTED=true` to turn the ranking off.
- **Event attributes are matched over the first 8 events of a span.** The
  `events` array has no "any element matches" construct in APL, so the
  test is repeated over eight event slots and or-ed. Spans with more
  events are matched only on their first eight.
- **A search whose only filter is trace-level** (`traceDuration`,
  `rootName`, `rootServiceName`) inspects only the newest candidate traces
  and may come back empty. The prefilter relaxes trace intrinsics on
  purpose so that it stays a superset. Metrics queries resolve them
  exactly, in a separate pass.
- **Trace-level metrics filters are bounded.** More than
  `PROXY_MAX_TRACE_INTRINSIC_TRACES` qualifying traces, a query window
  whose trace count exceeds what Axiom can group exactly, a filter that
  mixes trace-level and span-level terms inside one `||`, or a
  trace-level `compare()` selection is refused with a 400 rather than
  answered with wrong numbers. Trace values are computed from the spans
  inside the query window, so a trace clipped by a window edge reports a
  shorter `traceDuration`. See
  [docs/DESIGN.md](docs/DESIGN.md#trace-level-intrinsics-in-metrics).
- **`span:childCount`, `nestedSetLeft`, `nestedSetRight`, `parent.*` and
  `link.*`** need the span tree and are unsupported in metrics queries.
- **Service graph and span metrics are not served.** Grafana reads those
  from the Prometheus datasource configured on the Tempo datasource.

## License

See `LICENSE`.
