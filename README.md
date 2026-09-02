# axiom-tempo-proxy

Serve the [Grafana Tempo](https://grafana.com/docs/tempo/latest/api_docs/)
query API from an [Axiom](https://axiom.co) dataset of OpenTelemetry spans.

Point Grafana's Tempo datasource or Traces Drilldown at the proxy and your
traces in Axiom appear in the trace view, TraceQL search, tag autocomplete
and the RED metrics panels. No Tempo installation is needed. The proxy
translates each request into APL, runs it against Axiom, and returns the
result in Tempo's wire format.

## Quick start

```bash
export AXIOM_TOKEN=xaat-...
export AXIOM_DATASET=otel-traces-prod
go run ./cmd/axiom-tempo-proxy -listen :3200
```

Add a Tempo datasource in Grafana with the URL `http://localhost:3200`.

Or with Docker:

```bash
docker build -t axiom-tempo-proxy .
docker run --rm -p 3200:3200 \
  -e AXIOM_TOKEN=xaat-... \
  -e AXIOM_DATASET=otel-traces-prod \
  axiom-tempo-proxy
```

To check a dataset before pointing the proxy at it:

```bash
AXIOM_TOKEN=xaat-... go run ./cmd/schema-check -dataset otel-traces-prod
```

It lists the dataset's columns and tries every APL construct the proxy
relies on.

## What is supported

| Tempo API | Status |
|---|---|
| Trace by id (`/api/traces`, `/api/v2/traces`) | yes, protobuf and JSON |
| TraceQL search (`/api/search`) | yes, including structural operators, aggregates, `select`, `by`, `coalesce` |
| Legacy tag search (`tags=`, `minDuration`, `maxDuration`) | yes |
| Tags and tag values (v1 and v2, with `q` filter) | yes, including `event.*` |
| TraceQL metrics (`/api/metrics/query_range`, `/api/metrics/query`) | yes: `rate`, `count_over_time`, `min/max/avg/sum_over_time`, `quantile_over_time`, `histogram_over_time`, `compare`, `topk`, `bottomk`, comparisons, `by()`, arithmetic |
| Exemplars on metrics | yes |
| gRPC streaming (`StreamingQuerier`) | yes, on the same port |
| Service graph and span metrics | no, Grafana reads those from Prometheus |

Everything Grafana Traces Drilldown asks for is covered.

## How it works

- **Trace by id** is one APL query on `trace_id`, converted to OTLP.
- **Search** pushes the TraceQL filter down to APL to find candidate
  traces, pulls their spans, and runs Tempo's own TraceQL engine over them
  in process. That is what makes structural operators and aggregates
  behave exactly as in Tempo.
- **Metrics** are translated to native APL `summarize` queries, so they
  scale with Axiom rather than with the proxy. Exemplars come from the
  same query.
- **Schema discovery** reads the dataset's column list at startup and
  refreshes it, so attributes Axiom flattens (`attributes.http.method`)
  and attributes kept in the `attributes.custom` map both resolve.

[docs/DESIGN.md](docs/DESIGN.md) explains each strategy with the APL it
generates.

## Selecting the dataset

A request picks its dataset from, in order:

1. The URL prefix: `http://localhost:3200/otel-traces-prod/api/...`. Set
   the Grafana datasource URL to `http://localhost:3200/otel-traces-prod`.
   One proxy can serve many datasources this way.
2. The `X-Axiom-Dataset` header (name configurable).
3. The `?dataset=` query parameter.
4. `AXIOM_DATASET`, if set.

`PROXY_ALLOWED_DATASETS` limits which datasets a request may select.

## Streaming

When **Streaming** is enabled on the Grafana datasource, search and
metrics go over gRPC. The proxy serves Tempo's `StreamingQuerier` on the
same port over cleartext HTTP/2, so nothing extra is needed.

One caveat: for gRPC Grafana dials only the host and port, so a URL prefix
cannot select the dataset. Streaming datasources must use the
`X-Axiom-Dataset` header or a proxy with `AXIOM_DATASET` set.

## Configuration

Flags override environment variables, which override the defaults.

| Env | Flag | Default | Meaning |
|---|---|---|---|
| `AXIOM_TOKEN` | | | Axiom API token (required) |
| `AXIOM_DATASET` | `-dataset` | | default dataset |
| `AXIOM_URL` | `-axiom-url` | `https://api.axiom.co` | API base URL |
| `AXIOM_ORG_ID` | `-axiom-org-id` | | org id, needed for personal tokens |
| `AXIOM_QUERY_PATH` | | `/v1/datasets/_apl` | APL query endpoint path |
| `PROXY_LISTEN_ADDR` | `-listen` | `:3200` | listen address |
| `PROXY_DATASET_HEADER` | `-dataset-header` | `X-Axiom-Dataset` | header that selects the dataset |
| `PROXY_ALLOWED_DATASETS` | | any | comma separated allow-list |
| `PROXY_DEFAULT_LOOKBACK` | `-default-lookback` | `1h` | search window when none is given |
| `PROXY_TRACE_LOOKBACK` | `-trace-lookback` | `24h` | trace-by-id window when none is given |
| `PROXY_MAX_SEARCH_TRACES` | `-max-search-traces` | `500` | cap on candidate traces per search |
| `PROXY_MAX_SPANS_PER_FETCH` | `-max-spans-per-fetch` | `50000` | span budget per search or per trace |
| `PROXY_SEARCH_BATCH_TRACES` | `-search-batch-traces` | `400` | candidate traces per span-pull query |
| `PROXY_MAX_TAG_VALUES` | `-max-tag-values` | `5000` | cap on tag values |
| `PROXY_TAG_SAMPLE_ROWS` | `-tag-sample-rows` | `2000` | rows sampled to discover map and event keys |
| `PROXY_DEFAULT_EXEMPLARS` | `-default-exemplars` | `100` | exemplars per series when the request names no number (`0` disables) |
| `PROXY_MAX_EXEMPLARS` | `-max-exemplars` | `1000` | cap on exemplars per series |
| `PROXY_MAX_TRACE_INTRINSIC_TRACES` | `-max-trace-intrinsic-traces` | `5000` | cap on traces a trace-level metrics filter may match |
| `PROXY_NO_PREFER_SELECTED` | `-no-prefer-selected` | `false` | do not rank search results carrying `select()`ed attributes first |
| `PROXY_SCHEMA_REFRESH` | `-schema-refresh` | `5m` | schema re-discovery interval |
| `PROXY_QUERY_TIMEOUT` | `-query-timeout` | `60s` | per-request timeout |
| `PROXY_LOG_QUERIES` | `-log-queries` | `false` | log every generated APL query |
| `PROXY_LOG_LEVEL` | `-log-level` | `info` | log level |

## Deploying

The `Dockerfile` builds a static binary into a distroless image listening
on `:3200`.

[`deploy/`](deploy/) contains a helper for Google Compute Engine. It
installs the proxy as a systemd service over `gcloud compute ssh`:

```bash
AXIOM_TOKEN=xaat-... AXIOM_DATASET=otel-traces-prod \
  deploy/deploy.sh my-instance my-zone
```

## Development

```bash
go test ./...
go vet ./...
gofmt -l .
```

The tests run against an in-process Axiom stub and assert on the
generated APL, so no token or dataset is needed. CI runs the same three
checks.

## Limitations

- A search returns at most `limit` traces, newest first. When the query
  `select()`s attributes, traces carrying them are ranked first so the
  bounded result is useful. `PROXY_NO_PREFER_SELECTED` turns that off.
- Results are never silently truncated. Searches report dropped
  candidates in `metrics.additionalMetrics.droppedTraces`; metrics and
  trace-by-id v2 responses set `status: PARTIAL`.
- Event attribute filters look at the first 8 events of a span.
- Metrics filters on `traceDuration`, `rootName` and `rootServiceName`
  are resolved exactly but bounded by `PROXY_MAX_TRACE_INTRINSIC_TRACES`;
  a query that matches more traces is refused with a 400 instead of
  returning wrong numbers. Details in
  [docs/DESIGN.md](docs/DESIGN.md#trace-level-intrinsics-in-metrics).
- `span:childCount`, `nestedSetLeft`, `nestedSetRight`, `parent.*` and
  `link.*` are not supported in metrics queries.
- Service graph and span metrics are not served.

## License

AGPL-3.0-only. See [LICENSE](LICENSE) and [NOTICE](NOTICE).

The proxy embeds the TraceQL parser and engine from Grafana Tempo, which
is AGPL-3.0 licensed, so the proxy carries the same license.
