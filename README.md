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
| gRPC streaming | no (disable streaming on the datasource) |

## How it works

- **Trace by id** is one APL query on `trace_id`, converted to OTLP.
- **Search** pushes the query's spanset filters down to APL to find
  candidate traces, pulls the spans of those traces, and runs Tempo's own
  TraceQL engine in-process over them. Structural operators, aggregates,
  `select`, `by`, and `coalesce` all work exactly as in Tempo.
- **Search ranking**: a search returns at most `limit` traces, newest
  first. When the query `select()`s attributes (Drilldown's Exceptions tab
  selects `event.exception.*`), traces that actually carry those
  attributes are ranked ahead of traces that don't, so the bounded result
  shows the data the caller asked for instead of being dominated by, say,
  exception-free errors. Every returned trace still matches the filter.
  Disable with `PROXY_NO_PREFER_SELECTED=true`.
- **Metrics** are translated to native APL `summarize ... by bin(_time, step)`
  aggregations, so they scale with Axiom rather than with the proxy.
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
| `PROXY_MAX_SPANS_PER_FETCH` | `-max-spans-per-fetch` | `50000` | cap on spans pulled per query |
| `PROXY_MAX_TAG_VALUES` | `-max-tag-values` | `5000` | cap on tag values |
| `PROXY_SCHEMA_REFRESH` | `-schema-refresh` | `5m` | schema re-discovery interval |
| `PROXY_QUERY_TIMEOUT` | `-query-timeout` | `60s` | per-request timeout |
| `PROXY_LOG_QUERIES` | `-log-queries` | `false` | log every generated APL query |
| `PROXY_LOG_LEVEL` | `-log-level` | `info` | log level |

## Development

```bash
go test ./...
```
