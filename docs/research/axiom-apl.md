# Axiom trace querying reference for a Tempo to APL proxy

Reference notes compiled from the official Axiom documentation, axiom-go, and axiom-grafana. Items the official sources do not settle are marked **UNVERIFIED** and should be confirmed against a real dataset before being relied on.

## 1. Axiom's OTLP trace schema

Sources: https://axiom.co/docs/query-data/traces, https://axiom.co/docs/apl/data-types/map-fields, https://axiom.co/docs/reference/field-restrictions, language guides under https://axiom.co/docs/guides/opentelemetry-*.

The traces page states: "The following fields are expected to display the OpenTelemetry Traces dashboard: `duration`, `kind`, `name`, `parent_span_id`, `service.name`, `span_id`, and `trace_id`."

| Field | Type | Notes |
|---|---|---|
| `trace_id` | String | hex |
| `span_id` | String | hex |
| `parent_span_id` | String | null on root spans; docs use `isnull(parent_span_id)` |
| `name` | String | span/operation name |
| `kind` | String | lowercase in examples: `client`, `server`, `internal`, `producer`, `consumer` |
| `duration` | Timespan | APL `timespan`; divide by `1ms`/`1ns` to get a number |
| `error` | Boolean | whether the span is an error |
| `status.code` | String | literal casing **UNVERIFIED** (docs say "null, OK, error") |
| `status.message` | String | |
| `attributes` | Object | semantic-convention attrs flattened to `attributes.http.method` etc. |
| `attributes.custom` | Map | non-semconv span attrs; access `['attributes.custom']['my.key']` |
| `resource` | Object | resource attrs; custom ones in `resource.custom` map (or flat `resource.*` on older ingest) |
| `service.name` | String | **top-level** dotted field, not `resource.service.name` |
| `service.version`, `service.instance.id` | String | top-level |
| `scope.name`, `scope.version` | String | instrumentation scope |
| `telemetry.sdk.language/name/version` | String | |
| `events` | Array | element shape **UNVERIFIED** |
| `links` | Array | element shape **UNVERIFIED** (trace view shows trace_id, span_id, attributes) |
| `_time` | datetime | span start |
| `_sysTime` | datetime | ingest time |

Reserved names: `_blockInfo`, `_cursor`, `_rowID`, `_source`, `_sysTime`.

Address dotted fields as `['service.name']`; map keys as `['attributes.custom']['http.protocol']`.

Field type strings seen in axiom-grafana's type switch: `string`, `integer`, `float`, `bool`, `datetime`, `timespan`, `array`, `unknown`. Map type string **UNVERIFIED**.

## 2. Query REST API

Sources: https://axiom.co/docs/restapi/endpoints/queryApl, https://axiom.co/docs/restapi/query, https://axiom.co/docs/reference/edge-deployments, https://axiom.co/docs/restapi/api-limits.

- Endpoint: `POST https://api.axiom.co/v1/datasets/_apl?format=tabular` (also seen: `/v1/query/_apl`). Make path configurable.
- Edge query URLs exist (`https://us-east-1.aws.edge.axiom.co`, `https://eu-central-1.aws.edge.axiom.co`); `api.eu.axiom.co` is deprecated.
- Headers: `Authorization: Bearer <token>`, `Content-Type: application/json`; personal tokens also need `x-axiom-org-id`.
- Query params: `format` (`tabular`|`legacy`), `nocache`, `saveAsKind`, `dataset_name`.
- Body: `apl` (required), `startTime`/`endTime` (RFC3339 or `now-1h`), `cursor`, `includeCursor`, `variables`, `queryOptions.resolution` (seconds or `auto`; drives `bin_auto`).

Tabular response:

```json
{
  "format": "tabular",
  "datasetNames": ["otel-demo-traces"],
  "fieldsMetaMap": {"otel-demo-traces": [{"name":"duration","type":"timespan","unit":"","description":""}]},
  "tables": [{
    "name": "0",
    "sources": [{"name":"otel-demo-traces"}],
    "fields": [{"name":"_time","type":"datetime","hidden":false,"agg":null}, {"name":"count_","type":"integer","agg":{}}],
    "order": [{"field":"_time","desc":true}],
    "groups": [{"name":"service.name"}],
    "range": {"field":"_time","start":"...","end":"..."},
    "buckets": {"field":"_time","size": 60000000000},
    "columns": [[], []]
  }],
  "status": {"elapsedTime":0,"blocksExamined":0,"rowsExamined":0,"rowsMatched":0,"isPartial":false,"isEstimate":false,"cacheStatus":0,"minBlockTime":"...","maxBlockTime":"...","messages":[]}
}
```

`columns` is column-major: `columns[i][row]` aligns with `fields[i]`. Datetimes are RFC3339Nano strings. Timespans come back as strings (axiom-grafana parses with `time.ParseDuration`); safest to convert in APL (`duration / 1ms`) so numbers are returned.

Errors: JSON `{"message": "..."}`. 403 auth, 429 rate limit, 430 limit reached, 432 query memory exceeded. Rate-limit headers `X-RateLimit-*`, `X-QueryLimit-*`. Legacy `limit` default 1000, max 50000. Join sides capped at 50,000 rows. Query timeout not documented.

## 3. APL syntax for the translator

- Names with `.`, `-`, space, or keywords: `['name']`. Case-sensitive.
- Strings: `"..."` or `'...'`, escapes `\"`, `\'`, `\\`, `\n`, `\t`; raw `@"..."`. Regex flavor is RE2.
- `where` operators: `==`/`!=` (case-sensitive), `=~`/`!~` (case-insensitive equality), `contains`/`!contains` (ci), `contains_cs`, `startswith`/`endswith` (ci) + `_cs`, `has`/`!has`, `has_cs`, `matches regex`/`!matches regex` (RE2). Set: `x in ('a','b')`, `!in`, `in~`, `!in~`. Logic: `and`, `or`, `not(...)`. `between` **not documented**; use `x >= a and x <= b`.
- Literals: `datetime(2016-06-26T08:20:03.123456Z)`, `now()`, `now(-5m)`, `ago(1h)`. Timespan: `2d`, `1.5h`, `30m`, `10s`, `0.1s`, `timespan(15s)`; `duration / 1ms` gives a number. `datetime - datetime` → timespan; `datetime + timespan` → datetime.
- Conversions: `tostring`, `tolong`, `toint`, `toreal`, `todouble`, `todatetime` (numbers = nanoseconds), `totimespan` (numbers = nanoseconds), `unixtime_{seconds,milliseconds,microseconds,nanoseconds}_todatetime(n)`, `datetime_diff('nanosecond', a, b)` → long. `format_datetime` does **not** exist; `tostring(datetime)` gives RFC3339Nano.
- `project a, b, c = expr`, `project-away`, `extend x = expr`.
- `summarize [Alias =] Agg [, ...] [by ...]`: `count()`, `countif(pred)`, `dcount(f)`, `avg`, `min`, `max`, `sum` (+ `if` variants), `percentile(f, p)`, `percentiles_array(f, p1, p2, ...)`, `histogram(f, nbins)`, `make_list`, `make_set(f, [limit])`, `arg_max(expr, f1, ..., *)`, `arg_min`, `topk(f, k)`, `rate(f)`, `stdev`, `variance`. Default names: `count_`, `avg_duration`, `dcount_trace_id`. Time bins: `bin(_time, 1m)`, `bin_auto(_time)`. Gotcha: time bin first in `by` + `limit` → global top-N groups per bucket.
- `sort by f desc`, `order by`, `top N by expr`, `limit N` / `take N`, `distinct f1, f2`, `count`.
- `union`: `['a'] | union ['b'], ['c']`, `union withsource=dataset github*`. (Not used: the proxy queries a single dataset per request.)
- `join kind=inner|innerunique|leftouter Right on trace_id`: public preview, only these kinds, 50k rows/side, not usable in dashboard queries. Parenthesized subquery on the right **UNVERIFIED**.
- `let x = dynamic([...]);` documented; `let` bound to a dataset pipeline **UNVERIFIED**. No window functions. `bag_keys(map)` exists; `bag_unpack` does not; `mv-expand` **UNVERIFIED**. `case(c1, r1, ..., default)`, `iff(pred, a, b)`, `isnull`, `isnotnull`, `isempty`, `isnotempty`, `coalesce`, `strcat`, `tolower`, `extract(regex, group, text)`.

## 4. Schema discovery

- `GET /v2/datasets` → `[{id, name, description, created, updatedAt, kind, who, canWrite, retentionDays, useRetentionPeriod, edgeDeployment, edgeDeploymentUrl, mapFields[], sharedByOrg}]`. `mapFields` lists map-typed fields (e.g. `attributes.custom`).
- `GET /v2/datasets/{dataset_id}/fields` → `[{name, type, description, hidden, unit}]`. Use for Tempo tag-name lists.
- Every tabular response includes `fieldsMetaMap`.

## 5. Trace reconstruction and roll-ups

(a) All spans of one trace:

```apl
['otel-demo-traces']
| where trace_id == "4bf92f3577b34da6a3ce929d0e0e4736"
| extend duration_ns = tolong(duration / 1ns), start_unix_ns = tolong(datetime_diff('nanosecond', _time, datetime(1970-01-01)))
| project _time, start_unix_ns, duration_ns, trace_id, span_id, parent_span_id, name, kind,
          ['service.name'], ['status.code'], ['status.message'], error, ['scope.name'],
          ['attributes.custom'], resource, events, links
| sort by _time asc
| limit 50000
```

(b) Trace search with tag filters → per-trace aggregates (join-based, join is preview):

```apl
['otel-demo-traces']
| where ['service.name'] == "frontend" and ['attributes.http.method'] == "POST"
| summarize matched_spans = count() by trace_id
| join kind=inner (
    ['otel-demo-traces']
    | summarize span_count = count(), start = min(_time), end_ = max(_time + duration),
                root_name = arg_min(_time, name), root_service = arg_min(_time, ['service.name'])
      by trace_id
  ) on trace_id
| extend trace_duration_ms = (end_ - start) / 1ms
| sort by start desc
| limit 20
```

Join-free fallback (filters only on per-trace aggregates):

```apl
['otel-demo-traces']
| summarize span_count = count(), start = min(_time), end_ = max(_time + duration),
            root_name = arg_min(_time, name), root_service = arg_min(_time, ['service.name']),
            has_error = max(error)
  by trace_id
| extend trace_duration_ms = (end_ - start) / 1ms
| where root_service == "frontend" and trace_duration_ms > 100
| sort by start desc
| limit 20
```

Root span per docs: `where isnull(parent_span_id)`.

(c) RED metrics per service, 1-minute buckets:

```apl
['otel-demo-traces']
| where kind == "server"
| summarize requests = count(), errors = countif(error == true),
            p50 = percentile(duration / 1ms, 50), p90 = percentile(duration / 1ms, 90), p99 = percentile(duration / 1ms, 99)
  by bin(_time, 1m), ['service.name']
| extend rate_per_s = requests / 60.0, error_rate = todouble(errors) / requests
| sort by _time asc
```

(d) Tag value autocomplete:

```apl
['otel-demo-traces']
| where isnotempty(['attributes.http.method'])
| summarize c = count() by v = ['attributes.http.method']
| top 100 by c
```

Map key enumeration (tag names inside `attributes.custom`):

```apl
['otel-demo-traces']
| where isnotnull(['attributes.custom'])
| take 2000
| extend k = bag_keys(['attributes.custom'])
| distinct tostring(k)
```

## 6. Axiom's own Grafana support

- axiom-grafana has no Tempo-compatible API. Its `pkg/plugin/traces.go` builds a Grafana trace frame when a result has trace fields; aliases: traceID (`trace_id`, `traceID`, `traceId`, `trace.id`), spanID, operationName (`name`, `operationName`, `span.name`), serviceName (`service.name`, `resource.service.name`, `service_name`), startTime (`_time`, `timestamp`, `startTime`), duration (`duration`, `durationMs`, `durationNs`). Bare numeric `duration` treated as nanoseconds. Flattens `attributes` maps into dot-keyed tags, turns `events` into logs.

## 7. Observed query-engine behaviour

Behaviour of the query engine that the documentation does not state, and
that a proxy generating APL has to work around.

- **`where x in (<subquery>)` does not exist.** A parenthesised pipeline on
  the right of `in` is a hard 400: *"the in parameter can not currently
  handle table expressions with operations"*. Only literal lists work.
- **`join` silently truncates its left side at 50,000 rows.** An oversized
  left side comes back at exactly 50,000 rows with `isEstimate` unset and
  no message at all. An oversized right side does warn
  (`join_rhs_limit_warning`). Treat `join` as unusable for anything that
  must be exact.
- **`summarize ... by <high-cardinality>` truncates.** Grouping a large
  span volume by `trace_id` returns a fraction of the groups, and the
  number varies between runs. Truncation comes with
  `status.isEstimate = true` plus a `max_limit_warning` message; narrow
  queries of a few thousand groups are exact and carry neither flag. An
  explicit `| limit N` raises the same two flags, so a query that uses
  them as a truncation signal must not carry one. End it with a
  single-row `summarize count(), make_list(f, N)` instead.
- **`make_list(f, N)`** caps the list at N while `count()` in the same
  `summarize` still reports the true number, which is how overflow is
  detected. An empty input yields one row with `0` and `[]`.
- **Per-trace roll-ups.** `max(_time + duration) - min(_time)` returns a
  bare nanosecond number; comparisons against `2s`, `timespan(2s)` and
  `2000000000` all agree. `todatetime(max(...)) - todatetime(min(...))`
  returns a real timespan.
  `summarize r = arg_min(key, a, b)` names its outputs `a` and `b`, not
  `r_a`, so the values must be aliased with `extend` first.
- **A 5000-element literal `in (...)` list**, roughly 180 KB of query
  body, is accepted.

## Open items to confirm against a real dataset

1. `status.code` literals and casing; `kind` casing.
2. Structure of `events[]` and `links[]`.
3. `resource.custom` vs flat `resource.*` per dataset.
4. Wire encoding of `timespan` in tabular JSON.
5. `let` pipelines, `mv-expand`, `between`.
6. Query timeouts, and the exact rule behind the `summarize` group cap.
