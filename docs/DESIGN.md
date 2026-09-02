# Design

axiom-tempo-proxy exposes the Grafana Tempo query API and answers every
request from an Axiom dataset of OpenTelemetry spans by generating APL.
One proxy request always targets one dataset: datasets are split per
environment, so a Grafana datasource maps to one dataset (configured, or
selected per request through a header).

## Components

```
cmd/axiom-tempo-proxy   main: config, wiring, HTTP server
internal/config         env/flag configuration
internal/axiom          Axiom REST client: APL query (tabular), datasets, fields
internal/apl            APL string builder helpers (quoting, literals, pipeline)
internal/schema         TraceQL attribute -> Axiom column mapping, driven by the
                        dataset's discovered field list
internal/translate      TraceQL AST -> APL where predicates; metrics-stage parser
internal/fetch          traceql.SpansetFetcher backed by Axiom (search path)
internal/metrics        TraceQL metrics -> native APL summarize queries
internal/convert        Axiom rows -> OTLP protobuf trace (trace by id)
internal/server         Tempo HTTP handlers, the StreamingQuerier gRPC service,
                        param parsing, jsonpb/proto encoding
```

The Tempo module (`github.com/grafana/tempo@main`) is a dependency. It
provides the TraceQL parser and execution engine, the protobuf/jsonpb
response types, and the id helpers, so response encoding is byte-for-byte
what Tempo produces.

Every query endpoint has one implementation shared by both transports: a
`run*` function on the server takes the parsed request and returns the
tempopb response or an error. The HTTP handler parses query parameters
and writes the message; the gRPC method reads the same request off the
wire and sends it as the single final message of the stream. Errors carry
their HTTP status (`statusError`, `queryErrorStatus`) and the gRPC side
maps that status to a code, so both transports classify failures the same
way.

## Query strategies

**Trace by id.** One APL query: `where trace_id == "..."` over a window
derived from the request's start/end (or a configured lookback). Rows are
grouped by resource into OTLP ResourceSpans/ScopeSpans and returned as
protobuf or jsonpb depending on the Accept header.

**Search (TraceQL).** Hybrid. The spanset filter is translated into an APL
predicate used as a *prefilter* (exact when possible, a superset
otherwise). Query 1 finds candidate trace ids matching the prefilter
(ordered by recency, limited). The queries after it pull the spans of
those traces in batches of candidate ids (`PROXY_SEARCH_BATCH_TRACES`, one
query when they all fit), in candidate order, until the span budget
(`PROXY_MAX_SPANS_PER_FETCH`) is spent, projecting only the columns the
query's attributes need. The spans are wrapped in
`traceql.Spanset`s with nested-set bounds computed in memory, and Tempo's
own engine (`Engine.ExecuteSearch`) evaluates the full query, including
structural operators, aggregates, `select`, `by`, and `coalesce`. This
gives exact TraceQL semantics with bounded data transfer.

**Partial results.** A half-fetched trace is worse than a missing one: it
makes structural operators and aggregates lie. So each batch asks for one
row more than the budget allows and sorts by `trace_id`, which makes an
overflow both detectable and aligned to a trace boundary — the trailing
trace, and every candidate behind it, is dropped rather than returned
incomplete. Dropped candidates are counted in `fetch.Stats` and reported
as `metrics.additionalMetrics.droppedTraces` on the search response
(`SearchResponse` has no status field), plus a warning log carrying the
query. Where the protobuf does have a status it is used: metrics
`query_range`/`query` and trace-by-id v2 set `PARTIAL` with a message when
a trace hit the span cap or Axiom's result reports `status.isPartial`, and
Axiom's status messages are passed through in that message.

Query 2 `project`s only the columns the fetch request needs: every
intrinsic the dataset has, the top-level resource fields (`service.*`,
`telemetry.*`), and the columns backing the attributes the request
references — a flat column, a custom map when the attribute lives in one,
`events`/`links` for event- and link-scoped attributes. The engine only
ever reports the attributes the query mentioned (`spans.AttrSet`, derived
from the same conditions), so the projection cannot change results. It is
skipped when the request selects every attribute or when the schema was
not discovered, since a `project` naming a column the dataset lacks fails
the query with a 400. Trace by id never projects: the trace view shows
everything.

**Metrics (TraceQL metrics).** Native APL. The metrics stage is parsed
(`rate`, `count_over_time`, `*_over_time`, `quantile_over_time`,
`histogram_over_time`, `compare`, `topk`/`bottomk`, comparisons, `by`)
and rendered as `summarize ... by bin(_time, step), <by attrs>`. The spanset
filter must translate exactly; otherwise the request fails with a clear
error rather than returning wrong numbers.

**Exemplars.** They ride along on that same query rather than costing a
second one. The trace id and timestamp are aliased first (`extend
_exid = trace_id, _exts = _time`, because `arg_max` names its extra
result columns after the columns it is given and `_time` would collide
with the `bin()` grouping key), and an `_exv = arg_max(<value>, _exid,
_exts)` aggregation is appended to the `summarize`. That yields one span
per bucket per series: the extremum for the `*_over_time` functions
(`arg_min` for `min_over_time`, so the exemplar is the span the series
value reports) and the slowest span in the bucket for the counting
functions, which have no per-span value; `coalesce(..., 0.0)` keeps a
trace id even for buckets whose spans all lack a duration. Values follow
Tempo: the span's own value in seconds for the attribute functions, the
bucket's sample value for `rate`, `count_over_time`, and
`histogram_over_time` (Tempo emits NaN placeholders for those and fills
them in the query frontend the same way). For `quantile_over_time` the
exemplar goes to the one quantile series closest to it by value, as
Tempo's `assignExemplarToQuantile` does. `compare()` and instant queries
carry none. The result is thinned to an evenly spread subset of the
requested count, and exemplars Grafana would discard (value 0, or a
non-positive timestamp) are never emitted.

**Tags and tag values.** Tag names come from the dataset field list
(flattened `attributes.*`/`resource.*` fields) plus keys sampled from the
`attributes.custom`/`resource.custom` map fields. Tag values run
`summarize count() by value` with the `q` filter pushed down; an
unparsable `q` degrades to the unfiltered list, as Tempo does.

The `event` scope is listed too, from keys sampled out of the `events`
array (`take N | mv-expand events | project bag_keys(events['attributes'])`,
unioned over the rows) and cached with the rest of the schema. Event tag
values (`event.exception.message`, `event:name`) expand the array as
well: the `q` filter is applied to whole spans first, where the per-slot
`events[i]` expressions the translator emits are still meaningful, and
only then are the rows expanded to one event each and grouped. APL will
not group by a value of unknown type, which is what indexing into a
dynamic yields, so event values are always strings. `mv-expand` takes no
alias, so the expanded element keeps the column's own name. A scope with
no tags is omitted, so `link` never appears on a dataset without a
`links` column, and no query names one: APL fails a query outright when
it references a field the dataset does not have.

## Axiom schema assumptions

See `docs/research/axiom-apl.md`. Semantic-convention attributes are flat
dotted fields (`attributes.http.method`); other attributes live in the
`attributes.custom` map; `service.name` is a top-level field; `duration`
is a timespan. The mapping is data-driven from `GET /v2/datasets/{id}/fields`
so a dataset that flattens a different set of attributes still works.
