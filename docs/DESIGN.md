# Design

axiom-tempo-proxy exposes the Grafana Tempo query API and answers every
request from an Axiom dataset of OpenTelemetry spans by generating APL.
One proxy request always targets exactly one dataset, either the
configured default or one selected per request by URL prefix, header or
query parameter.

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
overflow both detectable and aligned to a trace boundary. The trailing
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
references: a flat column, a custom map when the attribute lives in one,
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

### Trace-level intrinsics in metrics

`traceDuration`/`trace:duration`, `rootName`/`trace:rootName` and
`rootServiceName`/`trace:rootService` describe a whole trace, so no single
span row can be tested for them. They are resolved in a separate pass and
turned into a `trace_id in (...)` restriction on the metrics query.

*Splitting the filter.* `translate.SplitTrace` flattens the filter's
top-level `&&` chain and sorts each conjunct into the trace-level or the
span-level bucket. A conjunct that mixes both, such as
`{ traceDuration > 2s || status = error }`, cannot be split and is refused
with a 400. A disjunction of purely trace-level terms is fine and stays on
the trace side. Span-level conjuncts still go into the metrics query's
`where`, so
`{ rootServiceName = "web" && status = error } | rate() by (name)` counts
only the error spans of the web-rooted traces.

*Computing the per-trace values.* One APL query aggregates by `trace_id`:

```apl
['ds']
| where isnotempty(trace_id)
| extend _rk = iff(isempty(parent_span_id), datetime(1970-01-01T00:00:00Z), _time),
         _rn = name, _rs = ['service.name']
| summarize _td = todatetime(max((_time + duration))) - todatetime(min(_time)),
            _r = arg_min(_rk, _rn, _rs)
  by trace_id
| where _rs == "web"
| summarize _n = count(), _ids = make_list(trace_id, 5000)
```

`_td` is the trace duration (last span end minus first span start). The
`arg_min` sort key puts parentless spans ahead of every real timestamp, so
`_rn`/`_rs` come from the root span when the trace has one and from the
earliest span otherwise, the same rule `spans.Trace.finish()` uses for
search. Only the values a query actually needs are computed. The final
`summarize` returns the exact number of qualifying traces alongside a
bounded id list, so overflow is detected rather than silently clipped.

*Narrowing.* The aggregation is the expensive part, so the traces it looks
at are narrowed first wherever that cannot change the result:

- when the query has a span-level filter, only traces holding a span that
  matches it can contribute to the metric, so a first query lists those
  trace ids and the aggregation runs over them;
- otherwise, when the trace-level filter only looks at the root span, it
  is pushed onto root spans directly
  (`where (isempty(parent_span_id)) and (name == "GET /x")`);
- otherwise the aggregation runs over every trace in the window.

*Why not `join`.* APL rejects `where trace_id in (<subquery>)` outright
("the in parameter can not currently handle table expressions with
operations"). `join kind=inner` silently truncates its **left** side at
50,000 rows: an oversized left side comes back at exactly 50,000 rows with
no warning in the response, while an oversized right side does warn
(`join_rhs_limit_warning`). A join would therefore return
plausible-looking wrong numbers. Inlining the ids costs one extra query
and about 36 bytes of query body per trace, roughly 180 KB at the 5000
cap, which Axiom accepts. It is exact.

*Limits.* Each of these is a deliberate 400, never a silently wrong number:

- Axiom caps the group cardinality of a `summarize` and flags a truncated
  result with `isEstimate` and a `max_limit_warning` message. The trace
  queries carry no explicit `limit` (which raises the same flags on its
  own), so the flag means truncation and the request is refused with
  *"too many traces in the query window to evaluate the trace-level
  filter exactly"*.
  Shortening the range or adding a selective span filter fixes it.
- More than `PROXY_MAX_TRACE_INTRINSIC_TRACES` (5000) qualifying traces is
  refused for the same reason.
- Trace values are computed from the spans **inside the query window**, so
  a trace clipped by a window edge gets a shorter `traceDuration`, and a
  trace whose root span falls outside the window falls back to its
  earliest in-window span. The root-span narrowing above additionally only
  finds traces whose root span is in the window.
- `compare()` selections split each bucket per span, so a trace-level
  selection is refused.
- `span:childCount`, `nestedSetLeft`/`nestedSetRight`, `parent.*` and
  `link.*` need the span tree and stay unsupported, with the existing
  errors.

*Search is unchanged.* `translate.Filter` still relaxes trace intrinsics
to `true`, so the search prefilter stays a superset and Tempo's engine
evaluates them exactly on the fetched spansets (`traceql.Spanset` carries
`RootSpanName`, `RootServiceName` and `DurationNanos`). Pushing the
trace-level predicate into the candidate query would turn the prefilter
into a *subset* at the window edges, because a trace whose spans are
clipped has a shorter measured duration and would be dropped before the
engine ever saw it. So it is deliberately left out. The cost is that a
search whose only filter is trace-level inspects just the newest
`limit * 3` candidate traces and may come back empty, as with any other
relaxed prefilter.

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
