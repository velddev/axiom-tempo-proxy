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
internal/server         Tempo HTTP handlers, param parsing, jsonpb/proto encoding
```

The Tempo module (`github.com/grafana/tempo@main`) is a dependency. It
provides the TraceQL parser and execution engine, the protobuf/jsonpb
response types, and the id helpers, so response encoding is byte-for-byte
what Tempo produces.

## Query strategies

**Trace by id.** One APL query: `where trace_id == "..."` over a window
derived from the request's start/end (or a configured lookback). Rows are
grouped by resource into OTLP ResourceSpans/ScopeSpans and returned as
protobuf or jsonpb depending on the Accept header.

**Search (TraceQL).** Hybrid. The spanset filter is translated into an APL
predicate used as a *prefilter* (exact when possible, a superset
otherwise). Query 1 finds candidate trace ids matching the prefilter
(ordered by recency, limited). Query 2 pulls all spans of those traces.
The spans are wrapped in `traceql.Spanset`s with nested-set bounds
computed in memory, and Tempo's own engine (`Engine.ExecuteSearch`)
evaluates the full query, including structural operators, aggregates,
`select`, `by`, and `coalesce`. This gives exact TraceQL semantics with
bounded data transfer.

**Metrics (TraceQL metrics).** Native APL. The metrics stage is parsed
(`rate`, `count_over_time`, `*_over_time`, `quantile_over_time`,
`histogram_over_time`, `compare`, `topk`/`bottomk`, comparisons, `by`)
and rendered as `summarize ... by bin(_time, step), <by attrs>`. The spanset
filter must translate exactly; otherwise the request fails with a clear
error rather than returning wrong numbers.

### Trace-level intrinsics in metrics

`traceDuration`/`trace:duration`, `rootName`/`trace:rootName` and
`rootServiceName`/`trace:rootService` describe a whole trace, so no single
span row can be tested for them. They are resolved in a separate pass and
turned into a `trace_id in (...)` restriction on the metrics query.

*Splitting the filter.* `translate.SplitTrace` flattens the filter's
top-level `&&` chain and sorts each conjunct into the trace-level or the
span-level bucket. A conjunct that mixes both — `{ traceDuration > 2s ||
status = error }` — cannot be split and is refused with a 400; a
disjunction of purely trace-level terms is fine and stays on the trace
side. Span-level conjuncts still go into the metrics query's `where`, so
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
earliest span otherwise — the rule `spans.Trace.finish()` already uses for
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

*Why not `join`.* Measured against a live dataset: `where trace_id in
(<subquery>)` is rejected outright ("the in parameter can not currently
handle table expressions with operations"), and `join kind=inner` silently
truncates its **left** side at 50,000 rows — a join over 510k spans came
back with exactly 50,000 and no warning at all in the response. The right
side does warn (`join_rhs_limit_warning`); the left does not, so a join
would return plausible-looking wrong numbers. Inlining the ids costs one
extra query and about 36 bytes of query body per trace (~180 KB at the
5000 cap, which Axiom accepts) but is exact.

*Limits.* Each of these is a deliberate 400, never a silently wrong number:

- Axiom caps the group cardinality of a `summarize` and flags such a
  result with `isEstimate` and a `max_limit_warning` message; on a busy
  dataset it starts dropping groups at roughly 5000 traces. The trace
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
into a *subset* at the window edges — a trace whose spans are clipped has
a shorter measured duration and would be dropped before the engine ever
saw it — so it is deliberately left out. The cost is that a search whose
only filter is trace-level inspects just the newest `limit * 3` candidate
traces and may come back empty, as with any other relaxed prefilter.

**Tags and tag values.** Tag names come from the dataset field list
(flattened `attributes.*`/`resource.*` fields) plus keys sampled from the
`attributes.custom`/`resource.custom` map fields. Tag values run
`summarize count() by value` with the `q` filter pushed down; an
unparsable `q` degrades to the unfiltered list, as Tempo does.

## Axiom schema assumptions

See `docs/research/axiom-apl.md`. Semantic-convention attributes are flat
dotted fields (`attributes.http.method`); other attributes live in the
`attributes.custom` map; `service.name` is a top-level field; `duration`
is a timespan. The mapping is data-driven from `GET /v2/datasets/{id}/fields`
so a dataset that flattens a different set of attributes still works.
