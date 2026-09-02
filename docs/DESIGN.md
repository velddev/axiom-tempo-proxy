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
(ordered by recency, limited). Query 2 pulls the spans of those traces.
The spans are wrapped in `traceql.Spanset`s with nested-set bounds
computed in memory, and Tempo's own engine (`Engine.ExecuteSearch`)
evaluates the full query, including structural operators, aggregates,
`select`, `by`, and `coalesce`. This gives exact TraceQL semantics with
bounded data transfer.

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
