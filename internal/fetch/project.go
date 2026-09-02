package fetch

import (
	"sort"

	"github.com/grafana/tempo/pkg/traceql"

	"github.com/velddev/axiom-tempo-proxy/internal/apl"
	"github.com/velddev/axiom-tempo-proxy/internal/schema"
)

// projectColumns returns the dataset columns the span pull needs to
// answer one fetch request, or nil to fetch every column.
//
// The pull is by far the most expensive query a search runs: it returns
// whole spans for up to MaxSpans rows, and a real dataset has a couple of
// hundred columns of which a query touches a handful. Projecting cuts the
// payload without changing results, because the engine only ever sees the
// attributes NewAttrSet exposes, which is the same set of conditions this
// is derived from.
//
// The returned columns are always ones the discovered schema has: APL
// fails the whole query with a 400 "invalid field" when a project stage
// names a column the dataset does not know.
//
// Nothing is projected when the request asks for every attribute
// (SecondPassSelectAll) or when the schema was not discovered, since then
// the set of existing columns is unknown.
func projectColumns(m *schema.Mapping, req traceql.FetchSpansRequest) []string {
	if m == nil || !m.Discovered() || req.SecondPassSelectAll {
		return nil
	}
	cfg := m.Config()
	seen := map[string]bool{}
	var out []string
	add := func(name string) {
		if name == "" || seen[name] || !m.HasField(name) {
			return
		}
		seen[name] = true
		out = append(out, name)
	}

	// Intrinsics are cheap scalars and are needed by everything: trace
	// metadata reads the name and service, grouping and the structural
	// operators read the ids, and the engine reads status, kind and
	// duration.
	for _, name := range []string{
		cfg.Time, cfg.TraceID, cfg.SpanID, cfg.ParentSpanID, cfg.Name, cfg.Kind,
		cfg.Duration, cfg.StatusCode, cfg.StatusMessage, cfg.Error,
		cfg.ServiceName, cfg.ScopeName, cfg.ScopeVersion, cfg.TraceState,
	} {
		add(name)
	}
	// Top-level resource fields (service.*, telemetry.*) carry the
	// resource identity used for grouping and for TraceSearchMetadata.
	for _, name := range cfg.TopLevelResourceFields {
		add(name)
	}

	extra := map[string]bool{}
	for _, conds := range [][]traceql.Condition{req.Conditions, req.SecondPassConditions} {
		for _, c := range conds {
			for _, f := range conditionFields(m, c.Attribute) {
				extra[f] = true
			}
		}
	}
	names := make([]string, 0, len(extra))
	for name := range extra {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		add(name)
	}
	return out
}

// conditionFields lists the dataset fields that can hold the value of one
// referenced attribute. Fields that do not exist are filtered out by the
// caller.
func conditionFields(m *schema.Mapping, a traceql.Attribute) []string {
	cfg := m.Config()
	switch {
	case a.Scope == traceql.AttributeScopeEvent,
		a.Intrinsic == traceql.IntrinsicEventName,
		a.Intrinsic == traceql.IntrinsicEventTimeSinceStart:
		return []string{cfg.Events}
	case a.Scope == traceql.AttributeScopeLink,
		a.Intrinsic == traceql.IntrinsicLinkTraceID,
		a.Intrinsic == traceql.IntrinsicLinkSpanID:
		return []string{cfg.Links}
	case a.Intrinsic != traceql.IntrinsicNone:
		// Span intrinsics are always projected; trace-level and nested-set
		// intrinsics are computed in memory.
		return nil
	}
	switch a.Scope {
	case traceql.AttributeScopeSpan:
		return []string{m.SpanAttribute(a.Name).Field}
	case traceql.AttributeScopeResource:
		return []string{m.ResourceAttribute(a.Name).Field}
	case traceql.AttributeScopeNone:
		// Unscoped: the value may sit on the span or on the resource, in a
		// flat column or in a custom map. Project every place it can be.
		return []string{m.SpanAttribute(a.Name).Field, m.ResourceAttribute(a.Name).Field}
	}
	return nil
}

// projectExprs renders column names as APL identifiers.
func projectExprs(cols []string) []string {
	out := make([]string, 0, len(cols))
	for _, c := range cols {
		out = append(out, apl.Ident(c))
	}
	return out
}
