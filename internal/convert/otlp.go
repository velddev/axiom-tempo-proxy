// Package convert turns the in-memory span model into Tempo protobuf
// responses.
package convert

import (
	"sort"
	"strings"

	"github.com/grafana/tempo/pkg/tempopb"
	common "github.com/grafana/tempo/pkg/tempopb/common/v1"
	resource "github.com/grafana/tempo/pkg/tempopb/resource/v1"
	v1 "github.com/grafana/tempo/pkg/tempopb/trace/v1"
	"github.com/grafana/tempo/pkg/traceql"

	"github.com/velddev/axiom-tempo-proxy/internal/spans"
)

// ToOTLP builds an OTLP trace from a grouped trace. Spans are grouped
// into ResourceSpans by their resource attributes and into ScopeSpans by
// instrumentation scope, matching how an OTLP exporter would batch them.
func ToOTLP(t *spans.Trace) *tempopb.Trace {
	type scopeGroup struct {
		scope *common.InstrumentationScope
		spans []*v1.Span
	}
	type resGroup struct {
		res    *resource.Resource
		scopes []*scopeGroup
		byKey  map[string]*scopeGroup
	}
	var groups []*resGroup
	byRes := map[string]*resGroup{}

	for _, s := range t.Spans {
		rk := resourceKey(s)
		rg, ok := byRes[rk]
		if !ok {
			rg = &resGroup{res: &resource.Resource{Attributes: keyValues(s.ResourceAttrs)}, byKey: map[string]*scopeGroup{}}
			byRes[rk] = rg
			groups = append(groups, rg)
		}
		sk := s.ScopeName + "\x00" + s.ScopeVersion
		sg, ok := rg.byKey[sk]
		if !ok {
			// Grafana's transformer dereferences Scope and Status without
			// nil checks, so always populate them.
			sg = &scopeGroup{scope: &common.InstrumentationScope{Name: s.ScopeName, Version: s.ScopeVersion}}
			rg.byKey[sk] = sg
			rg.scopes = append(rg.scopes, sg)
		}
		sg.spans = append(sg.spans, toSpan(s))
	}

	out := &tempopb.Trace{}
	for _, rg := range groups {
		rs := &v1.ResourceSpans{Resource: rg.res}
		for _, sg := range rg.scopes {
			rs.ScopeSpans = append(rs.ScopeSpans, &v1.ScopeSpans{Scope: sg.scope, Spans: sg.spans})
		}
		out.ResourceSpans = append(out.ResourceSpans, rs)
	}
	return out
}

func toSpan(s *spans.Span) *v1.Span {
	span := &v1.Span{
		TraceId:           s.TraceID,
		SpanId:            s.SpanID,
		ParentSpanId:      s.ParentSpanID,
		TraceState:        s.TraceState,
		Name:              s.Name,
		Kind:              kind(s.Kind),
		StartTimeUnixNano: s.StartNs,
		EndTimeUnixNano:   s.StartNs + s.DurationNs,
		Attributes:        keyValues(s.SpanAttrs),
		Status:            &v1.Status{Code: statusCode(s.Status), Message: s.StatusMessage},
	}
	for _, e := range s.Events {
		span.Events = append(span.Events, &v1.Span_Event{
			TimeUnixNano: e.TimeNs,
			Name:         e.Name,
			Attributes:   keyValues(e.Attrs),
		})
	}
	for _, l := range s.Links {
		span.Links = append(span.Links, &v1.Span_Link{
			TraceId:    l.TraceID,
			SpanId:     l.SpanID,
			TraceState: l.TraceState,
			Attributes: keyValues(l.Attrs),
		})
	}
	return span
}

func keyValues(attrs []spans.Attr) []*common.KeyValue {
	if len(attrs) == 0 {
		return nil
	}
	out := make([]*common.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		out = append(out, &common.KeyValue{Key: a.Key, Value: a.Val.AsAnyValue()})
	}
	return out
}

func resourceKey(s *spans.Span) string {
	keys := make([]string, 0, len(s.ResourceAttrs))
	for _, a := range s.ResourceAttrs {
		keys = append(keys, a.Key+"="+a.Val.EncodeToString(false))
	}
	sort.Strings(keys)
	return s.ServiceName + "\x00" + strings.Join(keys, "\x00")
}

func kind(k traceql.Kind) v1.Span_SpanKind {
	switch k {
	case traceql.KindInternal:
		return v1.Span_SPAN_KIND_INTERNAL
	case traceql.KindServer:
		return v1.Span_SPAN_KIND_SERVER
	case traceql.KindClient:
		return v1.Span_SPAN_KIND_CLIENT
	case traceql.KindProducer:
		return v1.Span_SPAN_KIND_PRODUCER
	case traceql.KindConsumer:
		return v1.Span_SPAN_KIND_CONSUMER
	}
	return v1.Span_SPAN_KIND_UNSPECIFIED
}

func statusCode(s traceql.Status) v1.Status_StatusCode {
	switch s {
	case traceql.StatusOk:
		return v1.Status_STATUS_CODE_OK
	case traceql.StatusError:
		return v1.Status_STATUS_CODE_ERROR
	}
	return v1.Status_STATUS_CODE_UNSET
}
