// Package schema maps TraceQL attributes and intrinsics onto the columns of
// an Axiom OpenTelemetry trace dataset.
//
// Axiom flattens well-known (semantic convention) attributes into dotted
// top-level fields such as attributes.http.method or resource.k8s.pod.name,
// and stores everything else inside map fields (attributes.custom,
// resource.custom). Which attributes are flat can only be known by looking
// at the dataset's field list, so a Mapping is built from the dataset's
// discovered fields when available and falls back to a coalesce of both
// forms otherwise.
package schema

import (
	"fmt"
	"sort"
	"strings"

	"github.com/grafana/tempo/pkg/traceql"

	"github.com/velddev/axiom-tempo-proxy/internal/apl"
	"github.com/velddev/axiom-tempo-proxy/internal/axiom"
)

// Type is the value type of a column as far as the translator cares.
type Type int

const (
	TypeUnknown Type = iota
	TypeString
	TypeInt
	TypeFloat
	TypeBool
	TypeDuration
	TypeDatetime
	TypeArray
	TypeMap
)

func (t Type) String() string {
	switch t {
	case TypeString:
		return "string"
	case TypeInt:
		return "int"
	case TypeFloat:
		return "float"
	case TypeBool:
		return "bool"
	case TypeDuration:
		return "duration"
	case TypeDatetime:
		return "datetime"
	case TypeArray:
		return "array"
	case TypeMap:
		return "map"
	}
	return "unknown"
}

// Column is an APL expression that yields an attribute's value.
type Column struct {
	// Expr is the APL expression.
	Expr string
	// Type is the column's type when known.
	Type Type
	// Field is the underlying dataset field name (without map key) when
	// the column is a direct field reference.
	Field string
	// MapKey is true when the column reads a key out of a map field.
	MapKey bool
	// Missing is true when the schema is known and the attribute cannot
	// exist in it.
	Missing bool
}

// Config names the dataset columns holding intrinsic span data. Zero
// values take the Axiom OTel defaults.
type Config struct {
	Time          string
	TraceID       string
	SpanID        string
	ParentSpanID  string
	Name          string
	Kind          string
	Duration      string
	StatusCode    string
	StatusMessage string
	Error         string
	ServiceName   string
	ScopeName     string
	ScopeVersion  string
	Events        string
	Links         string
	TraceState    string

	// SpanAttrPrefix is the prefix of flattened span attributes.
	SpanAttrPrefix string
	// SpanCustomMap is the map field holding non-flattened span attributes.
	SpanCustomMap string
	// ResourceAttrPrefix is the prefix of flattened resource attributes.
	ResourceAttrPrefix string
	// ResourceCustomMap is the map field holding non-flattened resource
	// attributes.
	ResourceCustomMap string
	// TopLevelResourceFields are resource attributes stored as top-level
	// fields under their own name (e.g. service.name).
	TopLevelResourceFields []string
	// MaxEventsPerSpan bounds how many event slots are inspected when a
	// query filters on event attributes; APL has no per-span "any element
	// matches" construct, so the test is repeated per index.
	MaxEventsPerSpan int
}

// DefaultConfig returns the Axiom OTel trace dataset layout.
func DefaultConfig() Config {
	return Config{
		Time:               "_time",
		TraceID:            "trace_id",
		SpanID:             "span_id",
		ParentSpanID:       "parent_span_id",
		Name:               "name",
		Kind:               "kind",
		Duration:           "duration",
		StatusCode:         "status.code",
		StatusMessage:      "status.message",
		Error:              "error",
		ServiceName:        "service.name",
		ScopeName:          "scope.name",
		ScopeVersion:       "scope.version",
		Events:             "events",
		Links:              "links",
		TraceState:         "trace_state",
		SpanAttrPrefix:     "attributes.",
		SpanCustomMap:      "attributes.custom",
		ResourceAttrPrefix: "resource.",
		ResourceCustomMap:  "resource.custom",
		TopLevelResourceFields: []string{
			"service.name", "service.version", "service.instance.id", "service.namespace",
			"telemetry.sdk.language", "telemetry.sdk.name", "telemetry.sdk.version",
		},
		MaxEventsPerSpan: 8,
	}
}

func (c Config) withDefaults() Config {
	d := DefaultConfig()
	set := func(dst *string, def string) {
		if *dst == "" {
			*dst = def
		}
	}
	set(&c.Time, d.Time)
	set(&c.TraceID, d.TraceID)
	set(&c.SpanID, d.SpanID)
	set(&c.ParentSpanID, d.ParentSpanID)
	set(&c.Name, d.Name)
	set(&c.Kind, d.Kind)
	set(&c.Duration, d.Duration)
	set(&c.StatusCode, d.StatusCode)
	set(&c.StatusMessage, d.StatusMessage)
	set(&c.Error, d.Error)
	set(&c.ServiceName, d.ServiceName)
	set(&c.ScopeName, d.ScopeName)
	set(&c.ScopeVersion, d.ScopeVersion)
	set(&c.Events, d.Events)
	set(&c.Links, d.Links)
	set(&c.TraceState, d.TraceState)
	set(&c.SpanAttrPrefix, d.SpanAttrPrefix)
	set(&c.SpanCustomMap, d.SpanCustomMap)
	set(&c.ResourceAttrPrefix, d.ResourceAttrPrefix)
	set(&c.ResourceCustomMap, d.ResourceCustomMap)
	if c.TopLevelResourceFields == nil {
		c.TopLevelResourceFields = d.TopLevelResourceFields
	}
	if c.MaxEventsPerSpan <= 0 {
		c.MaxEventsPerSpan = d.MaxEventsPerSpan
	}
	return c
}

// EventSlots returns, for an event attribute name, one column expression
// per inspected event slot, e.g. events[0]['attributes']['exception.type'].
// The bool is false when the dataset has no events column. An empty name
// addresses the event's own name (event:name).
func (m *Mapping) EventSlots(name string) ([]string, bool) {
	ev := m.Events()
	if ev.Missing {
		return nil, false
	}
	out := make([]string, 0, m.cfg.MaxEventsPerSpan)
	for i := 0; i < m.cfg.MaxEventsPerSpan; i++ {
		slot := fmt.Sprintf("%s[%d]", ev.Expr, i)
		if name == "" {
			out = append(out, slot+"['name']")
		} else {
			out = append(out, slot+"['attributes']['"+escapeKey(name)+"']")
		}
	}
	return out, true
}

// ExpandedEvent returns the APL expression for one field of a single event
// row, as produced by an `mv-expand <events>` stage. APL rejects an alias
// on mv-expand ("expand doesn't currently support aliases"), so after the
// expansion the column keeps its own name and holds one element. An empty
// name addresses the event's own name, as EventSlots does.
func (m *Mapping) ExpandedEvent(name string) (string, bool) {
	ev := m.Events()
	if ev.Missing {
		return "", false
	}
	if name == "" {
		return ev.Expr + "['name']", true
	}
	return ev.Expr + "['attributes']['" + escapeKey(name) + "']", true
}

// ExpandedEventAttributes returns the APL expression for the attribute bag
// of a single expanded event row, for key enumeration with bag_keys.
func (m *Mapping) ExpandedEventAttributes() (string, bool) {
	ev := m.Events()
	if ev.Missing {
		return "", false
	}
	return ev.Expr + "['attributes']", true
}

// ExpandedLinkAttributes is ExpandedEventAttributes for the links array.
// Datasets ingested by Axiom's OTel exporters do not always have a links
// column, so callers must check the bool before referencing it: APL fails
// a query outright when it names a field the dataset does not have.
func (m *Mapping) ExpandedLinkAttributes() (string, bool) {
	ln := m.Links()
	if ln.Missing {
		return "", false
	}
	return ln.Expr + "['attributes']", true
}

func escapeKey(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	return strings.ReplaceAll(s, `'`, `\'`)
}

// Mapping resolves TraceQL attributes to dataset columns.
type Mapping struct {
	cfg Config
	// fields is the discovered dataset schema, nil when unknown.
	fields   map[string]Type
	topLevel map[string]bool
}

// New builds a Mapping. fields may be nil when the dataset schema has not
// been discovered; the mapping then guesses column locations.
func New(cfg Config, fields []axiom.DatasetField) *Mapping {
	m := &Mapping{cfg: cfg.withDefaults(), topLevel: map[string]bool{}}
	for _, f := range m.cfg.TopLevelResourceFields {
		m.topLevel[f] = true
	}
	if fields != nil {
		m.fields = make(map[string]Type, len(fields))
		for _, f := range fields {
			m.fields[f.Name] = typeFromAxiom(f.Type)
		}
	}
	return m
}

// Config returns the effective column configuration.
func (m *Mapping) Config() Config { return m.cfg }

// Discovered reports whether the dataset schema was available.
func (m *Mapping) Discovered() bool { return m.fields != nil }

// HasField reports whether the dataset is known to have the field. It is
// always false when the schema was not discovered.
func (m *Mapping) HasField(name string) bool {
	_, ok := m.fields[name]
	return ok
}

// FieldType returns the discovered type of a field.
func (m *Mapping) FieldType(name string) Type {
	return m.fields[name]
}

// Field returns a direct reference to a dataset column. When the schema
// is known and the column is absent the result is flagged Missing, since
// APL rejects references to unknown fields outright.
func (m *Mapping) Field(name string) Column {
	c := Column{Expr: apl.Ident(name), Type: m.fields[name], Field: name}
	if m.fields != nil {
		if _, ok := m.fields[name]; !ok {
			c.Missing = true
		}
	}
	return c
}

// SpansOnly is a predicate restricting rows to spans, for datasets that
// also hold logs.
func (m *Mapping) SpansOnly() string {
	return apl.Call("isnotempty", m.TraceID().Expr)
}

// Intrinsic columns.
func (m *Mapping) Time() Column          { return m.typed(m.cfg.Time, TypeDatetime) }
func (m *Mapping) TraceID() Column       { return m.typed(m.cfg.TraceID, TypeString) }
func (m *Mapping) SpanID() Column        { return m.typed(m.cfg.SpanID, TypeString) }
func (m *Mapping) ParentSpanID() Column  { return m.typed(m.cfg.ParentSpanID, TypeString) }
func (m *Mapping) Name() Column          { return m.typed(m.cfg.Name, TypeString) }
func (m *Mapping) Kind() Column          { return m.typed(m.cfg.Kind, TypeString) }
func (m *Mapping) Duration() Column      { return m.typed(m.cfg.Duration, TypeDuration) }
func (m *Mapping) StatusCode() Column    { return m.typed(m.cfg.StatusCode, TypeString) }
func (m *Mapping) StatusMessage() Column { return m.typed(m.cfg.StatusMessage, TypeString) }
func (m *Mapping) Error() Column         { return m.typed(m.cfg.Error, TypeBool) }
func (m *Mapping) ServiceName() Column   { return m.typed(m.cfg.ServiceName, TypeString) }
func (m *Mapping) ScopeName() Column     { return m.typed(m.cfg.ScopeName, TypeString) }
func (m *Mapping) ScopeVersion() Column  { return m.typed(m.cfg.ScopeVersion, TypeString) }
func (m *Mapping) Events() Column        { return m.typed(m.cfg.Events, TypeArray) }
func (m *Mapping) Links() Column         { return m.typed(m.cfg.Links, TypeArray) }
func (m *Mapping) TraceState() Column    { return m.typed(m.cfg.TraceState, TypeString) }

// IsRoot is a predicate selecting root spans.
func (m *Mapping) IsRoot() string {
	return apl.Call("isempty", m.ParentSpanID().Expr)
}

func (m *Mapping) typed(name string, t Type) Column {
	c := m.Field(name)
	if c.Type == TypeUnknown {
		c.Type = t
	}
	return c
}

// SpanAttribute resolves a span-scoped attribute name.
func (m *Mapping) SpanAttribute(name string) Column {
	return m.scoped(name, m.cfg.SpanAttrPrefix, m.cfg.SpanCustomMap)
}

// ResourceAttribute resolves a resource-scoped attribute name.
func (m *Mapping) ResourceAttribute(name string) Column {
	if m.topLevel[name] {
		c := m.Field(name)
		if c.Type == TypeUnknown {
			c.Type = TypeString
		}
		return c
	}
	return m.scoped(name, m.cfg.ResourceAttrPrefix, m.cfg.ResourceCustomMap)
}

// UnscopedAttribute resolves an attribute that may live on the span or the
// resource. The span value wins when both exist, matching TraceQL.
func (m *Mapping) UnscopedAttribute(name string) Column {
	span := m.SpanAttribute(name)
	res := m.ResourceAttribute(name)
	switch {
	case span.Missing && res.Missing:
		return span
	case span.Missing:
		return res
	case res.Missing:
		return span
	}
	t := span.Type
	if t != res.Type {
		t = TypeUnknown
	}
	return Column{Expr: apl.Call("coalesce", span.Expr, res.Expr), Type: t}
}

func (m *Mapping) scoped(name, prefix, custom string) Column {
	flat := prefix + name
	mapped := Column{Expr: apl.MapKey(custom, name), Type: TypeUnknown, Field: custom, MapKey: true}
	if !m.Discovered() {
		return Column{Expr: apl.Call("coalesce", apl.Ident(flat), mapped.Expr), Type: TypeUnknown}
	}
	if t, ok := m.fields[flat]; ok && t != TypeMap {
		return Column{Expr: apl.Ident(flat), Type: t, Field: flat}
	}
	if _, ok := m.fields[custom]; ok {
		return mapped
	}
	// Neither exists; the attribute is simply absent. Reference the
	// flat form so the query stays valid and matches nothing.
	return Column{Expr: apl.Ident(flat), Type: TypeUnknown, Field: flat, Missing: true}
}

// Resolve maps a TraceQL attribute to a column. The bool result is false
// when the attribute cannot be expressed as a per-span column expression
// (trace-level intrinsics, events, links, nested set values).
func (m *Mapping) Resolve(a traceql.Attribute) (Column, bool) {
	if a.Parent {
		return Column{}, false
	}
	if a.Intrinsic != traceql.IntrinsicNone {
		return m.resolveIntrinsic(a.Intrinsic)
	}
	switch a.Scope {
	case traceql.AttributeScopeSpan:
		return m.SpanAttribute(a.Name), true
	case traceql.AttributeScopeResource:
		return m.ResourceAttribute(a.Name), true
	case traceql.AttributeScopeNone:
		return m.UnscopedAttribute(a.Name), true
	case traceql.AttributeScopeInstrumentation:
		switch a.Name {
		case "name":
			return m.ScopeName(), true
		case "version":
			return m.ScopeVersion(), true
		}
	}
	return Column{}, false
}

func (m *Mapping) resolveIntrinsic(i traceql.Intrinsic) (Column, bool) {
	switch i {
	case traceql.IntrinsicDuration, traceql.ScopedIntrinsicSpanDuration:
		return m.Duration(), true
	case traceql.IntrinsicName, traceql.ScopedIntrinsicSpanName:
		return m.Name(), true
	case traceql.IntrinsicStatus, traceql.ScopedIntrinsicSpanStatus:
		c := m.StatusCode()
		// The translator falls back to the error flag when the code column
		// is absent, so never report it as missing here.
		c.Missing = false
		return c, true
	case traceql.IntrinsicStatusMessage, traceql.ScopedIntrinsicSpanStatusMessage:
		return m.StatusMessage(), true
	case traceql.IntrinsicKind, traceql.ScopedIntrinsicSpanKind:
		return m.Kind(), true
	case traceql.IntrinsicTraceID:
		return m.TraceID(), true
	case traceql.IntrinsicSpanID:
		return m.SpanID(), true
	case traceql.IntrinsicParentID:
		return m.ParentSpanID(), true
	case traceql.IntrinsicInstrumentationName:
		return m.ScopeName(), true
	case traceql.IntrinsicInstrumentationVersion:
		return m.ScopeVersion(), true
	case traceql.IntrinsicSpanStartTime:
		return m.Time(), true
	}
	return Column{}, false
}

// TagNames returns the attribute names known for a scope from the
// discovered schema. Map-field keys are not included since they need
// sampling the data.
func (m *Mapping) TagNames(scope traceql.AttributeScope) []string {
	if m.fields == nil {
		return nil
	}
	var out []string
	switch scope {
	case traceql.AttributeScopeSpan:
		for f, t := range m.fields {
			if f == m.cfg.SpanCustomMap || t == TypeMap {
				continue
			}
			if strings.HasPrefix(f, m.cfg.SpanAttrPrefix) {
				out = append(out, strings.TrimPrefix(f, m.cfg.SpanAttrPrefix))
			}
		}
	case traceql.AttributeScopeResource:
		for f, t := range m.fields {
			if f == m.cfg.ResourceCustomMap || t == TypeMap {
				continue
			}
			if strings.HasPrefix(f, m.cfg.ResourceAttrPrefix) {
				out = append(out, strings.TrimPrefix(f, m.cfg.ResourceAttrPrefix))
			} else if m.topLevel[f] {
				out = append(out, f)
			}
		}
	}
	sort.Strings(out)
	return out
}

func typeFromAxiom(t string) Type {
	switch strings.ToLower(t) {
	case "string":
		return TypeString
	case "integer", "int", "int64", "long":
		return TypeInt
	case "float", "float64", "real", "double":
		return TypeFloat
	case "bool", "boolean":
		return TypeBool
	case "timespan", "duration":
		return TypeDuration
	case "datetime", "timestamp":
		return TypeDatetime
	case "array":
		return TypeArray
	case "map", "dynamic", "object":
		return TypeMap
	}
	return TypeUnknown
}
