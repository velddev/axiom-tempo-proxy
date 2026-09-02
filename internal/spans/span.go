// Package spans holds the in-memory span model used between the Axiom
// result rows and the Tempo engine / OTLP encoders.
package spans

import (
	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/util"
)

// Attr is one attribute.
type Attr struct {
	Key string
	Val traceql.Static
}

// Event is a span event.
type Event struct {
	Name   string
	TimeNs uint64
	Attrs  []Attr
}

// Link is a span link.
type Link struct {
	TraceID    []byte
	SpanID     []byte
	TraceState string
	Attrs      []Attr
}

// TraceInfo is trace-level data shared by all spans of a trace.
type TraceInfo struct {
	TraceID      []byte
	RootName     string
	RootService  string
	StartNs      uint64
	DurationNs   uint64
	ServiceStats map[string]traceql.ServiceStats
}

// Span is one span with its resource and scope context.
type Span struct {
	TraceID      []byte
	SpanID       []byte
	ParentSpanID []byte

	Name          string
	Kind          traceql.Kind
	StartNs       uint64
	DurationNs    uint64
	Status        traceql.Status
	StatusMessage string
	TraceState    string

	ServiceName  string
	ScopeName    string
	ScopeVersion string

	ResourceAttrs []Attr
	SpanAttrs     []Attr
	Events        []Event
	Links         []Link

	// Structural data, filled by AssignNestedSets.
	nestedSetLeft   int32
	nestedSetRight  int32
	nestedSetParent int32
	childCount      int32

	trace   *TraceInfo
	exposed *AttrSet
}

// AttrSet is the set of attributes a fetch request asked for. When set on
// a span, AllAttributes only reports those, matching how Tempo's storage
// only materialises requested columns, so search results carry the
// attributes the query mentioned rather than every column.
type AttrSet struct {
	exact      map[traceql.Attribute]struct{}
	unscoped   map[string]struct{}
	intrinsics map[traceql.Intrinsic]struct{}
}

// NewAttrSet builds an AttrSet from fetch conditions.
func NewAttrSet(conds ...[]traceql.Condition) *AttrSet {
	s := &AttrSet{
		exact:      map[traceql.Attribute]struct{}{},
		unscoped:   map[string]struct{}{},
		intrinsics: map[traceql.Intrinsic]struct{}{},
	}
	for _, list := range conds {
		for _, c := range list {
			a := c.Attribute
			if a.Intrinsic != traceql.IntrinsicNone {
				s.intrinsics[canonicalIntrinsic(a.Intrinsic)] = struct{}{}
				continue
			}
			if a.Scope == traceql.AttributeScopeNone {
				s.unscoped[a.Name] = struct{}{}
				continue
			}
			s.exact[a] = struct{}{}
		}
	}
	return s
}

// Has reports whether an attribute was requested.
func (s *AttrSet) Has(a traceql.Attribute) bool {
	if s == nil {
		return true
	}
	if a.Intrinsic != traceql.IntrinsicNone {
		_, ok := s.intrinsics[canonicalIntrinsic(a.Intrinsic)]
		return ok
	}
	if _, ok := s.exact[a]; ok {
		return true
	}
	if a.Scope == traceql.AttributeScopeSpan || a.Scope == traceql.AttributeScopeResource {
		_, ok := s.unscoped[a.Name]
		return ok
	}
	return false
}

// canonicalIntrinsic folds scoped intrinsics onto their unscoped twins.
func canonicalIntrinsic(i traceql.Intrinsic) traceql.Intrinsic {
	switch i {
	case traceql.ScopedIntrinsicSpanDuration:
		return traceql.IntrinsicDuration
	case traceql.ScopedIntrinsicSpanName:
		return traceql.IntrinsicName
	case traceql.ScopedIntrinsicSpanStatus:
		return traceql.IntrinsicStatus
	case traceql.ScopedIntrinsicSpanStatusMessage:
		return traceql.IntrinsicStatusMessage
	case traceql.ScopedIntrinsicSpanKind:
		return traceql.IntrinsicKind
	case traceql.ScopedIntrinsicTraceRootName:
		return traceql.IntrinsicTraceRootSpan
	case traceql.ScopedIntrinsicTraceRootService:
		return traceql.IntrinsicTraceRootService
	case traceql.ScopedIntrinsicTraceDuration:
		return traceql.IntrinsicTraceDuration
	}
	return i
}

// SetExposed restricts AllAttributes to the given set (nil = all).
func (s *Span) SetExposed(set *AttrSet) { s.exposed = set }

var _ traceql.Span = (*Span)(nil)

// EndNs returns the span end time.
func (s *Span) EndNs() uint64 { return s.StartNs + s.DurationNs }

// IsRoot reports whether the span has no parent.
func (s *Span) IsRoot() bool { return len(s.ParentSpanID) == 0 }

// Trace returns the trace-level info, or nil before grouping.
func (s *Span) Trace() *TraceInfo { return s.trace }

// NestedSet returns the nested set bounds and parent.
func (s *Span) NestedSet() (left, right, parent int32) {
	return s.nestedSetLeft, s.nestedSetRight, s.nestedSetParent
}

// ID implements traceql.Span.
func (s *Span) ID() []byte { return s.SpanID }

// StartTimeUnixNanos implements traceql.Span.
func (s *Span) StartTimeUnixNanos() uint64 { return s.StartNs }

// DurationNanos implements traceql.Span.
func (s *Span) DurationNanos() uint64 { return s.DurationNs }

// AttributeFor implements traceql.Span.
func (s *Span) AttributeFor(a traceql.Attribute) (traceql.Static, bool) {
	if a.Parent {
		return traceql.NewStaticNil(), false
	}
	if a.Intrinsic != traceql.IntrinsicNone {
		return s.intrinsic(a.Intrinsic)
	}
	switch a.Scope {
	case traceql.AttributeScopeSpan:
		return lookup(s.SpanAttrs, a.Name)
	case traceql.AttributeScopeResource:
		return lookup(s.ResourceAttrs, a.Name)
	case traceql.AttributeScopeNone:
		if v, ok := lookup(s.SpanAttrs, a.Name); ok {
			return v, true
		}
		return lookup(s.ResourceAttrs, a.Name)
	case traceql.AttributeScopeEvent:
		return s.eventAttr(a.Name)
	case traceql.AttributeScopeLink:
		return s.linkAttr(a.Name)
	case traceql.AttributeScopeInstrumentation:
		switch a.Name {
		case "name":
			return traceql.NewStaticString(s.ScopeName), s.ScopeName != ""
		case "version":
			return traceql.NewStaticString(s.ScopeVersion), s.ScopeVersion != ""
		}
	}
	return traceql.NewStaticNil(), false
}

func (s *Span) intrinsic(i traceql.Intrinsic) (traceql.Static, bool) {
	switch i {
	case traceql.IntrinsicDuration, traceql.ScopedIntrinsicSpanDuration:
		return traceql.NewStaticDuration(nsToDuration(s.DurationNs)), true
	case traceql.IntrinsicName, traceql.ScopedIntrinsicSpanName:
		return traceql.NewStaticString(s.Name), true
	case traceql.IntrinsicStatus, traceql.ScopedIntrinsicSpanStatus:
		return traceql.NewStaticStatus(s.Status), true
	case traceql.IntrinsicStatusMessage, traceql.ScopedIntrinsicSpanStatusMessage:
		return traceql.NewStaticString(s.StatusMessage), true
	case traceql.IntrinsicKind, traceql.ScopedIntrinsicSpanKind:
		return traceql.NewStaticKind(s.Kind), true
	case traceql.IntrinsicChildCount:
		return traceql.NewStaticInt(int(s.childCount)), true
	case traceql.IntrinsicNestedSetLeft:
		return traceql.NewStaticInt(int(s.nestedSetLeft)), true
	case traceql.IntrinsicNestedSetRight:
		return traceql.NewStaticInt(int(s.nestedSetRight)), true
	case traceql.IntrinsicNestedSetParent:
		return traceql.NewStaticInt(int(s.nestedSetParent)), true
	case traceql.IntrinsicTraceID:
		return traceql.NewStaticString(util.TraceIDToHexString(s.TraceID)), true
	case traceql.IntrinsicSpanID:
		return traceql.NewStaticString(util.SpanIDToHexString(s.SpanID)), true
	case traceql.IntrinsicParentID:
		if len(s.ParentSpanID) == 0 {
			return traceql.NewStaticNil(), false
		}
		return traceql.NewStaticString(util.SpanIDToHexString(s.ParentSpanID)), true
	case traceql.IntrinsicSpanStartTime:
		return traceql.NewStaticInt(int(s.StartNs)), true
	case traceql.IntrinsicInstrumentationName:
		return traceql.NewStaticString(s.ScopeName), s.ScopeName != ""
	case traceql.IntrinsicInstrumentationVersion:
		return traceql.NewStaticString(s.ScopeVersion), s.ScopeVersion != ""
	case traceql.IntrinsicEventName:
		return s.eventNames()
	case traceql.IntrinsicEventTimeSinceStart:
		return s.eventTimesSinceStart()
	case traceql.IntrinsicLinkTraceID:
		return s.linkIDs(true)
	case traceql.IntrinsicLinkSpanID:
		return s.linkIDs(false)
	}
	if s.trace != nil {
		switch i {
		case traceql.IntrinsicTraceRootService, traceql.ScopedIntrinsicTraceRootService:
			return traceql.NewStaticString(s.trace.RootService), true
		case traceql.IntrinsicTraceRootSpan, traceql.ScopedIntrinsicTraceRootName:
			return traceql.NewStaticString(s.trace.RootName), true
		case traceql.IntrinsicTraceDuration, traceql.ScopedIntrinsicTraceDuration:
			return traceql.NewStaticDuration(nsToDuration(s.trace.DurationNs)), true
		case traceql.IntrinsicTraceStartTime:
			return traceql.NewStaticInt(int(s.trace.StartNs)), true
		}
	}
	return traceql.NewStaticNil(), false
}

// AllAttributes implements traceql.Span.
func (s *Span) AllAttributes() map[traceql.Attribute]traceql.Static {
	out := make(map[traceql.Attribute]traceql.Static, len(s.SpanAttrs)+len(s.ResourceAttrs)+8)
	s.AllAttributesFunc(func(a traceql.Attribute, v traceql.Static) { out[a] = v })
	return out
}

// AllAttributesFunc implements traceql.Span. When an exposed set is
// configured only requested attributes are reported; the span name is
// always included because Tempo reads it from here.
func (s *Span) AllAttributesFunc(fn func(traceql.Attribute, traceql.Static)) {
	emit := func(a traceql.Attribute, v traceql.Static) {
		if s.exposed == nil || s.exposed.Has(a) {
			fn(a, v)
		}
	}
	for _, a := range s.ResourceAttrs {
		emit(traceql.NewScopedAttribute(traceql.AttributeScopeResource, false, a.Key), a.Val)
	}
	for _, a := range s.SpanAttrs {
		emit(traceql.NewScopedAttribute(traceql.AttributeScopeSpan, false, a.Key), a.Val)
	}
	emit(traceql.IntrinsicDurationAttribute, traceql.NewStaticDuration(nsToDuration(s.DurationNs)))
	fn(traceql.IntrinsicNameAttribute, traceql.NewStaticString(s.Name))
	emit(traceql.IntrinsicStatusAttribute, traceql.NewStaticStatus(s.Status))
	emit(traceql.IntrinsicKindAttribute, traceql.NewStaticKind(s.Kind))
	if s.StatusMessage != "" {
		emit(traceql.IntrinsicStatusMessageAttribute, traceql.NewStaticString(s.StatusMessage))
	}
	if s.ScopeName != "" {
		emit(traceql.NewIntrinsic(traceql.IntrinsicInstrumentationName), traceql.NewStaticString(s.ScopeName))
	}
	if s.ScopeVersion != "" {
		emit(traceql.NewIntrinsic(traceql.IntrinsicInstrumentationVersion), traceql.NewStaticString(s.ScopeVersion))
	}
	emit(traceql.NewIntrinsic(traceql.IntrinsicNestedSetParent), traceql.NewStaticInt(int(s.nestedSetParent)))
	emit(traceql.NewIntrinsic(traceql.IntrinsicNestedSetLeft), traceql.NewStaticInt(int(s.nestedSetLeft)))
	emit(traceql.NewIntrinsic(traceql.IntrinsicNestedSetRight), traceql.NewStaticInt(int(s.nestedSetRight)))
	emit(traceql.NewIntrinsic(traceql.IntrinsicChildCount), traceql.NewStaticInt(int(s.childCount)))
	if s.trace != nil {
		emit(traceql.NewIntrinsic(traceql.IntrinsicTraceRootService), traceql.NewStaticString(s.trace.RootService))
		emit(traceql.NewIntrinsic(traceql.IntrinsicTraceRootSpan), traceql.NewStaticString(s.trace.RootName))
		emit(traceql.NewIntrinsic(traceql.IntrinsicTraceDuration), traceql.NewStaticDuration(nsToDuration(s.trace.DurationNs)))
	}
	for _, e := range s.Events {
		for _, a := range e.Attrs {
			emit(traceql.NewScopedAttribute(traceql.AttributeScopeEvent, false, a.Key), a.Val)
		}
	}
	for _, l := range s.Links {
		for _, a := range l.Attrs {
			emit(traceql.NewScopedAttribute(traceql.AttributeScopeLink, false, a.Key), a.Val)
		}
	}
}

func lookup(attrs []Attr, key string) (traceql.Static, bool) {
	for i := range attrs {
		if attrs[i].Key == key {
			return attrs[i].Val, true
		}
	}
	return traceql.NewStaticNil(), false
}

// eventAttr collects an attribute across all events. Multiple events
// yield an array so any-element matching works as in Tempo.
func (s *Span) eventAttr(key string) (traceql.Static, bool) {
	var vals []traceql.Static
	for _, e := range s.Events {
		if v, ok := lookup(e.Attrs, key); ok {
			vals = append(vals, v)
		}
	}
	return combine(vals)
}

func (s *Span) linkAttr(key string) (traceql.Static, bool) {
	var vals []traceql.Static
	for _, l := range s.Links {
		if v, ok := lookup(l.Attrs, key); ok {
			vals = append(vals, v)
		}
	}
	return combine(vals)
}

func (s *Span) eventNames() (traceql.Static, bool) {
	vals := make([]traceql.Static, 0, len(s.Events))
	for _, e := range s.Events {
		vals = append(vals, traceql.NewStaticString(e.Name))
	}
	return combine(vals)
}

func (s *Span) eventTimesSinceStart() (traceql.Static, bool) {
	vals := make([]traceql.Static, 0, len(s.Events))
	for _, e := range s.Events {
		var d uint64
		if e.TimeNs > s.StartNs {
			d = e.TimeNs - s.StartNs
		}
		vals = append(vals, traceql.NewStaticDuration(nsToDuration(d)))
	}
	return combine(vals)
}

func (s *Span) linkIDs(trace bool) (traceql.Static, bool) {
	vals := make([]traceql.Static, 0, len(s.Links))
	for _, l := range s.Links {
		if trace {
			vals = append(vals, traceql.NewStaticString(util.TraceIDToHexString(l.TraceID)))
		} else {
			vals = append(vals, traceql.NewStaticString(util.SpanIDToHexString(l.SpanID)))
		}
	}
	return combine(vals)
}

// combine turns several values into one static: a scalar for one value,
// a typed array when homogeneous, a string array otherwise.
func combine(vals []traceql.Static) (traceql.Static, bool) {
	switch len(vals) {
	case 0:
		return traceql.NewStaticNil(), false
	case 1:
		return vals[0], true
	}
	t := vals[0].Type
	homogeneous := true
	for _, v := range vals[1:] {
		if v.Type != t {
			homogeneous = false
			break
		}
	}
	if homogeneous {
		switch t {
		case traceql.TypeString:
			out := make([]string, len(vals))
			for i, v := range vals {
				out[i] = v.EncodeToString(false)
			}
			return traceql.NewStaticStringArray(out), true
		case traceql.TypeInt:
			out := make([]int, len(vals))
			for i, v := range vals {
				out[i], _ = v.Int()
			}
			return traceql.NewStaticIntArray(out), true
		case traceql.TypeFloat:
			out := make([]float64, len(vals))
			for i, v := range vals {
				out[i] = v.Float()
			}
			return traceql.NewStaticFloatArray(out), true
		case traceql.TypeBoolean:
			out := make([]bool, len(vals))
			for i, v := range vals {
				out[i], _ = v.Bool()
			}
			return traceql.NewStaticBooleanArray(out), true
		}
	}
	out := make([]string, len(vals))
	for i, v := range vals {
		out[i] = v.EncodeToString(false)
	}
	return traceql.NewStaticStringArray(out), true
}

// --- structural operators ---

func asSpan(s traceql.Span) *Span {
	sp, _ := s.(*Span)
	return sp
}

// isDescendantOf reports whether a is a descendant of b.
func isDescendantOf(a, b *Span) bool {
	if a == nil || b == nil || a.nestedSetLeft == 0 || b.nestedSetLeft == 0 || a.nestedSetRight == 0 || b.nestedSetRight == 0 {
		return false
	}
	return a.nestedSetLeft > b.nestedSetLeft && a.nestedSetRight < b.nestedSetRight
}

// isChildOf reports whether a is a direct child of b.
func isChildOf(a, b *Span) bool {
	if a == nil || b == nil || a.nestedSetParent <= 0 || b.nestedSetLeft == 0 {
		return false
	}
	return a.nestedSetParent == b.nestedSetLeft
}

// isSiblingOf reports whether a and b share a parent.
func isSiblingOf(a, b *Span) bool {
	if a == nil || b == nil || a == b || a.nestedSetParent == 0 || b.nestedSetParent == 0 {
		return false
	}
	return a.nestedSetParent == b.nestedSetParent
}

// relate implements the shared logic of the structural operators. rel(l,
// r) reports whether the rhs span r stands in the relation to the lhs
// span l (after any inversion has been applied by the caller).
func relate(lhs, rhs []traceql.Span, rel func(l, r *Span) bool, falseForAll, union bool, buffer []traceql.Span) []traceql.Span {
	if union {
		seen := make(map[*Span]struct{}, len(lhs)+len(rhs))
		add := func(s *Span) {
			if _, ok := seen[s]; !ok {
				seen[s] = struct{}{}
				buffer = append(buffer, s)
			}
		}
		for _, l := range lhs {
			ls := asSpan(l)
			for _, r := range rhs {
				rs := asSpan(r)
				if rel(ls, rs) {
					add(ls)
					add(rs)
				}
			}
		}
		return buffer
	}
	for _, r := range rhs {
		rs := asSpan(r)
		matched := false
		for _, l := range lhs {
			if rel(asSpan(l), rs) {
				matched = true
				break
			}
		}
		if matched != falseForAll {
			buffer = append(buffer, r)
		}
	}
	return buffer
}

// DescendantOf implements traceql.Span.
func (s *Span) DescendantOf(lhs, rhs []traceql.Span, falseForAll, invert, union bool, buffer []traceql.Span) []traceql.Span {
	if len(lhs) == 0 && len(rhs) == 0 {
		return nil
	}
	rel := func(l, r *Span) bool { return isDescendantOf(r, l) }
	if invert {
		rel = func(l, r *Span) bool { return isDescendantOf(l, r) }
	}
	return relate(lhs, rhs, rel, falseForAll, union, buffer)
}

// ChildOf implements traceql.Span.
func (s *Span) ChildOf(lhs, rhs []traceql.Span, falseForAll, invert, union bool, buffer []traceql.Span) []traceql.Span {
	if len(lhs) == 0 && len(rhs) == 0 {
		return nil
	}
	rel := func(l, r *Span) bool { return isChildOf(r, l) }
	if invert {
		rel = func(l, r *Span) bool { return isChildOf(l, r) }
	}
	return relate(lhs, rhs, rel, falseForAll, union, buffer)
}

// SiblingOf implements traceql.Span.
func (s *Span) SiblingOf(lhs, rhs []traceql.Span, falseForAll, union bool, buffer []traceql.Span) []traceql.Span {
	if len(lhs) == 0 && len(rhs) == 0 {
		return nil
	}
	return relate(lhs, rhs, isSiblingOf, falseForAll, union, buffer)
}
