package spans

import (
	"bytes"
	"sort"
	"time"

	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/util"
)

// Trace is a group of spans sharing a trace id.
type Trace struct {
	Info  *TraceInfo
	Spans []*Span
}

// GroupTraces groups spans by trace id, assigns nested set bounds, and
// computes trace-level info. Spans within each trace are ordered by start
// time.
func GroupTraces(all []*Span) []*Trace {
	byID := map[string]*Trace{}
	var order []string
	for _, s := range all {
		key := string(s.TraceID)
		t, ok := byID[key]
		if !ok {
			t = &Trace{Info: &TraceInfo{TraceID: s.TraceID}}
			byID[key] = t
			order = append(order, key)
		}
		t.Spans = append(t.Spans, s)
	}
	out := make([]*Trace, 0, len(order))
	for _, key := range order {
		t := byID[key]
		t.finish()
		out = append(out, t)
	}
	return out
}

// finish sorts spans, assigns nested sets, and fills TraceInfo.
func (t *Trace) finish() {
	sort.SliceStable(t.Spans, func(i, j int) bool { return t.Spans[i].StartNs < t.Spans[j].StartNs })
	AssignNestedSets(t.Spans)

	info := t.Info
	info.ServiceStats = map[string]traceql.ServiceStats{}
	var root *Span
	var minStart, maxEnd uint64
	for i, s := range t.Spans {
		s.trace = info
		if i == 0 || s.StartNs < minStart {
			minStart = s.StartNs
		}
		if e := s.EndNs(); e > maxEnd {
			maxEnd = e
		}
		st := info.ServiceStats[s.ServiceName]
		st.SpanCount++
		if s.Status == traceql.StatusError {
			st.ErrorCount++
		}
		info.ServiceStats[s.ServiceName] = st
		if root == nil && s.IsRoot() {
			root = s
		}
	}
	if root == nil && len(t.Spans) > 0 {
		// No parentless span (partial trace): use the earliest span.
		root = t.Spans[0]
	}
	if root != nil {
		info.RootName = root.Name
		info.RootService = root.ServiceName
	}
	info.StartNs = minStart
	if maxEnd > minStart {
		info.DurationNs = maxEnd - minStart
	}
}

// Spanset wraps the trace as a traceql.Spanset for the engine.
func (t *Trace) Spanset() *traceql.Spanset {
	ss := &traceql.Spanset{
		TraceID:            t.Info.TraceID,
		RootSpanName:       t.Info.RootName,
		RootServiceName:    t.Info.RootService,
		StartTimeUnixNanos: t.Info.StartNs,
		DurationNanos:      t.Info.DurationNs,
		ServiceStats:       t.Info.ServiceStats,
		Spans:              make([]traceql.Span, len(t.Spans)),
	}
	for i, s := range t.Spans {
		ss.Spans[i] = s
	}
	return ss
}

// AssignNestedSets computes nested set left/right bounds, parent links
// and child counts for spans of one trace, the way Tempo does: roots get
// parent -1, every other span's parent is its parent's left bound, and
// spans whose parent is missing (a disconnected trace) keep zero bounds
// so structural operators never match them.
func AssignNestedSets(spans []*Span) {
	if len(spans) == 0 {
		return
	}
	byID := make(map[string][]*Span, len(spans))
	for _, s := range spans {
		byID[string(s.SpanID)] = append(byID[string(s.SpanID)], s)
	}
	children := make(map[*Span][]*Span, len(spans))
	var roots []*Span
	for _, s := range spans {
		s.nestedSetLeft, s.nestedSetRight, s.nestedSetParent, s.childCount = 0, 0, 0, 0
		if s.IsRoot() {
			roots = append(roots, s)
			continue
		}
		parents := byID[string(s.ParentSpanID)]
		var parent *Span
		for _, p := range parents {
			if p != s {
				parent = p
				break
			}
		}
		if parent == nil {
			// Orphan: Tempo leaves it unassigned.
			continue
		}
		children[parent] = append(children[parent], s)
	}

	counter := int32(0)
	// Iterative DFS to avoid deep recursion on long chains.
	type frame struct {
		span *Span
		next int
	}
	visited := make(map[*Span]bool, len(spans))
	for _, root := range roots {
		if visited[root] {
			continue
		}
		counter++
		root.nestedSetLeft = counter
		root.nestedSetParent = -1
		visited[root] = true
		stack := []frame{{span: root}}
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			kids := children[top.span]
			if top.next < len(kids) {
				child := kids[top.next]
				top.next++
				if visited[child] {
					continue
				}
				visited[child] = true
				counter++
				child.nestedSetLeft = counter
				child.nestedSetParent = top.span.nestedSetLeft
				top.span.childCount++
				stack = append(stack, frame{span: child})
				continue
			}
			counter++
			top.span.nestedSetRight = counter
			stack = stack[:len(stack)-1]
		}
	}
}

// HexTraceID returns the trace id as Tempo renders it.
func (t *Trace) HexTraceID() string {
	return util.TraceIDToHexString(t.Info.TraceID)
}

// SameTrace reports whether two ids are equal.
func SameTrace(a, b []byte) bool { return bytes.Equal(a, b) }

func nsToDuration(ns uint64) time.Duration {
	if ns > uint64(1<<63-1) {
		return time.Duration(1<<63 - 1)
	}
	return time.Duration(ns)
}
