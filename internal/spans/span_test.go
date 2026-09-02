package spans

import (
	"testing"

	"github.com/grafana/tempo/pkg/traceql"
)

// buildTrace makes:
//
//	a (root)
//	├── b
//	│   └── d
//	└── c
//	orphan (parent missing)
func buildTrace() map[string]*Span {
	mk := func(id, parent string, start uint64) *Span {
		s := &Span{TraceID: []byte("t"), SpanID: []byte(id), Name: id, StartNs: start, DurationNs: 10, ServiceName: "svc-" + id}
		if parent != "" {
			s.ParentSpanID = []byte(parent)
		}
		return s
	}
	m := map[string]*Span{
		"a":      mk("a", "", 1),
		"b":      mk("b", "a", 2),
		"c":      mk("c", "a", 3),
		"d":      mk("d", "b", 4),
		"orphan": mk("orphan", "zzz", 5),
	}
	return m
}

func names(spans []traceql.Span) []string {
	out := make([]string, len(spans))
	for i, s := range spans {
		out[i] = s.(*Span).Name
	}
	return out
}

func eq(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := map[string]int{}
	for _, x := range a {
		set[x]++
	}
	for _, x := range b {
		set[x]--
	}
	for _, v := range set {
		if v != 0 {
			return false
		}
	}
	return true
}

func TestNestedSets(t *testing.T) {
	m := buildTrace()
	all := []*Span{m["a"], m["b"], m["c"], m["d"], m["orphan"]}
	traces := GroupTraces(all)
	if len(traces) != 1 {
		t.Fatalf("traces = %d", len(traces))
	}
	tr := traces[0]
	if tr.Info.RootName != "a" || tr.Info.RootService != "svc-a" {
		t.Errorf("root = %s/%s", tr.Info.RootName, tr.Info.RootService)
	}
	if tr.Info.StartNs != 1 || tr.Info.DurationNs != 14 {
		t.Errorf("start/duration = %d/%d", tr.Info.StartNs, tr.Info.DurationNs)
	}
	l, r, p := m["a"].NestedSet()
	if l != 1 || r != 8 || p != -1 {
		t.Errorf("a = %d %d %d", l, r, p)
	}
	l, _, p = m["b"].NestedSet()
	if p != 1 {
		t.Errorf("b parent = %d", p)
	}
	_, _, p = m["d"].NestedSet()
	if p != l {
		t.Errorf("d parent = %d want %d", p, l)
	}
	l, r, p = m["orphan"].NestedSet()
	if l != 0 || r != 0 || p != 0 {
		t.Errorf("orphan = %d %d %d, want unassigned", l, r, p)
	}
	if v, _ := m["a"].AttributeFor(traceql.NewIntrinsic(traceql.IntrinsicChildCount)); v.String() != "2" {
		t.Errorf("a childCount = %s", v.String())
	}
	if v, _ := m["d"].AttributeFor(traceql.NewIntrinsic(traceql.IntrinsicTraceRootService)); v.EncodeToString(false) != "svc-a" {
		t.Errorf("d rootService = %s", v.String())
	}
}

func TestStructural(t *testing.T) {
	m := buildTrace()
	all := []*Span{m["a"], m["b"], m["c"], m["d"], m["orphan"]}
	GroupTraces(all)
	sp := func(ids ...string) []traceql.Span {
		out := make([]traceql.Span, len(ids))
		for i, id := range ids {
			out[i] = m[id]
		}
		return out
	}
	var s Span

	// {a} >> {b,c,d,orphan}: descendants of a.
	got := names(s.DescendantOf(sp("a"), sp("b", "c", "d", "orphan"), false, false, false, nil))
	if !eq(got, []string{"b", "c", "d"}) {
		t.Errorf(">> = %v", got)
	}
	// {d} << {a,b,c}: ancestors of d.
	got = names(s.DescendantOf(sp("d"), sp("a", "b", "c"), false, true, false, nil))
	if !eq(got, []string{"a", "b"}) {
		t.Errorf("<< = %v", got)
	}
	// {a} !>> {b,d,orphan}: not descendants of a.
	got = names(s.DescendantOf(sp("a"), sp("b", "d", "orphan"), true, false, false, nil))
	if !eq(got, []string{"orphan"}) {
		t.Errorf("!>> = %v", got)
	}
	// {a} &>> {b,d}: union of both sides.
	got = names(s.DescendantOf(sp("a"), sp("b", "d", "orphan"), false, false, true, nil))
	if !eq(got, []string{"a", "b", "d"}) {
		t.Errorf("&>> = %v", got)
	}
	// {a} > {b,c,d}: direct children.
	got = names(s.ChildOf(sp("a"), sp("b", "c", "d"), false, false, false, nil))
	if !eq(got, []string{"b", "c"}) {
		t.Errorf("> = %v", got)
	}
	// {d} < {a,b}: parent of d.
	got = names(s.ChildOf(sp("d"), sp("a", "b"), false, true, false, nil))
	if !eq(got, []string{"b"}) {
		t.Errorf("< = %v", got)
	}
	// {b} ~ {a,c,d}: siblings of b.
	got = names(s.SiblingOf(sp("b"), sp("a", "c", "d", "b"), false, false, nil))
	if !eq(got, []string{"c"}) {
		t.Errorf("~ = %v", got)
	}
}
