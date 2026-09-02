package fetch

import (
	"strings"
	"testing"

	"github.com/grafana/tempo/pkg/traceql"

	"github.com/velddev/axiom-tempo-proxy/internal/axiom"
	"github.com/velddev/axiom-tempo-proxy/internal/schema"
)

func testFields(names ...string) []axiom.DatasetField {
	out := make([]axiom.DatasetField, 0, len(names)/2)
	for i := 0; i+1 < len(names); i += 2 {
		out = append(out, axiom.DatasetField{Name: names[i], Type: names[i+1]})
	}
	return out
}

// testMapping mirrors the shape of a real Axiom OTel dataset: some flat
// semconv columns, custom maps, events and links, and a few intrinsics
// the dataset happens not to have (scope.version, trace_state).
func testMapping() *schema.Mapping {
	return schema.New(schema.DefaultConfig(), testFields(
		"_time", "datetime", "trace_id", "string", "span_id", "string",
		"parent_span_id", "string", "name", "string", "kind", "string",
		"duration", "timespan", "status.code", "string", "status.message", "string",
		"error", "bool", "service.name", "string", "service.version", "string",
		"telemetry.sdk.language", "string", "scope.name", "string",
		"attributes.http.method", "string", "attributes.http.status_code", "integer",
		"attributes.custom", "map", "resource.custom", "map",
		"resource.k8s.pod.name", "string", "events", "array", "links", "array",
		"unrelated.log.column", "string",
	))
}

// intrinsics is what every projection carries: the intrinsic columns the
// dataset has, then its top-level resource fields.
var intrinsics = []string{
	"_time", "trace_id", "span_id", "parent_span_id", "name", "kind", "duration",
	"status.code", "status.message", "error", "service.name", "scope.name",
	"service.version", "telemetry.sdk.language",
}

func request(t *testing.T, query string) traceql.FetchSpansRequest {
	t.Helper()
	req, err := traceql.ExtractFetchSpansRequest(query)
	if err != nil {
		t.Fatalf("extract %q: %v", query, err)
	}
	return req
}

func TestProjectColumns(t *testing.T) {
	m := testMapping()
	cases := []struct {
		name  string
		query string
		extra []string
	}{
		{"intrinsic only", `{ status = error }`, nil},
		{"duration", `{ duration > 1s }`, nil},
		{"trace intrinsic", `{ rootServiceName = "frontend" }`, nil},
		{"flat span attribute", `{ span.http.method = "GET" }`, []string{"attributes.http.method"}},
		{"custom map attribute", `{ span.app.user.id = "u-1" }`, []string{"attributes.custom"}},
		{"flat resource attribute", `{ resource.k8s.pod.name = "p" }`, []string{"resource.k8s.pod.name"}},
		{"top level resource attribute", `{ resource.service.name = "frontend" }`, nil},
		{"custom resource attribute", `{ resource.deployment.environment = "prod" }`, []string{"resource.custom"}},
		{"unscoped attribute", `{ .http.method = "GET" }`, []string{"attributes.http.method", "resource.custom"}},
		{"unscoped custom attribute", `{ .app.user.id = "u-1" }`, []string{"attributes.custom", "resource.custom"}},
		{"event attribute", `{ event.exception.type = "DbError" }`, []string{"events"}},
		{"event name intrinsic", `{ event:name = "exception" }`, []string{"events"}},
		{"link attribute", `{ link.relation = "follows" }`, []string{"links"}},
		{"link intrinsic", `{ link:traceID = "abc" }`, []string{"links"}},
		{"select", `{ status = error } | select(span.http.method, event.exception.message)`,
			[]string{"attributes.http.method", "events"}},
		{"several", `{ span.http.method = "GET" && resource.k8s.pod.name = "p" }`,
			[]string{"attributes.http.method", "resource.k8s.pod.name"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := projectColumns(m, request(t, tc.query))
			want := append(append([]string{}, intrinsics...), tc.extra...)
			if strings.Join(got, ",") != strings.Join(want, ",") {
				t.Errorf("projectColumns(%q) =\n  %v\nwant\n  %v", tc.query, got, want)
			}
		})
	}
}

// Every projected column must exist: APL rejects a project stage naming
// an unknown field with a 400 and the whole search fails.
func TestProjectColumnsNeverNamesMissingColumns(t *testing.T) {
	m := testMapping()
	for _, q := range []string{
		`{ span.nope = "x" }`,
		`{ resource.nope = "x" }`,
		`{ .nope = "x" }`,
		`{ span:status = error }`,
		`{ span:kind = server }`,
		`{ event.nope = "x" }`,
	} {
		for _, c := range projectColumns(m, request(t, q)) {
			if !m.HasField(c) {
				t.Errorf("%s: projected unknown column %q", q, c)
			}
		}
	}

	// A dataset without custom maps must not fall back to the flat form of
	// an attribute it does not have either.
	bare := schema.New(schema.DefaultConfig(), testFields(
		"_time", "datetime", "trace_id", "string", "span_id", "string",
		"parent_span_id", "string", "name", "string", "service.name", "string",
	))
	got := projectColumns(bare, request(t, `{ span.app.user.id = "u-1" }`))
	want := []string{"_time", "trace_id", "span_id", "parent_span_id", "name", "service.name"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("bare schema projection = %v, want %v", got, want)
	}
}

// Structural operators need the parent id and trace metadata needs the
// service and span name, whatever the query asked for.
func TestProjectColumnsAlwaysCarriesStructureAndMetadata(t *testing.T) {
	m := testMapping()
	got := projectColumns(m, request(t, `{ span.http.method = "GET" } >> { status = error }`))
	set := map[string]bool{}
	for _, c := range got {
		set[c] = true
	}
	for _, need := range []string{"trace_id", "span_id", "parent_span_id", "name", "service.name", "_time", "duration"} {
		if !set[need] {
			t.Errorf("projection %v is missing %q", got, need)
		}
	}
}

func TestProjectColumnsSelectAllAndUndiscovered(t *testing.T) {
	m := testMapping()
	req := request(t, `{ status = error }`)
	req.SecondPassSelectAll = true
	if got := projectColumns(m, req); got != nil {
		t.Errorf("select-all projection = %v, want none", got)
	}

	guessed := schema.New(schema.DefaultConfig(), nil)
	if got := projectColumns(guessed, request(t, `{ status = error }`)); got != nil {
		t.Errorf("undiscovered projection = %v, want none", got)
	}
	if got := projectColumns(nil, request(t, `{ status = error }`)); got != nil {
		t.Errorf("nil mapping projection = %v, want none", got)
	}
}

// Attributes reached through a parent.* reference sit in the same dataset
// columns as the span's own.
func TestProjectColumnsParentScopedAttribute(t *testing.T) {
	m := testMapping()
	req := traceql.FetchSpansRequest{Conditions: []traceql.Condition{{
		Attribute: traceql.NewScopedAttribute(traceql.AttributeScopeSpan, true, "http.method"),
	}}}
	got := projectColumns(m, req)
	if got[len(got)-1] != "attributes.http.method" {
		t.Errorf("parent-scoped projection = %v", got)
	}
}

func TestProjectExprsQuotesDottedNames(t *testing.T) {
	got := projectExprs([]string{"_time", "trace_id", "status.code", "kind"})
	want := []string{"_time", "trace_id", "['status.code']", "['kind']"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Errorf("projectExprs = %v, want %v", got, want)
	}
}
