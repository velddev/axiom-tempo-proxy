package translate

import (
	"testing"

	"github.com/velddev/axiom-tempo-proxy/internal/axiom"
	"github.com/velddev/axiom-tempo-proxy/internal/schema"
)

func testFields() []axiom.DatasetField {
	mk := func(name, typ string) axiom.DatasetField { return axiom.DatasetField{Name: name, Type: typ} }
	return []axiom.DatasetField{
		mk("_time", "datetime"), mk("trace_id", "string"), mk("span_id", "string"),
		mk("parent_span_id", "string"), mk("name", "string"), mk("kind", "string"),
		mk("duration", "timespan"), mk("status.code", "string"), mk("status.message", "string"),
		mk("error", "bool"), mk("service.name", "string"), mk("scope.name", "string"),
		mk("attributes.http.method", "string"), mk("attributes.http.status_code", "integer"),
		mk("attributes.custom", "map"), mk("resource.custom", "map"),
		mk("resource.k8s.pod.name", "string"),
	}
}

func newTestTranslator(t *testing.T, discovered bool) *Translator {
	t.Helper()
	var fields []axiom.DatasetField
	if discovered {
		fields = testFields()
	}
	return New(schema.New(schema.DefaultConfig(), fields))
}

func TestFilterDiscovered(t *testing.T) {
	tr := newTestTranslator(t, true)
	cases := []struct {
		q     string
		want  string
		exact bool
	}{
		{`{}`, ``, true},
		{`{ true }`, ``, true},
		{`{nestedSetParent<0 && true}`, `isempty(parent_span_id)`, true},
		{`{ nestedSetParent >= 0 }`, `not(isempty(parent_span_id))`, true},
		{`{ resource.service.name = "frontend" }`, `['service.name'] == "frontend"`, true},
		{`{ span.http.status_code >= 500 }`, `['attributes.http.status_code'] >= 500`, true},
		{`{ span.http.method = "GET" }`, `['attributes.http.method'] == "GET"`, true},
		{`{ span.foo = "x" }`, `tostring(['attributes.custom']['foo']) == "x"`, true},
		{`{ span.retries > 3 }`, `tolong(['attributes.custom']['retries']) > 3`, true},
		{`{ resource.k8s.pod.name = "p" }`, `['resource.k8s.pod.name'] == "p"`, true},
		{`{ resource.env = "prod" }`, `tostring(['resource.custom']['env']) == "prod"`, true},
		{`{ .service.name = "x" }`, `tostring(coalesce(['attributes.custom']['service.name'], ['service.name'])) == "x"`, true},
		{`{ .http.method = "x" }`, `tostring(coalesce(['attributes.http.method'], ['resource.custom']['http.method'])) == "x"`, true},
		{`{ .foo = "x" }`, `tostring(coalesce(['attributes.custom']['foo'], ['resource.custom']['foo'])) == "x"`, true},
		{`{ status = error }`, `(['status.code'] =~ "error") or (['status.code'] =~ "STATUS_CODE_ERROR") or (error == true)`, true},
		{`{ status != error }`, `not((['status.code'] =~ "error") or (['status.code'] =~ "STATUS_CODE_ERROR") or (error == true))`, true},
		{`{ status = ok }`, `(['status.code'] =~ "ok") or (['status.code'] =~ "STATUS_CODE_OK")`, true},
		{`{ kind = server }`, `(['kind'] =~ "server") or (['kind'] =~ "SPAN_KIND_SERVER")`, true},
		{`{ duration > 100ms }`, `duration > 100ms`, true},
		{`{ span:duration >= 1.5s }`, `duration >= 1500ms`, true},
		{`{ name =~ "GET.*" }`, `name matches regex @"^(?:GET.*)$"`, true},
		{`{ name !~ "GET.*" }`, `not(name matches regex @"^(?:GET.*)$")`, true},
		{`{ span.db.system.name != "" }`, `tostring(['attributes.custom']['db.system.name']) != ""`, true},
		{`{ resource.service.name != nil }`, `isnotnull(['service.name'])`, true},
		{`{ span.foo = nil }`, `isnull(['attributes.custom']['foo'])`, true},
		{`{nestedSetParent<0 && true && status = error && span.http.method = "GET"}`,
			`(isempty(parent_span_id)) and ((['status.code'] =~ "error") or (['status.code'] =~ "STATUS_CODE_ERROR") or (error == true)) and (['attributes.http.method'] == "GET")`, true},
		{`{ span.http.status_code = 200 || span.foo = "y" }`,
			`(['attributes.http.status_code'] == 200) or (tostring(['attributes.custom']['foo']) == "y")`, true},
		{`{ instrumentation:name = "lib" }`, `['scope.name'] == "lib"`, true},
		{`{ trace:id = "abc" }`, `trace_id == "abc"`, true},
		{`{ span.flag = true }`, `tobool(['attributes.custom']['flag']) == true`, true},
		{`{ span.ratio < 0.5 }`, `toreal(['attributes.custom']['ratio']) < 0.5`, true},
		// Relaxed cases.
		{`{ event.exception.message = "boom" }`, ``, false},
		{`{ traceDuration > 1s }`, ``, false},
		{`{ rootServiceName = "x" }`, ``, false},
		{`{ span.a = 1 && (traceDuration > 1s || span.b = 2) }`, `tolong(['attributes.custom']['a']) == 1`, false},
		{`{ span.a = 1 && event.name = "x" }`, `tolong(['attributes.custom']['a']) == 1`, false},
		{`{ parent.span.a = 1 }`, ``, false},
		{`{ span:childCount > 2 }`, ``, false},
		{`{ span.a = 1 && span.b = 2 && span.c = 3 }`, `(tolong(['attributes.custom']['a']) == 1) and (tolong(['attributes.custom']['b']) == 2) and (tolong(['attributes.custom']['c']) == 3)`, true},
		{`{ !(span.a = 1) }`, `not(tolong(['attributes.custom']['a']) == 1)`, true},
	}
	for _, c := range cases {
		expr, err := ParseFilter(c.q)
		if err != nil {
			t.Errorf("%s: parse: %v", c.q, err)
			continue
		}
		f := tr.Filter(expr)
		if f.Where != c.want || f.Exact != c.exact {
			t.Errorf("%s:\n  got  %q exact=%v (unsupported=%v)\n  want %q exact=%v", c.q, f.Where, f.Exact, f.Unsupported, c.want, c.exact)
		}
	}
}

func TestFilterUndiscovered(t *testing.T) {
	tr := newTestTranslator(t, false)
	cases := map[string]string{
		`{ span.http.method = "GET" }`:     `tostring(coalesce(['attributes.http.method'], ['attributes.custom']['http.method'])) == "GET"`,
		`{ resource.service.name = "x" }`:  `['service.name'] == "x"`,
		`{ resource.env = "prod" }`:        `tostring(coalesce(['resource.env'], ['resource.custom']['env'])) == "prod"`,
		`{ duration > 1s }`:                `duration > 1s`,
		`{ span.http.status_code >= 500 }`: `tolong(coalesce(['attributes.http.status_code'], ['attributes.custom']['http.status_code'])) >= 500`,
	}
	for q, want := range cases {
		expr, err := ParseFilter(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		if got := tr.Filter(expr).Where; got != want {
			t.Errorf("%s:\n  got  %q\n  want %q", q, got, want)
		}
	}
}

// A dataset without custom maps: unknown attributes cannot exist, and
// referencing them in APL would be a hard error, so they become false.
func TestFilterMissingAttributes(t *testing.T) {
	var fields []axiom.DatasetField
	for _, f := range testFields() {
		if f.Name == "attributes.custom" || f.Name == "resource.custom" || f.Name == "error" {
			continue
		}
		fields = append(fields, f)
	}
	tr := New(schema.New(schema.DefaultConfig(), fields))
	cases := map[string]string{
		`{ span.foo = "x" }`:                           `false`,
		`{ span.foo != "x" }`:                          `false`,
		`{ span.foo != nil }`:                          `false`,
		`{ span.foo = nil }`:                           `true`,
		`{ resource.env = "prod" }`:                    `false`,
		`{ .foo = "x" }`:                               `false`,
		`{ span.foo = 1 || span.http.method = "GET" }`: `(false) or (['attributes.http.method'] == "GET")`,
		`{ span.foo = 1 && span.http.method = "GET" }`: `(false) and (['attributes.http.method'] == "GET")`,
		`{ status = error }`:                           `(['status.code'] =~ "error") or (['status.code'] =~ "STATUS_CODE_ERROR")`,
		`{ span.http.method = "GET" }`:                 `['attributes.http.method'] == "GET"`,
	}
	for q, want := range cases {
		expr, err := ParseFilter(q)
		if err != nil {
			t.Fatalf("%s: %v", q, err)
		}
		f := tr.Filter(expr)
		got := f.Where
		if got == "" && f.Exact {
			got = "true"
		}
		if got != want || !f.Exact {
			t.Errorf("%s:\n  got  %q exact=%v\n  want %q", q, got, f.Exact, want)
		}
	}
}

func TestParseFilterRejectsPipelines(t *testing.T) {
	if _, err := ParseFilter(`{ } | rate()`); err == nil {
		t.Error("expected error for metrics query")
	}
	if _, err := ParseFilter(`{ span.a = 1 } | by(span.a)`); err == nil {
		t.Error("expected error for by()")
	}
	if _, err := ParseFilter(`{ span.a = 1 } | select(span.b)`); err != nil {
		t.Errorf("select should be accepted: %v", err)
	}
}

func TestParseDurationLiteral(t *testing.T) {
	ns, err := ParseDurationLiteral("1.5s")
	if err != nil || ns != 1500000000 {
		t.Errorf("got %d, %v", ns, err)
	}
	ns, err = ParseDurationLiteral("100ms")
	if err != nil || ns != 100000000 {
		t.Errorf("got %d, %v", ns, err)
	}
}
