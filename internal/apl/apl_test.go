package apl

import (
	"testing"
	"time"
)

func TestIdent(t *testing.T) {
	cases := map[string]string{
		"name":            "name",
		"trace_id":        "trace_id",
		"service.name":    "['service.name']",
		"attributes.http": "['attributes.http']",
		"kind":            "['kind']",
		"1abc":            "['1abc']",
		"it's":            `['it\'s']`,
	}
	for in, want := range cases {
		if got := Ident(in); got != want {
			t.Errorf("Ident(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestString(t *testing.T) {
	if got := String(`a"b\c` + "\n"); got != `"a\"b\\c\n"` {
		t.Errorf("String = %s", got)
	}
}

func TestTimespan(t *testing.T) {
	cases := map[time.Duration]string{
		0:                       "0s",
		time.Second:             "1s",
		1500 * time.Millisecond: "1500ms",
		2 * time.Minute:         "2m",
		3 * time.Hour:           "3h",
		48 * time.Hour:          "2d",
		250 * time.Microsecond:  "250microsecond",
		1234:                    "totimespan(1234)",
		-time.Second:            "-1s",
	}
	for in, want := range cases {
		if got := Timespan(in); got != want {
			t.Errorf("Timespan(%v) = %q, want %q", in, got, want)
		}
	}
}

func TestAndOr(t *testing.T) {
	if got := And("a == 1", "", "b == 2"); got != "(a == 1) and (b == 2)" {
		t.Errorf("And = %q", got)
	}
	if got := And("a == 1"); got != "a == 1" {
		t.Errorf("And single = %q", got)
	}
	if got := Or(); got != "" {
		t.Errorf("Or empty = %q", got)
	}
}

func TestQuery(t *testing.T) {
	q := NewQuery("otel-traces").
		Where(`trace_id == "abc"`).
		Where("").
		Summarize([]string{"count()"}, []string{"bin(_time, 1m)"}).
		Limit(10)
	want := "['otel-traces']\n| where trace_id == \"abc\"\n| summarize count() by bin(_time, 1m)\n| limit 10"
	if got := q.String(); got != want {
		t.Errorf("Query =\n%s\nwant\n%s", got, want)
	}
}

func TestFloat(t *testing.T) {
	if got := Float(0.9); got != "0.9" {
		t.Errorf("Float(0.9) = %q", got)
	}
	if got := Float(5); got != "5.0" {
		t.Errorf("Float(5) = %q", got)
	}
}
