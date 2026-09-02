// Command schema-check inspects an Axiom trace dataset and runs the APL
// constructs the proxy relies on, reporting what works. Use it before
// pointing the proxy at a new dataset.
//
//	AXIOM_TOKEN=... go run ./cmd/schema-check -dataset otel-traces
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/velddev/axiom-tempo-proxy/internal/axiom"
	"github.com/velddev/axiom-tempo-proxy/internal/schema"
)

func main() {
	dataset := flag.String("dataset", os.Getenv("AXIOM_DATASET"), "dataset to inspect")
	baseURL := flag.String("axiom-url", envOr("AXIOM_URL", axiom.DefaultBaseURL), "Axiom API base URL")
	lookback := flag.Duration("lookback", 24*time.Hour, "how far back to sample")
	flag.Parse()
	if *dataset == "" {
		fmt.Fprintln(os.Stderr, "a dataset is required (-dataset or AXIOM_DATASET)")
		os.Exit(2)
	}
	client, err := axiom.New(axiom.Config{BaseURL: *baseURL, Token: os.Getenv("AXIOM_TOKEN"), OrgID: os.Getenv("AXIOM_ORG_ID")})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	end := time.Now()
	start := end.Add(-*lookback)
	ds := "['" + *dataset + "']"
	cfg := schema.DefaultConfig()

	fmt.Printf("== fields of %s ==\n", *dataset)
	fields, err := client.ListFields(ctx, *dataset)
	if err != nil {
		fmt.Println("  ListFields failed:", err)
	} else {
		sort.Slice(fields, func(i, j int) bool { return fields[i].Name < fields[j].Name })
		for _, f := range fields {
			fmt.Printf("  %-50s %s\n", f.Name, f.Type)
		}
		m := schema.New(cfg, fields)
		for _, want := range []string{cfg.TraceID, cfg.SpanID, cfg.ParentSpanID, cfg.Name, cfg.Kind, cfg.Duration, cfg.StatusCode, cfg.ServiceName} {
			if !m.HasField(want) {
				fmt.Printf("  MISSING expected column %q\n", want)
			}
		}
	}

	run := func(title, apl string) {
		fmt.Printf("\n== %s ==\n%s\n", title, apl)
		res, err := client.Query(ctx, apl, axiom.QueryOptions{Start: start, End: end})
		if err != nil {
			fmt.Println("  FAILED:", err)
			return
		}
		t := res.FirstTable()
		if t == nil {
			fmt.Println("  (no table)")
			return
		}
		names := make([]string, len(t.Fields))
		for i, f := range t.Fields {
			names[i] = f.Name + ":" + f.Type
		}
		fmt.Println("  columns:", strings.Join(names, ", "))
		for i := 0; i < t.NumRows() && i < 3; i++ {
			row := map[string]json.RawMessage{}
			for _, f := range t.Fields {
				row[f.Name] = t.Row(i).Raw(f.Name)
			}
			b, _ := json.Marshal(row)
			fmt.Println("  row:", truncate(string(b), 600))
		}
		fmt.Printf("  rows: %d\n", t.NumRows())
	}

	run("sample span (raw encodings of duration, status, kind, events)",
		ds+"\n| limit 2")
	run("root span predicate",
		ds+"\n| where isempty("+ident(cfg.ParentSpanID)+")\n| summarize count()")
	run("status values",
		ds+"\n| summarize c = count() by v = "+ident(cfg.StatusCode)+", e = "+ident(cfg.Error)+"\n| top 10 by c")
	run("kind values",
		ds+"\n| summarize c = count() by v = "+ident(cfg.Kind)+"\n| top 10 by c")
	run("duration arithmetic (timespan / 1s, / 1ms)",
		ds+"\n| extend s = toreal("+ident(cfg.Duration)+" / 1s), ms = "+ident(cfg.Duration)+" / 1ms\n| project "+ident(cfg.Duration)+", s, ms\n| limit 2")
	run("duration comparison literal",
		ds+"\n| where "+ident(cfg.Duration)+" > 100ms\n| summarize count()")
	run("map key access",
		ds+"\n| where isnotnull("+ident(cfg.SpanCustomMap)+")\n| project k = bag_keys("+ident(cfg.SpanCustomMap)+")\n| limit 3")
	run("per-trace rollup with arg_min",
		ds+"\n| summarize n = count(), start = min(_time), root = arg_min(_time, "+ident(cfg.Name)+", "+ident(cfg.ServiceName)+") by "+ident(cfg.TraceID)+"\n| sort by start desc\n| limit 2")
	run("countif per trace",
		ds+"\n| summarize m0 = countif(isempty("+ident(cfg.ParentSpanID)+")), m1 = count() by "+ident(cfg.TraceID)+"\n| where m0 > 0\n| limit 2")
	run("trace id in()",
		ds+"\n| summarize by "+ident(cfg.TraceID)+"\n| limit 2")
	run("rate-style bucketed count",
		ds+"\n| summarize v = count() by _bucket = bin(_time, 1m), g0 = "+ident(cfg.ServiceName)+"\n| sort by _bucket asc\n| limit 3")
	run("percentile of duration seconds",
		ds+"\n| summarize q0 = percentile(toreal("+ident(cfg.Duration)+" / 1s), 90.0), q1 = percentile(toreal("+ident(cfg.Duration)+" / 1s), 50.0) by _bucket = bin(_time, 5m)\n| limit 3")
	run("log2 histogram bucketing",
		ds+"\n| extend _bucketv = pow(2.0, ceiling(log2(toreal("+ident(cfg.Duration)+" / 1s) * 1000000000.0)))\n| summarize v = count() by _bucketv\n| limit 5")
	run("iff selection flag",
		ds+"\n| extend _sel = iff("+ident(cfg.StatusCode)+` =~ "error", true, false)`+"\n| summarize v = count() by _sel")
	run("regex match",
		ds+"\n| where "+ident(cfg.Name)+` matches regex @"^(?:GET.*)$"`+"\n| summarize count()")
	run("events shape",
		ds+"\n| where isnotnull("+ident(cfg.Events)+")\n| project "+ident(cfg.Events)+"\n| limit 1")
	run("links shape",
		ds+"\n| where isnotnull("+ident(cfg.Links)+")\n| project "+ident(cfg.Links)+"\n| limit 1")
}

func ident(name string) string {
	return "['" + name + "']"
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
