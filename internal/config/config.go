// Package config loads proxy configuration from environment variables and
// flags.
package config

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config is the proxy configuration.
type Config struct {
	// ListenAddr is the HTTP listen address.
	ListenAddr string

	// AxiomURL is the Axiom API base URL.
	AxiomURL string
	// AxiomToken is the Axiom API token.
	AxiomToken string
	// AxiomOrgID is required for personal tokens.
	AxiomOrgID string
	// AxiomQueryPath overrides the APL query endpoint path.
	AxiomQueryPath string

	// Dataset is the default trace dataset, used when a request does not
	// select one. Optional.
	Dataset string
	// DatasetHeader names a request header that may override Dataset.
	DatasetHeader string
	// AllowedDatasets restricts header overrides; empty allows any.
	AllowedDatasets []string

	// SchemaRefresh is how often dataset fields are re-discovered.
	SchemaRefresh time.Duration

	// MaxSearchTraces caps the traces returned by a search.
	MaxSearchTraces int
	// MaxSpansPerFetch caps the spans pulled from Axiom for one search
	// (across all of its batches) or one trace-by-id query.
	MaxSpansPerFetch int
	// SearchBatchTraces is how many candidate trace ids one span-pull
	// query of a search asks for.
	SearchBatchTraces int
	// MaxTagValues caps values returned for tag autocomplete.
	MaxTagValues int
	// TagSampleRows is how many rows are sampled to discover map keys.
	TagSampleRows int
	// DefaultLookback is the search window when the client omits one.
	DefaultLookback time.Duration
	// TraceLookback is the window searched for trace-by-id when the
	// client omits start/end.
	TraceLookback time.Duration
	// QueryTimeout bounds one Axiom query.
	QueryTimeout time.Duration

	// NoPreferSelected disables ranking search candidates that carry the
	// attributes a query select()s ahead of those that do not.
	NoPreferSelected bool

	// LogLevel is debug, info, warn, or error.
	LogLevel string
	// LogQueries logs every generated APL query.
	LogQueries bool
}

// Default returns the default configuration.
func Default() Config {
	return Config{
		ListenAddr:        ":3200",
		AxiomURL:          "https://api.axiom.co",
		DatasetHeader:     "X-Axiom-Dataset",
		SchemaRefresh:     5 * time.Minute,
		MaxSearchTraces:   500,
		MaxSpansPerFetch:  50000,
		SearchBatchTraces: 50,
		MaxTagValues:      5000,
		TagSampleRows:     2000,
		DefaultLookback:   time.Hour,
		TraceLookback:     24 * time.Hour,
		QueryTimeout:      60 * time.Second,
		LogLevel:          "info",
	}
}

// Load builds the configuration from defaults, environment variables, then
// flags. Flags are parsed from args (without the program name).
func Load(args []string) (Config, error) {
	c := Default()

	env := func(key string, dst *string) {
		if v, ok := os.LookupEnv(key); ok {
			*dst = v
		}
	}
	env("PROXY_LISTEN_ADDR", &c.ListenAddr)
	env("AXIOM_URL", &c.AxiomURL)
	env("AXIOM_TOKEN", &c.AxiomToken)
	env("AXIOM_ORG_ID", &c.AxiomOrgID)
	env("AXIOM_QUERY_PATH", &c.AxiomQueryPath)
	env("AXIOM_DATASET", &c.Dataset)
	env("PROXY_DATASET_HEADER", &c.DatasetHeader)
	env("PROXY_LOG_LEVEL", &c.LogLevel)
	if v, ok := os.LookupEnv("PROXY_ALLOWED_DATASETS"); ok {
		c.AllowedDatasets = splitList(v)
	}
	if v, ok := os.LookupEnv("PROXY_LOG_QUERIES"); ok {
		c.LogQueries = v == "1" || strings.EqualFold(v, "true")
	}
	if v, ok := os.LookupEnv("PROXY_NO_PREFER_SELECTED"); ok {
		c.NoPreferSelected = v == "1" || strings.EqualFold(v, "true")
	}
	envDur := func(key string, dst *time.Duration) error {
		if v, ok := os.LookupEnv(key); ok {
			d, err := time.ParseDuration(v)
			if err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
			*dst = d
		}
		return nil
	}
	envInt := func(key string, dst *int) error {
		if v, ok := os.LookupEnv(key); ok {
			n, err := strconv.Atoi(v)
			if err != nil {
				return fmt.Errorf("%s: %w", key, err)
			}
			*dst = n
		}
		return nil
	}
	for _, err := range []error{
		envDur("PROXY_SCHEMA_REFRESH", &c.SchemaRefresh),
		envDur("PROXY_DEFAULT_LOOKBACK", &c.DefaultLookback),
		envDur("PROXY_TRACE_LOOKBACK", &c.TraceLookback),
		envDur("PROXY_QUERY_TIMEOUT", &c.QueryTimeout),
		envInt("PROXY_MAX_SEARCH_TRACES", &c.MaxSearchTraces),
		envInt("PROXY_MAX_SPANS_PER_FETCH", &c.MaxSpansPerFetch),
		envInt("PROXY_SEARCH_BATCH_TRACES", &c.SearchBatchTraces),
		envInt("PROXY_MAX_TAG_VALUES", &c.MaxTagValues),
		envInt("PROXY_TAG_SAMPLE_ROWS", &c.TagSampleRows),
	} {
		if err != nil {
			return c, err
		}
	}

	fs := flag.NewFlagSet("axiom-tempo-proxy", flag.ContinueOnError)
	fs.StringVar(&c.ListenAddr, "listen", c.ListenAddr, "HTTP listen address")
	fs.StringVar(&c.AxiomURL, "axiom-url", c.AxiomURL, "Axiom API base URL")
	fs.StringVar(&c.AxiomOrgID, "axiom-org-id", c.AxiomOrgID, "Axiom org id (personal tokens)")
	fs.StringVar(&c.Dataset, "dataset", c.Dataset, "default Axiom dataset (optional; requests may select one via /{dataset}/api/... prefix)")
	fs.StringVar(&c.DatasetHeader, "dataset-header", c.DatasetHeader, "request header that selects the dataset")
	fs.StringVar(&c.LogLevel, "log-level", c.LogLevel, "log level")
	fs.BoolVar(&c.LogQueries, "log-queries", c.LogQueries, "log generated APL queries")
	fs.BoolVar(&c.NoPreferSelected, "no-prefer-selected", c.NoPreferSelected, "do not rank search candidates carrying select()ed attributes first")
	fs.DurationVar(&c.SchemaRefresh, "schema-refresh", c.SchemaRefresh, "dataset schema refresh interval")
	fs.DurationVar(&c.DefaultLookback, "default-lookback", c.DefaultLookback, "search window when none is given")
	fs.DurationVar(&c.TraceLookback, "trace-lookback", c.TraceLookback, "trace-by-id window when none is given")
	fs.DurationVar(&c.QueryTimeout, "query-timeout", c.QueryTimeout, "per-query timeout")
	fs.IntVar(&c.MaxSearchTraces, "max-search-traces", c.MaxSearchTraces, "cap on traces per search")
	fs.IntVar(&c.MaxSpansPerFetch, "max-spans-per-fetch", c.MaxSpansPerFetch, "cap on spans pulled per search or trace")
	fs.IntVar(&c.SearchBatchTraces, "search-batch-traces", c.SearchBatchTraces, "candidate traces per span-pull query")
	fs.IntVar(&c.MaxTagValues, "max-tag-values", c.MaxTagValues, "cap on tag values")
	fs.IntVar(&c.TagSampleRows, "tag-sample-rows", c.TagSampleRows, "rows sampled to discover map keys")
	if err := fs.Parse(args); err != nil {
		return c, err
	}

	if c.AxiomToken == "" {
		return c, fmt.Errorf("AXIOM_TOKEN is required")
	}
	return c, nil
}

// DatasetAllowed reports whether a header-selected dataset may be used.
func (c Config) DatasetAllowed(name string) bool {
	if name == c.Dataset {
		return true
	}
	if len(c.AllowedDatasets) == 0 {
		return true
	}
	for _, d := range c.AllowedDatasets {
		if d == name {
			return true
		}
	}
	return false
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
