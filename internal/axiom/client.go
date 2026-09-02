// Package axiom is a minimal client for the Axiom REST API covering the
// endpoints the proxy needs: running APL queries and discovering dataset
// schemas.
package axiom

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	DefaultBaseURL   = "https://api.axiom.co"
	DefaultQueryPath = "/v1/datasets/_apl"
)

// Config configures a Client.
type Config struct {
	// BaseURL is the API origin, e.g. https://api.axiom.co or an edge URL.
	BaseURL string
	// Token is the API token sent as a Bearer credential.
	Token string
	// OrgID is required when Token is a personal token.
	OrgID string
	// QueryPath overrides the APL query endpoint path.
	QueryPath string
	// HTTPClient is optional; a default with a 60s timeout is used otherwise.
	HTTPClient *http.Client
	// UserAgent is sent on every request.
	UserAgent string
}

// Client talks to the Axiom API.
type Client struct {
	baseURL   string
	token     string
	orgID     string
	queryPath string
	userAgent string
	http      *http.Client
}

// New creates a Client. It returns an error if the config is unusable.
func New(cfg Config) (*Client, error) {
	if cfg.Token == "" {
		return nil, fmt.Errorf("axiom: token is required")
	}
	base := strings.TrimRight(cfg.BaseURL, "/")
	if base == "" {
		base = DefaultBaseURL
	}
	if _, err := url.Parse(base); err != nil {
		return nil, fmt.Errorf("axiom: invalid base url %q: %w", cfg.BaseURL, err)
	}
	qp := cfg.QueryPath
	if qp == "" {
		qp = DefaultQueryPath
	}
	hc := cfg.HTTPClient
	if hc == nil {
		hc = &http.Client{Timeout: 60 * time.Second}
	}
	ua := cfg.UserAgent
	if ua == "" {
		ua = "axiom-tempo-proxy"
	}
	return &Client{
		baseURL:   base,
		token:     cfg.Token,
		orgID:     cfg.OrgID,
		queryPath: qp,
		userAgent: ua,
		http:      hc,
	}, nil
}

// QueryOptions tune a single APL query.
type QueryOptions struct {
	// Start and End bound the query time range. Zero values are omitted, in
	// which case Axiom applies its own default range.
	Start, End time.Time
	// Resolution is the bin_auto resolution in seconds, or "auto".
	Resolution string
	// NoCache bypasses Axiom's query cache.
	NoCache bool
}

type queryRequest struct {
	APL          string        `json:"apl"`
	StartTime    string        `json:"startTime,omitempty"`
	EndTime      string        `json:"endTime,omitempty"`
	QueryOptions *queryOptions `json:"queryOptions,omitempty"`
}

type queryOptions struct {
	Resolution string `json:"resolution,omitempty"`
}

// Query runs an APL query and returns the tabular result.
func (c *Client) Query(ctx context.Context, apl string, opts QueryOptions) (*Result, error) {
	body := queryRequest{APL: apl}
	if !opts.Start.IsZero() {
		body.StartTime = opts.Start.UTC().Format(time.RFC3339Nano)
	}
	if !opts.End.IsZero() {
		body.EndTime = opts.End.UTC().Format(time.RFC3339Nano)
	}
	if opts.Resolution != "" {
		body.QueryOptions = &queryOptions{Resolution: opts.Resolution}
	}

	q := url.Values{}
	q.Set("format", "tabular")
	if opts.NoCache {
		q.Set("nocache", "true")
	}
	endpoint := c.baseURL + c.queryPath + "?" + q.Encode()

	var res Result
	if err := c.do(ctx, http.MethodPost, endpoint, body, &res); err != nil {
		return nil, err
	}
	res.APL = apl
	return &res, nil
}

// Dataset is a subset of the fields returned by GET /v2/datasets.
type Dataset struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Kind        string   `json:"kind"`
	MapFields   []string `json:"mapFields"`
}

// ListDatasets returns the datasets visible to the token.
func (c *Client) ListDatasets(ctx context.Context) ([]Dataset, error) {
	var out []Dataset
	if err := c.do(ctx, http.MethodGet, c.baseURL+"/v2/datasets", nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// DatasetField describes one field of a dataset as reported by
// GET /v2/datasets/{id}/fields.
type DatasetField struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
	Hidden      bool   `json:"hidden"`
	Unit        string `json:"unit"`
}

// ListFields returns the known fields of a dataset.
func (c *Client) ListFields(ctx context.Context, dataset string) ([]DatasetField, error) {
	var out []DatasetField
	endpoint := c.baseURL + "/v2/datasets/" + url.PathEscape(dataset) + "/fields"
	if err := c.do(ctx, http.MethodGet, endpoint, nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// APIError is returned for non-2xx responses.
type APIError struct {
	Status  int
	Message string
	TraceID string
}

func (e *APIError) Error() string {
	if e.TraceID != "" {
		return fmt.Sprintf("axiom: %d %s (trace %s)", e.Status, e.Message, e.TraceID)
	}
	return fmt.Sprintf("axiom: %d %s", e.Status, e.Message)
}

func (c *Client) do(ctx context.Context, method, endpoint string, in, out any) error {
	var reqBody io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return fmt.Errorf("axiom: encode request: %w", err)
		}
		reqBody = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, reqBody)
	if err != nil {
		return fmt.Errorf("axiom: build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+c.token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", c.userAgent)
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.orgID != "" {
		req.Header.Set("X-Axiom-Org-Id", c.orgID)
	}

	resp, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("axiom: %s %s: %w", method, endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return decodeError(resp)
	}
	if out == nil {
		return nil
	}
	dec := json.NewDecoder(resp.Body)
	dec.UseNumber()
	if err := dec.Decode(out); err != nil {
		return fmt.Errorf("axiom: decode response: %w", err)
	}
	return nil
}

func decodeError(resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	apiErr := &APIError{Status: resp.StatusCode, TraceID: resp.Header.Get("X-Axiom-Trace-Id")}
	var payload struct {
		Message string `json:"message"`
	}
	if json.Unmarshal(body, &payload) == nil && payload.Message != "" {
		apiErr.Message = payload.Message
	} else {
		apiErr.Message = strings.TrimSpace(string(body))
		if apiErr.Message == "" {
			apiErr.Message = http.StatusText(resp.StatusCode)
		}
	}
	return apiErr
}
