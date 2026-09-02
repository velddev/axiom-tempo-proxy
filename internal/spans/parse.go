package spans

import (
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/tempo/pkg/traceql"
	"github.com/grafana/tempo/pkg/util"

	"github.com/velddev/axiom-tempo-proxy/internal/axiom"
	"github.com/velddev/axiom-tempo-proxy/internal/schema"
)

// Parser turns Axiom result rows into Spans using the dataset layout.
type Parser struct {
	cfg      schema.Config
	topLevel map[string]bool
}

// NewParser creates a Parser for a mapping's layout.
func NewParser(m *schema.Mapping) *Parser {
	cfg := m.Config()
	p := &Parser{cfg: cfg, topLevel: map[string]bool{}}
	for _, f := range cfg.TopLevelResourceFields {
		p.topLevel[f] = true
	}
	return p
}

type colKind int

const (
	colIgnore colKind = iota
	colTime
	colTraceID
	colSpanID
	colParentSpanID
	colName
	colKind_
	colDuration
	colStatusCode
	colStatusMessage
	colError
	colServiceName
	colScopeName
	colScopeVersion
	colEvents
	colLinks
	colTraceState
	colSpanAttr
	colSpanMap
	colResourceAttr
	colResourceMap
)

type column struct {
	name string
	kind colKind
	key  string // attribute key for attr columns
}

func (p *Parser) classify(fields []axiom.Field) []column {
	cols := make([]column, len(fields))
	c := p.cfg
	for i, f := range fields {
		col := column{name: f.Name}
		switch f.Name {
		case c.Time:
			col.kind = colTime
		case c.TraceID:
			col.kind = colTraceID
		case c.SpanID:
			col.kind = colSpanID
		case c.ParentSpanID:
			col.kind = colParentSpanID
		case c.Name:
			col.kind = colName
		case c.Kind:
			col.kind = colKind_
		case c.Duration:
			col.kind = colDuration
		case c.StatusCode:
			col.kind = colStatusCode
		case c.StatusMessage:
			col.kind = colStatusMessage
		case c.Error:
			col.kind = colError
		case c.ServiceName:
			col.kind = colServiceName
		case c.ScopeName:
			col.kind = colScopeName
		case c.ScopeVersion:
			col.kind = colScopeVersion
		case c.Events:
			col.kind = colEvents
		case c.Links:
			col.kind = colLinks
		case c.TraceState:
			col.kind = colTraceState
		case c.SpanCustomMap:
			col.kind = colSpanMap
		case c.ResourceCustomMap:
			col.kind = colResourceMap
		default:
			switch {
			case p.topLevel[f.Name]:
				col.kind, col.key = colResourceAttr, f.Name
			case strings.HasPrefix(f.Name, c.SpanAttrPrefix):
				col.kind, col.key = colSpanAttr, strings.TrimPrefix(f.Name, c.SpanAttrPrefix)
			case strings.HasPrefix(f.Name, c.ResourceAttrPrefix):
				col.kind, col.key = colResourceAttr, strings.TrimPrefix(f.Name, c.ResourceAttrPrefix)
			case strings.HasPrefix(f.Name, "_"):
				col.kind = colIgnore
			default:
				col.kind, col.key = colSpanAttr, f.Name
			}
		}
		cols[i] = col
	}
	return cols
}

// Parse converts every row of the table into a Span. Rows without a
// trace id or span id are skipped.
func (p *Parser) Parse(t *axiom.Table) []*Span {
	if t == nil {
		return nil
	}
	cols := p.classify(t.Fields)
	out := make([]*Span, 0, t.NumRows())
	for i := 0; i < t.NumRows(); i++ {
		row := t.Row(i)
		s := p.parseRow(row, cols)
		if s != nil {
			out = append(out, s)
		}
	}
	return out
}

func (p *Parser) parseRow(row axiom.Row, cols []column) *Span {
	s := &Span{Status: traceql.StatusUnset}
	var errFlag, errSeen bool
	for _, col := range cols {
		if col.kind == colIgnore {
			continue
		}
		raw := row.Raw(col.name)
		if raw == nil {
			continue
		}
		switch col.kind {
		case colTime:
			if ts := row.Time(col.name); !ts.IsZero() {
				s.StartNs = uint64(ts.UnixNano())
			}
		case colTraceID:
			s.TraceID = traceIDBytes(row.String(col.name))
		case colSpanID:
			s.SpanID = spanIDBytes(row.String(col.name))
		case colParentSpanID:
			if v := row.String(col.name); v != "" {
				s.ParentSpanID = spanIDBytes(v)
			}
		case colName:
			s.Name = row.String(col.name)
		case colKind_:
			s.Kind = parseKind(row.String(col.name))
		case colDuration:
			s.DurationNs = uint64(row.Duration(col.name))
		case colStatusCode:
			s.Status = parseStatus(row.String(col.name))
		case colStatusMessage:
			s.StatusMessage = row.String(col.name)
		case colError:
			errFlag, errSeen = row.Bool(col.name), true
		case colServiceName:
			s.ServiceName = row.String(col.name)
			s.ResourceAttrs = append(s.ResourceAttrs, Attr{Key: col.name, Val: traceql.NewStaticString(s.ServiceName)})
		case colScopeName:
			s.ScopeName = row.String(col.name)
		case colScopeVersion:
			s.ScopeVersion = row.String(col.name)
		case colTraceState:
			s.TraceState = row.String(col.name)
		case colEvents:
			s.Events = parseEvents(raw)
		case colLinks:
			s.Links = parseLinks(raw)
		case colSpanAttr:
			if v, ok := StaticFromJSON(raw); ok {
				s.SpanAttrs = append(s.SpanAttrs, Attr{Key: col.key, Val: v})
			}
		case colResourceAttr:
			if v, ok := StaticFromJSON(raw); ok {
				s.ResourceAttrs = append(s.ResourceAttrs, Attr{Key: col.key, Val: v})
			}
		case colSpanMap:
			s.SpanAttrs = appendFlattened(s.SpanAttrs, "", raw)
		case colResourceMap:
			s.ResourceAttrs = appendFlattened(s.ResourceAttrs, "", raw)
		}
	}
	if len(s.TraceID) == 0 || len(s.SpanID) == 0 {
		return nil
	}
	if errSeen && errFlag && s.Status == traceql.StatusUnset {
		s.Status = traceql.StatusError
	}
	return s
}

func traceIDBytes(s string) []byte {
	if b, err := util.HexStringToTraceID(s); err == nil {
		return b
	}
	return []byte(s)
}

func spanIDBytes(s string) []byte {
	if b, err := util.HexStringToSpanID(s); err == nil {
		return b
	}
	return []byte(s)
}

func parseKind(s string) traceql.Kind {
	switch strings.ToLower(strings.TrimPrefix(strings.ToLower(s), "span_kind_")) {
	case "server", "2":
		return traceql.KindServer
	case "client", "3":
		return traceql.KindClient
	case "producer", "4":
		return traceql.KindProducer
	case "consumer", "5":
		return traceql.KindConsumer
	case "internal", "1":
		return traceql.KindInternal
	}
	return traceql.KindUnspecified
}

func parseStatus(s string) traceql.Status {
	switch strings.ToLower(strings.TrimPrefix(strings.ToLower(s), "status_code_")) {
	case "error", "2":
		return traceql.StatusError
	case "ok", "1":
		return traceql.StatusOk
	}
	return traceql.StatusUnset
}

// StaticFromJSON converts a JSON scalar or array into a Static. Objects
// and nulls yield ok=false.
func StaticFromJSON(raw json.RawMessage) (traceql.Static, bool) {
	if len(raw) == 0 {
		return traceql.NewStaticNil(), false
	}
	switch raw[0] {
	case '"':
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return traceql.NewStaticNil(), false
		}
		return traceql.NewStaticString(s), true
	case 't', 'f':
		var b bool
		if err := json.Unmarshal(raw, &b); err != nil {
			return traceql.NewStaticNil(), false
		}
		return traceql.NewStaticBool(b), true
	case 'n':
		return traceql.NewStaticNil(), false
	case '[':
		var items []json.RawMessage
		if err := json.Unmarshal(raw, &items); err != nil {
			return traceql.NewStaticNil(), false
		}
		vals := make([]traceql.Static, 0, len(items))
		for _, it := range items {
			if v, ok := StaticFromJSON(it); ok {
				vals = append(vals, v)
			}
		}
		if len(vals) == 0 {
			return traceql.NewStaticNil(), false
		}
		if len(vals) == 1 {
			return arrayOf(vals), true
		}
		return combine(vals)
	case '{':
		return traceql.NewStaticNil(), false
	default:
		text := string(raw)
		if i, err := strconv.ParseInt(text, 10, 64); err == nil {
			return traceql.NewStaticInt(int(i)), true
		}
		if f, err := strconv.ParseFloat(text, 64); err == nil {
			return traceql.NewStaticFloat(f), true
		}
		return traceql.NewStaticString(text), true
	}
}

// arrayOf wraps a single value in a one-element array of its type.
func arrayOf(vals []traceql.Static) traceql.Static {
	v := vals[0]
	switch v.Type {
	case traceql.TypeInt:
		i, _ := v.Int()
		return traceql.NewStaticIntArray([]int{i})
	case traceql.TypeFloat:
		return traceql.NewStaticFloatArray([]float64{v.Float()})
	case traceql.TypeBoolean:
		b, _ := v.Bool()
		return traceql.NewStaticBooleanArray([]bool{b})
	}
	return traceql.NewStaticStringArray([]string{v.EncodeToString(false)})
}

// appendFlattened adds the keys of a JSON object as attributes, joining
// nested object keys with dots.
func appendFlattened(attrs []Attr, prefix string, raw json.RawMessage) []Attr {
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return attrs
	}
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		v := obj[k]
		key := k
		if prefix != "" {
			key = prefix + "." + k
		}
		if len(v) > 0 && v[0] == '{' {
			attrs = appendFlattened(attrs, key, v)
			continue
		}
		if st, ok := StaticFromJSON(v); ok {
			attrs = append(attrs, Attr{Key: key, Val: st})
		}
	}
	return attrs
}

func parseEvents(raw json.RawMessage) []Event {
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil
	}
	events := make([]Event, 0, len(items))
	for _, it := range items {
		e := Event{}
		for k, v := range it {
			switch strings.ToLower(k) {
			case "name":
				e.Name = jsonString(v)
			case "timestamp", "time", "time_unix_nano", "timeunixnano", "_time":
				e.TimeNs = parseTimeNs(v)
			case "attributes":
				e.Attrs = appendFlattened(e.Attrs, "", v)
			case "dropped_attributes_count", "droppedattributescount":
			default:
				if st, ok := StaticFromJSON(v); ok {
					e.Attrs = append(e.Attrs, Attr{Key: k, Val: st})
				} else if len(v) > 0 && v[0] == '{' {
					e.Attrs = appendFlattened(e.Attrs, k, v)
				}
			}
		}
		events = append(events, e)
	}
	return events
}

func parseLinks(raw json.RawMessage) []Link {
	var items []map[string]json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil
	}
	links := make([]Link, 0, len(items))
	for _, it := range items {
		l := Link{}
		for k, v := range it {
			switch strings.ToLower(k) {
			case "trace_id", "traceid":
				l.TraceID = traceIDBytes(jsonString(v))
			case "span_id", "spanid":
				l.SpanID = spanIDBytes(jsonString(v))
			case "trace_state", "tracestate":
				l.TraceState = jsonString(v)
			case "attributes":
				l.Attrs = appendFlattened(l.Attrs, "", v)
			case "dropped_attributes_count", "droppedattributescount":
			default:
				if st, ok := StaticFromJSON(v); ok {
					l.Attrs = append(l.Attrs, Attr{Key: k, Val: st})
				} else if len(v) > 0 && v[0] == '{' {
					l.Attrs = appendFlattened(l.Attrs, k, v)
				}
			}
		}
		links = append(links, l)
	}
	return links
}

func jsonString(v json.RawMessage) string {
	var s string
	if json.Unmarshal(v, &s) == nil {
		return s
	}
	return strings.Trim(string(v), `"`)
}

// parseTimeNs accepts RFC3339 strings and integer nanosecond timestamps.
func parseTimeNs(v json.RawMessage) uint64 {
	s := jsonString(v)
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return uint64(t.UnixNano())
	}
	if n, err := strconv.ParseUint(s, 10, 64); err == nil {
		// Heuristic: values below 1e12 are seconds, below 1e15 millis.
		switch {
		case n < 1e12:
			return n * 1e9
		case n < 1e15:
			return n * 1e6
		case n < 1e18:
			return n * 1e3
		}
		return n
	}
	return 0
}
