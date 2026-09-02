package server

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/model"
)

// parseUnixSeconds parses an integer unix-seconds parameter. Missing
// values return the zero time.
func parseUnixSeconds(r *http.Request, name string) (time.Time, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return time.Time{}, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}, fmt.Errorf("invalid %s: %s", name, v)
	}
	return time.Unix(n, 0), nil
}

// parseTimestamp accepts Tempo's metrics timestamp forms: unix seconds
// (up to 10 digits), unix nanoseconds, fractional seconds, or RFC3339.
func parseTimestamp(v string) (time.Time, bool, error) {
	if v == "" {
		return time.Time{}, false, nil
	}
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t, true, nil
	}
	if strings.Contains(v, ".") {
		f, err := strconv.ParseFloat(v, 64)
		if err != nil {
			return time.Time{}, false, fmt.Errorf("invalid timestamp %q", v)
		}
		sec, frac := math.Modf(f)
		return time.Unix(int64(sec), int64(frac*1e9)), true, nil
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		return time.Time{}, false, fmt.Errorf("invalid timestamp %q", v)
	}
	if len(strings.TrimPrefix(v, "-")) > 10 {
		return time.Unix(0, n), true, nil
	}
	return time.Unix(n, 0), true, nil
}

// parseStep accepts float seconds or a Go duration.
func parseStep(v string) (time.Duration, error) {
	if v == "" {
		return 0, nil
	}
	if f, err := strconv.ParseFloat(v, 64); err == nil {
		if f < 0 {
			return 0, fmt.Errorf("step must be positive")
		}
		return time.Duration(f * float64(time.Second)), nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid step %q", v)
	}
	if d < 0 {
		return 0, fmt.Errorf("step must be positive")
	}
	return d, nil
}

// parsePromDuration parses a Prometheus-style duration like 15m or 3h.
func parsePromDuration(v string) (time.Duration, error) {
	if v == "" {
		return 0, nil
	}
	d, err := model.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("invalid duration %q", v)
	}
	return time.Duration(d), nil
}

func parseUint(r *http.Request, name string, def uint32) (uint32, error) {
	v := r.URL.Query().Get(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.ParseUint(v, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("invalid %s: %s", name, v)
	}
	return uint32(n), nil
}

// parseLogfmtTags parses Tempo's legacy tags parameter: space separated
// key=value pairs where values may be double quoted.
func parseLogfmtTags(s string) (map[string]string, error) {
	out := map[string]string{}
	i := 0
	for i < len(s) {
		for i < len(s) && s[i] == ' ' {
			i++
		}
		if i >= len(s) {
			break
		}
		eq := strings.IndexByte(s[i:], '=')
		if eq < 0 {
			return nil, fmt.Errorf("invalid tags: %q", s)
		}
		key := s[i : i+eq]
		i += eq + 1
		var val string
		if i < len(s) && s[i] == '"' {
			i++
			var b strings.Builder
			for i < len(s) && s[i] != '"' {
				if s[i] == '\\' && i+1 < len(s) {
					i++
				}
				b.WriteByte(s[i])
				i++
			}
			i++ // closing quote
			val = b.String()
		} else {
			end := strings.IndexByte(s[i:], ' ')
			if end < 0 {
				end = len(s) - i
			}
			val = s[i : i+end]
			i += end
		}
		if _, dup := out[key]; dup {
			return nil, fmt.Errorf("duplicate tag %q", key)
		}
		out[key] = val
	}
	return out, nil
}
