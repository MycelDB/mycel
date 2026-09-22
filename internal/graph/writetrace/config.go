package writetrace

import (
	"os"
	"strconv"
	"strings"
	"time"
)

// Config controls low-overhead graph write timing logs. It is intentionally
// small and env-driven so production investigations can enable it without
// changing daemon behavior or payload logging.
type Config struct {
	Enabled   bool
	Threshold time.Duration
}

// FromEnv reads graph write timing controls from the process environment.
//
//   - MYCELD_GRAPH_WRITE_TRACE enables timing logs when true/1/yes/on.
//   - MYCELD_GRAPH_WRITE_TRACE_THRESHOLD suppresses logs faster than the
//     threshold. It accepts Go durations such as "250ms" or an integer number
//     of milliseconds for operator convenience.
func FromEnv() Config {
	return Config{
		Enabled:   parseBool(os.Getenv("MYCELD_GRAPH_WRITE_TRACE")),
		Threshold: parseDuration(os.Getenv("MYCELD_GRAPH_WRITE_TRACE_THRESHOLD")),
	}
}

func (c Config) ShouldLog(duration time.Duration) bool {
	if !c.Enabled {
		return false
	}
	return c.Threshold <= 0 || duration >= c.Threshold
}

func MS(d time.Duration) float64 { return float64(d.Microseconds()) / 1000.0 }

func parseBool(raw string) bool {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "t", "true", "y", "yes", "on", "enabled":
		return true
	default:
		return false
	}
}

func parseDuration(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	if d, err := time.ParseDuration(raw); err == nil {
		return d
	}
	if ms, err := strconv.ParseInt(raw, 10, 64); err == nil && ms >= 0 {
		return time.Duration(ms) * time.Millisecond
	}
	return 0
}
