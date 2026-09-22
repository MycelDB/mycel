package writetrace

import (
	"testing"
	"time"
)

func TestFromEnvParsesEnableAndThreshold(t *testing.T) {
	t.Setenv("MYCELD_GRAPH_WRITE_TRACE", "true")
	t.Setenv("MYCELD_GRAPH_WRITE_TRACE_THRESHOLD", "250ms")
	cfg := FromEnv()
	if !cfg.Enabled {
		t.Fatal("expected graph write tracing to be enabled")
	}
	if cfg.Threshold != 250*time.Millisecond {
		t.Fatalf("threshold = %v, want 250ms", cfg.Threshold)
	}
	if cfg.ShouldLog(100 * time.Millisecond) {
		t.Fatal("ShouldLog logged below threshold")
	}
	if !cfg.ShouldLog(300 * time.Millisecond) {
		t.Fatal("ShouldLog did not log above threshold")
	}
}

func TestFromEnvParsesIntegerThresholdAsMilliseconds(t *testing.T) {
	t.Setenv("MYCELD_GRAPH_WRITE_TRACE", "1")
	t.Setenv("MYCELD_GRAPH_WRITE_TRACE_THRESHOLD", "42")
	cfg := FromEnv()
	if cfg.Threshold != 42*time.Millisecond {
		t.Fatalf("threshold = %v, want 42ms", cfg.Threshold)
	}
}
