package config

import (
	"path/filepath"
	"testing"
)

func TestLoadFromEnvDefaults(t *testing.T) {
	t.Setenv("MYCELD_DATA_DIR", "")
	t.Setenv("MYCELD_MODE", "")
	t.Setenv("MYCELD_LOG_LEVEL", "")
	t.Setenv("MYCELD_LOG_FORMAT", "")
	t.Setenv("MYCELD_GRPC_ADDR", "")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}
	if filepath.Base(cfg.DataDir) != "mycel_data" {
		t.Fatalf("expected default data dir to end in mycel_data, got %q", cfg.DataDir)
	}
	if cfg.Mode != DefaultMode || cfg.LogLevel != DefaultLogLevel || cfg.LogFormat != DefaultLogFormat || cfg.GRPCAddr != DefaultGRPCAddr {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestConfigValidateRejectsBadValues(t *testing.T) {
	cases := []Config{
		{DataDir: "", Mode: "standalone", LogLevel: "info", LogFormat: "text", GRPCAddr: DefaultGRPCAddr},
		{DataDir: "/tmp/mycel", Mode: "bad", LogLevel: "info", LogFormat: "text", GRPCAddr: DefaultGRPCAddr},
		{DataDir: "/tmp/mycel", Mode: "standalone", LogLevel: "noisy", LogFormat: "text", GRPCAddr: DefaultGRPCAddr},
		{DataDir: "/tmp/mycel", Mode: "standalone", LogLevel: "info", LogFormat: "xml", GRPCAddr: DefaultGRPCAddr},
		{DataDir: "/tmp/mycel", Mode: "standalone", LogLevel: "info", LogFormat: "text", GRPCAddr: ""},
	}
	for _, tc := range cases {
		if err := tc.Validate(); err == nil {
			t.Fatalf("Validate(%+v) expected error", tc)
		}
	}
}
