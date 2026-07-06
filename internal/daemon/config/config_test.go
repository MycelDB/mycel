package config

import (
	"path/filepath"
	"testing"
	"time"
)

func TestLoadFromEnvDefaults(t *testing.T) {
	t.Setenv("MYCELD_DATA_DIR", "")
	t.Setenv("MYCELD_MODE", "")
	t.Setenv("MYCELD_LOG_LEVEL", "")
	t.Setenv("MYCELD_LOG_FORMAT", "")
	t.Setenv("MYCELD_GRPC_ADDR", "")
	t.Setenv("MYCELD_BOOTSTRAP_ADMIN_USERNAME", "")
	t.Setenv("MYCELD_BOOTSTRAP_ADMIN_PASSWORD", "")
	t.Setenv("MYCELD_TLS_CERT_FILE", "")
	t.Setenv("MYCELD_TLS_KEY_FILE", "")
	t.Setenv("MYCELD_TLS_CLIENT_CA_FILE", "")
	t.Setenv("MYCELD_TLS_REQUIRE_CLIENT_CERT", "")

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
	if !cfg.SemanticMaintenance.Enabled || cfg.SemanticMaintenance.DirtyCooldown != DefaultSemanticDirtyCooldown || cfg.SemanticMaintenance.WorkerCount != DefaultSemanticWorkerCount || cfg.SemanticMaintenance.MaxBatchSize != DefaultSemanticMaxBatchSize {
		t.Fatalf("unexpected semantic maintenance defaults: %+v", cfg.SemanticMaintenance)
	}
}

func TestLoadFromEnvBootstrapAdminCredentials(t *testing.T) {
	t.Setenv("MYCELD_DATA_DIR", t.TempDir())
	t.Setenv("MYCELD_BOOTSTRAP_ADMIN_USERNAME", "bootstrap-admin")
	t.Setenv("MYCELD_BOOTSTRAP_ADMIN_PASSWORD", "bootstrap-password")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}
	if cfg.BootstrapAdminUsername != "bootstrap-admin" || cfg.BootstrapAdminPassword != "bootstrap-password" {
		t.Fatalf("unexpected bootstrap credentials in config: username=%q password=%q", cfg.BootstrapAdminUsername, cfg.BootstrapAdminPassword)
	}
}

func TestLoadFromEnvSemanticMaintenanceOverrides(t *testing.T) {
	t.Setenv("MYCELD_DATA_DIR", t.TempDir())
	t.Setenv("MYCELD_SEMANTIC_MAINTENANCE_ENABLED", "false")
	t.Setenv("MYCELD_SEMANTIC_DIRTY_COOLDOWN", "2m")
	t.Setenv("MYCELD_SEMANTIC_ANALYZER_INTERVAL", "3s")
	t.Setenv("MYCELD_SEMANTIC_WORKER_INTERVAL", "4s")
	t.Setenv("MYCELD_SEMANTIC_WORKER_COUNT", "5")
	t.Setenv("MYCELD_SEMANTIC_MAX_BATCH_SIZE", "50")
	t.Setenv("MYCELD_SEMANTIC_CREDENTIAL_MAX_REQUESTS_PER_MINUTE", "12")
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}
	sem := cfg.SemanticMaintenance
	if sem.Enabled || sem.DirtyCooldown != 2*time.Minute || sem.AnalyzerInterval != 3*time.Second || sem.WorkerInterval != 4*time.Second || sem.WorkerCount != 5 || sem.MaxBatchSize != 50 || sem.CredentialDefaults.MaxRequestsPerMinute != 12 {
		t.Fatalf("unexpected semantic overrides: %+v", sem)
	}
}

func TestConfigValidateRejectsBadValues(t *testing.T) {
	cases := []Config{
		{DataDir: "", Mode: "standalone", LogLevel: "info", LogFormat: "text", GRPCAddr: DefaultGRPCAddr},
		{DataDir: "/tmp/mycel", Mode: "bad", LogLevel: "info", LogFormat: "text", GRPCAddr: DefaultGRPCAddr},
		{DataDir: "/tmp/mycel", Mode: "standalone", LogLevel: "noisy", LogFormat: "text", GRPCAddr: DefaultGRPCAddr},
		{DataDir: "/tmp/mycel", Mode: "standalone", LogLevel: "info", LogFormat: "xml", GRPCAddr: DefaultGRPCAddr},
		{DataDir: "/tmp/mycel", Mode: "standalone", LogLevel: "info", LogFormat: "text", GRPCAddr: ""},
		{DataDir: "/tmp/mycel", Mode: "standalone", LogLevel: "info", LogFormat: "text", GRPCAddr: DefaultGRPCAddr, BootstrapAdminUsername: "admin"},
		{DataDir: "/tmp/mycel", Mode: "standalone", LogLevel: "info", LogFormat: "text", GRPCAddr: DefaultGRPCAddr, TLSCertFile: "cert.pem"},
		{DataDir: "/tmp/mycel", Mode: "standalone", LogLevel: "info", LogFormat: "text", GRPCAddr: DefaultGRPCAddr, TLSKeyFile: "key.pem"},
		{DataDir: "/tmp/mycel", Mode: "standalone", LogLevel: "info", LogFormat: "text", GRPCAddr: DefaultGRPCAddr, TLSClientCAFile: "ca.pem"},
		{DataDir: "/tmp/mycel", Mode: "standalone", LogLevel: "info", LogFormat: "text", GRPCAddr: DefaultGRPCAddr, TLSCertFile: "cert.pem", TLSKeyFile: "key.pem", TLSRequireClientCert: true},
	}
	for _, tc := range cases {
		if err := tc.Validate(); err == nil {
			t.Fatalf("Validate(%+v) expected error", tc)
		}
	}
}
