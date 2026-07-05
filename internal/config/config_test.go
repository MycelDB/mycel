package config

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/pflag"
)

func TestLoadDefaults(t *testing.T) {
	clearConfigEnv(t)

	cfg, err := Load(Options{})
	if err != nil {
		t.Fatalf("load defaults failed: %v", err)
	}
	if cfg.Output != "text" {
		t.Fatalf("expected text output default, got %+v", cfg)
	}
	if cfg.DaemonAddr != "" || cfg.Username != "" || cfg.Password != "" || cfg.DaemonTLS {
		t.Fatalf("expected empty daemon/auth defaults, got %+v", cfg)
	}
}

func TestLoadPrecedenceFileEnvFlags(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("MYCEL_OUTPUT", "json")
	t.Setenv("MYCELD_GRPC_ADDR", "127.0.0.1:7777")
	t.Setenv("MYCELD_TLS", "true")
	configPath := filepath.Join(t.TempDir(), "mycel.yaml")
	if err := os.WriteFile(configPath, []byte(`
output: text
username: file-user
password: file-pass
daemon:
  addr: 127.0.0.1:5555
  tls_ca: /file/ca.pem
  tls_server_name: file-server
`), 0o600); err != nil {
		t.Fatalf("write config failed: %v", err)
	}
	flags := testFlags(t,
		"--config", configPath,
		"--username", "flag-user",
		"--daemon-addr", "127.0.0.1:9999",
		"--daemon-tls-client-cert", "/flag/client.pem",
		"--daemon-tls-client-key", "/flag/client-key.pem",
	)

	cfg, err := Load(Options{Flags: flags})
	if err != nil {
		t.Fatalf("load config failed: %v", err)
	}
	if cfg.Output != "json" {
		t.Fatalf("expected env output override, got %q", cfg.Output)
	}
	if cfg.Username != "flag-user" || cfg.Password != "file-pass" {
		t.Fatalf("unexpected credentials: %+v", cfg)
	}
	if cfg.DaemonAddr != "127.0.0.1:9999" || !cfg.DaemonTLS {
		t.Fatalf("unexpected daemon connection config: %+v", cfg)
	}
	if cfg.DaemonTLSCAFile != "/file/ca.pem" || cfg.DaemonTLSServerName != "file-server" {
		t.Fatalf("expected file TLS settings, got %+v", cfg)
	}
	if cfg.DaemonTLSClientCertFile != "/flag/client.pem" || cfg.DaemonTLSClientKeyFile != "/flag/client-key.pem" {
		t.Fatalf("expected flag mTLS settings, got %+v", cfg)
	}
}

func TestLoadRejectsInvalidOutput(t *testing.T) {
	clearConfigEnv(t)
	if _, err := Load(Options{Flags: testFlags(t, "--output", "xml")}); err == nil {
		t.Fatal("expected validation error")
	}
}

func TestLoadRejectsPartialClientCertificate(t *testing.T) {
	clearConfigEnv(t)
	if _, err := Load(Options{Flags: testFlags(t, "--daemon-tls-client-cert", "/client.pem")}); err == nil {
		t.Fatal("expected validation error")
	}
}

func clearConfigEnv(t *testing.T) {
	t.Helper()
	envNames := []string{
		EnvConfig,
		"MYCEL_OUTPUT",
		"MYCEL_USERNAME",
		"MYCEL_PASSWORD",
		"MYCELD_GRPC_ADDR",
		"MYCELD_TLS",
		"MYCELD_TLS_CA_FILE",
		"MYCELD_TLS_SERVER_NAME",
		"MYCELD_TLS_INSECURE_SKIP_VERIFY",
		"MYCELD_TLS_CLIENT_CERT_FILE",
		"MYCELD_TLS_CLIENT_KEY_FILE",
	}
	for _, name := range envNames {
		t.Setenv(name, "")
	}
}

func testFlags(t *testing.T, args ...string) *pflag.FlagSet {
	t.Helper()
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("config", "", "")
	flags.String("output", "", "")
	flags.String("username", "", "")
	flags.String("password", "", "")
	flags.String("daemon-addr", "", "")
	flags.Bool("daemon-tls", false, "")
	flags.String("daemon-tls-ca", "", "")
	flags.String("daemon-tls-server-name", "", "")
	flags.Bool("daemon-tls-insecure-skip-verify", false, "")
	flags.String("daemon-tls-client-cert", "", "")
	flags.String("daemon-tls-client-key", "", "")
	if err := flags.Parse(args); err != nil {
		t.Fatalf("parse flags failed: %v", err)
	}
	return flags
}
