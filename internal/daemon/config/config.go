package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	DefaultMode                   = "standalone"
	DefaultLogLevel               = "info"
	DefaultLogFormat              = "text"
	DefaultGRPCAddr               = "127.0.0.1:9091"
	DefaultBootstrapAdminUsername = "admin"
)

type Config struct {
	DataDir                   string
	Mode                      string
	LogLevel                  string
	LogFormat                 string
	GRPCAddr                  string
	UserStoreEncryptionKeyB64 string
	BootstrapAdminUsername    string
	BootstrapAdminPassword    string
	TLSCertFile               string
	TLSKeyFile                string
	TLSClientCAFile           string
	TLSRequireClientCert      bool
}

func LoadFromEnv() (Config, error) {
	dataDir := strings.TrimSpace(os.Getenv("MYCELD_DATA_DIR"))
	if dataDir == "" {
		var err error
		dataDir, err = DefaultDataDir()
		if err != nil {
			return Config{}, err
		}
	}
	cfg := Config{
		DataDir:                   dataDir,
		Mode:                      valueOrDefault(os.Getenv("MYCELD_MODE"), DefaultMode),
		LogLevel:                  valueOrDefault(os.Getenv("MYCELD_LOG_LEVEL"), DefaultLogLevel),
		LogFormat:                 valueOrDefault(os.Getenv("MYCELD_LOG_FORMAT"), DefaultLogFormat),
		GRPCAddr:                  valueOrDefault(os.Getenv("MYCELD_GRPC_ADDR"), DefaultGRPCAddr),
		UserStoreEncryptionKeyB64: strings.TrimSpace(os.Getenv("MYCELD_USER_STORE_ENCRYPTION_KEY_B64")),
		BootstrapAdminUsername:    strings.TrimSpace(os.Getenv("MYCELD_BOOTSTRAP_ADMIN_USERNAME")),
		BootstrapAdminPassword:    os.Getenv("MYCELD_BOOTSTRAP_ADMIN_PASSWORD"),
		TLSCertFile:               strings.TrimSpace(os.Getenv("MYCELD_TLS_CERT_FILE")),
		TLSKeyFile:                strings.TrimSpace(os.Getenv("MYCELD_TLS_KEY_FILE")),
		TLSClientCAFile:           strings.TrimSpace(os.Getenv("MYCELD_TLS_CLIENT_CA_FILE")),
		TLSRequireClientCert:      parseBoolEnv(os.Getenv("MYCELD_TLS_REQUIRE_CLIENT_CERT")),
	}
	return cfg, cfg.Validate()
}

func DefaultDataDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return "", fmt.Errorf("could not resolve home directory for default MYCELD_DATA_DIR")
	}
	return filepath.Join(home, "mycel_data"), nil
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.DataDir) == "" {
		return fmt.Errorf("MYCELD_DATA_DIR must not be empty")
	}
	switch strings.ToLower(strings.TrimSpace(c.Mode)) {
	case "standalone", "mesh":
	default:
		return fmt.Errorf("MYCELD_MODE must be standalone or mesh")
	}
	switch strings.ToLower(strings.TrimSpace(c.LogLevel)) {
	case "debug", "info", "warn", "error":
	default:
		return fmt.Errorf("MYCELD_LOG_LEVEL must be debug, info, warn, or error")
	}
	switch strings.ToLower(strings.TrimSpace(c.LogFormat)) {
	case "text", "json":
	default:
		return fmt.Errorf("MYCELD_LOG_FORMAT must be text or json")
	}
	if strings.TrimSpace(c.GRPCAddr) == "" {
		return fmt.Errorf("MYCELD_GRPC_ADDR must not be empty")
	}
	if strings.TrimSpace(c.BootstrapAdminUsername) != "" && c.BootstrapAdminPassword == "" {
		return fmt.Errorf("MYCELD_BOOTSTRAP_ADMIN_PASSWORD must be set when MYCELD_BOOTSTRAP_ADMIN_USERNAME is set")
	}
	certSet := strings.TrimSpace(c.TLSCertFile) != ""
	keySet := strings.TrimSpace(c.TLSKeyFile) != ""
	if certSet != keySet {
		return fmt.Errorf("MYCELD_TLS_CERT_FILE and MYCELD_TLS_KEY_FILE must be set together")
	}
	if strings.TrimSpace(c.TLSClientCAFile) != "" && !certSet {
		return fmt.Errorf("MYCELD_TLS_CLIENT_CA_FILE requires MYCELD_TLS_CERT_FILE and MYCELD_TLS_KEY_FILE")
	}
	if c.TLSRequireClientCert && strings.TrimSpace(c.TLSClientCAFile) == "" {
		return fmt.Errorf("MYCELD_TLS_REQUIRE_CLIENT_CERT requires MYCELD_TLS_CLIENT_CA_FILE")
	}
	return nil
}

func parseBoolEnv(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	default:
		return false
	}
}

func valueOrDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}
