package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

const (
	DefaultMode      = "standalone"
	DefaultLogLevel  = "info"
	DefaultLogFormat = "text"
)

type Config struct {
	DataDir   string
	Mode      string
	LogLevel  string
	LogFormat string
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
		DataDir:   dataDir,
		Mode:      valueOrDefault(os.Getenv("MYCELD_MODE"), DefaultMode),
		LogLevel:  valueOrDefault(os.Getenv("MYCELD_LOG_LEVEL"), DefaultLogLevel),
		LogFormat: valueOrDefault(os.Getenv("MYCELD_LOG_FORMAT"), DefaultLogFormat),
	}
	return cfg, cfg.Validate()
}

func DefaultDataDir() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil || strings.TrimSpace(home) == "" {
		return "", fmt.Errorf("could not resolve home directory for default MYCELD_DATA_DIR")
	}
	return filepath.Join(home, ".mycel"), nil
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
	return nil
}

func valueOrDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}
