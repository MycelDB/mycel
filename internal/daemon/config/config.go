package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultMode                             = "standalone"
	DefaultLogLevel                         = "info"
	DefaultLogFormat                        = "text"
	DefaultGRPCAddr                         = "127.0.0.1:9091"
	DefaultBootstrapAdminUsername           = "admin"
	DefaultSemanticDirtyCooldown            = 60 * time.Second
	DefaultSemanticAnalyzerInterval         = 5 * time.Second
	DefaultSemanticWorkerInterval           = 5 * time.Second
	DefaultSemanticWorkerCount              = 2
	DefaultSemanticMaxBatchSize             = 100
	DefaultSemanticMaxConcurrentProvider    = 4
	DefaultSemanticMaxRequestsPerMinute     = 60
	DefaultSemanticMaxTokensPerMinute       = 100000
	DefaultSemanticProviderConcurrent       = 2
	DefaultSemanticCredentialConcurrent     = 1
	DefaultSemanticCredentialRequestsPerMin = 30
	DefaultSemanticCredentialTokensPerMin   = 50000
	DefaultBackupInterval                   = 24 * time.Hour
	DefaultBackupRetentionCount             = 7
	DefaultBackupCompression                = "zip"
	DefaultBackupQuiesceDrainTimeout        = 2 * time.Minute
	DefaultBackupTimeout                    = 30 * time.Minute
	DefaultBackupRetryAfter                 = 5 * time.Second
	DefaultBackupStatusHistoryLimit         = 20
)

type SemanticThrottleConfig struct {
	MaxConcurrentCalls   int
	MaxRequestsPerMinute int
	MaxTokensPerMinute   int
}

type SemanticMaintenanceConfig struct {
	Enabled                    bool
	DirtyCooldown              time.Duration
	AnalyzerInterval           time.Duration
	WorkerInterval             time.Duration
	WorkerCount                int
	MaxBatchSize               int
	MaxConcurrentProviderCalls int
	MaxRequestsPerMinute       int
	MaxTokensPerMinute         int
	ProviderDefaults           SemanticThrottleConfig
	CredentialDefaults         SemanticThrottleConfig
}

type BackupConfig struct {
	Enabled                bool
	BackupDir              string
	Interval               time.Duration
	RetentionCount         int
	IncludeLogs            bool
	Compression            string
	QuiesceDrainTimeout    time.Duration
	BackupTimeout          time.Duration
	RetryAfter             time.Duration
	StatusHistoryLimit     int
	AllowReadsDuringBackup bool
}

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
	SemanticMaintenance       SemanticMaintenanceConfig
	Backup                    BackupConfig
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
		Backup: BackupConfig{
			Enabled:                parseBoolEnvDefault(os.Getenv("MYCELD_BACKUP_ENABLED"), false),
			BackupDir:              strings.TrimSpace(os.Getenv("MYCELD_BACKUP_DIR")),
			Interval:               parseDurationEnv(os.Getenv("MYCELD_BACKUP_INTERVAL"), DefaultBackupInterval),
			RetentionCount:         parseIntEnv(os.Getenv("MYCELD_BACKUP_RETENTION_COUNT"), DefaultBackupRetentionCount),
			IncludeLogs:            parseBoolEnvDefault(os.Getenv("MYCELD_BACKUP_INCLUDE_LOGS"), false),
			Compression:            valueOrDefault(os.Getenv("MYCELD_BACKUP_COMPRESSION"), DefaultBackupCompression),
			QuiesceDrainTimeout:    parseDurationEnv(os.Getenv("MYCELD_BACKUP_QUIESCE_DRAIN_TIMEOUT"), DefaultBackupQuiesceDrainTimeout),
			BackupTimeout:          parseDurationEnv(os.Getenv("MYCELD_BACKUP_TIMEOUT"), DefaultBackupTimeout),
			RetryAfter:             parseDurationEnv(os.Getenv("MYCELD_BACKUP_RETRY_AFTER"), DefaultBackupRetryAfter),
			StatusHistoryLimit:     parseIntEnv(os.Getenv("MYCELD_BACKUP_STATUS_HISTORY_LIMIT"), DefaultBackupStatusHistoryLimit),
			AllowReadsDuringBackup: parseBoolEnvDefault(os.Getenv("MYCELD_BACKUP_ALLOW_READS_DURING_BACKUP"), false),
		},
		SemanticMaintenance: SemanticMaintenanceConfig{
			Enabled:                    parseBoolEnvDefault(os.Getenv("MYCELD_SEMANTIC_MAINTENANCE_ENABLED"), true),
			DirtyCooldown:              parseDurationEnv(os.Getenv("MYCELD_SEMANTIC_DIRTY_COOLDOWN"), DefaultSemanticDirtyCooldown),
			AnalyzerInterval:           parseDurationEnv(os.Getenv("MYCELD_SEMANTIC_ANALYZER_INTERVAL"), DefaultSemanticAnalyzerInterval),
			WorkerInterval:             parseDurationEnv(os.Getenv("MYCELD_SEMANTIC_WORKER_INTERVAL"), DefaultSemanticWorkerInterval),
			WorkerCount:                parseIntEnv(os.Getenv("MYCELD_SEMANTIC_WORKER_COUNT"), DefaultSemanticWorkerCount),
			MaxBatchSize:               parseIntEnv(os.Getenv("MYCELD_SEMANTIC_MAX_BATCH_SIZE"), DefaultSemanticMaxBatchSize),
			MaxConcurrentProviderCalls: parseIntEnv(os.Getenv("MYCELD_SEMANTIC_MAX_CONCURRENT_PROVIDER_CALLS"), DefaultSemanticMaxConcurrentProvider),
			MaxRequestsPerMinute:       parseIntEnv(os.Getenv("MYCELD_SEMANTIC_MAX_REQUESTS_PER_MINUTE"), DefaultSemanticMaxRequestsPerMinute),
			MaxTokensPerMinute:         parseIntEnv(os.Getenv("MYCELD_SEMANTIC_MAX_TOKENS_PER_MINUTE"), DefaultSemanticMaxTokensPerMinute),
			ProviderDefaults: SemanticThrottleConfig{
				MaxConcurrentCalls:   parseIntEnv(os.Getenv("MYCELD_SEMANTIC_PROVIDER_MAX_CONCURRENT_CALLS"), DefaultSemanticProviderConcurrent),
				MaxRequestsPerMinute: parseIntEnv(os.Getenv("MYCELD_SEMANTIC_PROVIDER_MAX_REQUESTS_PER_MINUTE"), DefaultSemanticMaxRequestsPerMinute),
				MaxTokensPerMinute:   parseIntEnv(os.Getenv("MYCELD_SEMANTIC_PROVIDER_MAX_TOKENS_PER_MINUTE"), DefaultSemanticMaxTokensPerMinute),
			},
			CredentialDefaults: SemanticThrottleConfig{
				MaxConcurrentCalls:   parseIntEnv(os.Getenv("MYCELD_SEMANTIC_CREDENTIAL_MAX_CONCURRENT_CALLS"), DefaultSemanticCredentialConcurrent),
				MaxRequestsPerMinute: parseIntEnv(os.Getenv("MYCELD_SEMANTIC_CREDENTIAL_MAX_REQUESTS_PER_MINUTE"), DefaultSemanticCredentialRequestsPerMin),
				MaxTokensPerMinute:   parseIntEnv(os.Getenv("MYCELD_SEMANTIC_CREDENTIAL_MAX_TOKENS_PER_MINUTE"), DefaultSemanticCredentialTokensPerMin),
			},
		},
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
	if err := c.SemanticMaintenance.Validate(); err != nil {
		return err
	}
	if err := c.Backup.Validate(); err != nil {
		return err
	}
	return nil
}

func (c BackupConfig) Validate() error {
	if c.Interval < 0 {
		return fmt.Errorf("MYCELD_BACKUP_INTERVAL must be positive")
	}
	if c.RetentionCount < 0 {
		return fmt.Errorf("MYCELD_BACKUP_RETENTION_COUNT must be positive")
	}
	if c.QuiesceDrainTimeout < 0 {
		return fmt.Errorf("MYCELD_BACKUP_QUIESCE_DRAIN_TIMEOUT must be positive")
	}
	if c.BackupTimeout < 0 {
		return fmt.Errorf("MYCELD_BACKUP_TIMEOUT must be positive")
	}
	if c.RetryAfter < 0 {
		return fmt.Errorf("MYCELD_BACKUP_RETRY_AFTER must be positive")
	}
	if c.StatusHistoryLimit < 0 {
		return fmt.Errorf("MYCELD_BACKUP_STATUS_HISTORY_LIMIT must be positive")
	}
	if compression := strings.ToLower(strings.TrimSpace(c.Compression)); compression != "" && compression != "zip" {
		return fmt.Errorf("MYCELD_BACKUP_COMPRESSION must be zip")
	}
	return nil
}

func (c SemanticMaintenanceConfig) Validate() error {
	if c.DirtyCooldown < 0 {
		return fmt.Errorf("MYCELD_SEMANTIC_DIRTY_COOLDOWN must be positive")
	}
	if c.AnalyzerInterval < 0 {
		return fmt.Errorf("MYCELD_SEMANTIC_ANALYZER_INTERVAL must be positive")
	}
	if c.WorkerInterval < 0 {
		return fmt.Errorf("MYCELD_SEMANTIC_WORKER_INTERVAL must be positive")
	}
	for name, value := range map[string]int{
		"MYCELD_SEMANTIC_WORKER_COUNT":                       c.WorkerCount,
		"MYCELD_SEMANTIC_MAX_BATCH_SIZE":                     c.MaxBatchSize,
		"MYCELD_SEMANTIC_MAX_CONCURRENT_PROVIDER_CALLS":      c.MaxConcurrentProviderCalls,
		"MYCELD_SEMANTIC_MAX_REQUESTS_PER_MINUTE":            c.MaxRequestsPerMinute,
		"MYCELD_SEMANTIC_MAX_TOKENS_PER_MINUTE":              c.MaxTokensPerMinute,
		"MYCELD_SEMANTIC_PROVIDER_MAX_CONCURRENT_CALLS":      c.ProviderDefaults.MaxConcurrentCalls,
		"MYCELD_SEMANTIC_PROVIDER_MAX_REQUESTS_PER_MINUTE":   c.ProviderDefaults.MaxRequestsPerMinute,
		"MYCELD_SEMANTIC_PROVIDER_MAX_TOKENS_PER_MINUTE":     c.ProviderDefaults.MaxTokensPerMinute,
		"MYCELD_SEMANTIC_CREDENTIAL_MAX_CONCURRENT_CALLS":    c.CredentialDefaults.MaxConcurrentCalls,
		"MYCELD_SEMANTIC_CREDENTIAL_MAX_REQUESTS_PER_MINUTE": c.CredentialDefaults.MaxRequestsPerMinute,
		"MYCELD_SEMANTIC_CREDENTIAL_MAX_TOKENS_PER_MINUTE":   c.CredentialDefaults.MaxTokensPerMinute,
	} {
		if value < 0 {
			return fmt.Errorf("%s must be positive", name)
		}
	}
	return nil
}

func parseBoolEnv(value string) bool {
	return parseBoolEnvDefault(value, false)
}

func parseBoolEnvDefault(value string, fallback bool) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		return fallback
	}
}

func parseDurationEnv(value string, fallback time.Duration) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return -1
	}
	return d
}

func parseIntEnv(value string, fallback int) int {
	value = strings.TrimSpace(value)
	if value == "" {
		return fallback
	}
	i, err := strconv.Atoi(value)
	if err != nil {
		return -1
	}
	return i
}

func valueOrDefault(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return strings.TrimSpace(value)
}
