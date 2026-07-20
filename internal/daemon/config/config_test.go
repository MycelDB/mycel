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
	if cfg.Backup.Enabled || cfg.Backup.Interval != DefaultBackupInterval || cfg.Backup.RetentionCount != DefaultBackupRetentionCount || cfg.Backup.Compression != DefaultBackupCompression || cfg.Backup.QuiesceDrainTimeout != DefaultBackupQuiesceDrainTimeout || cfg.Backup.BackupTimeout != DefaultBackupTimeout || cfg.Backup.RetryAfter != DefaultBackupRetryAfter || cfg.Backup.StatusHistoryLimit != DefaultBackupStatusHistoryLimit || cfg.Backup.AllowReadsDuringBackup {
		t.Fatalf("unexpected backup defaults: %+v", cfg.Backup)
	}
	if cfg.AccessTokenTTL != DefaultAccessTokenTTL {
		t.Fatalf("unexpected access token TTL default: %s", cfg.AccessTokenTTL)
	}
	if cfg.Cluster.Engine != DefaultClusterEngine || cfg.Cluster.RaftNodeCount != DefaultClusterRaftNodeCount || cfg.Cluster.RaftPartitionCount != DefaultClusterRaftPartitionCount || cfg.Cluster.RaftReplicaFactor != DefaultClusterRaftReplicaFactor || cfg.Cluster.RaftLocalNodeID != DefaultClusterRaftLocalNodeID {
		t.Fatalf("unexpected cluster raft defaults: %+v", cfg.Cluster)
	}
}

func TestLoadFromEnvRaftClusterOverrides(t *testing.T) {
	t.Setenv("MYCELD_DATA_DIR", t.TempDir())
	t.Setenv("MYCELD_CLUSTER_ENGINE", "raft")
	t.Setenv("MYCELD_CLUSTER_RAFT_NODE_COUNT", "5")
	t.Setenv("MYCELD_CLUSTER_RAFT_PARTITION_COUNT", "128")
	t.Setenv("MYCELD_CLUSTER_RAFT_REPLICA_FACTOR", "3")
	t.Setenv("MYCELD_CLUSTER_RAFT_LOCAL_NODE_ID", "4")
	t.Setenv("MYCELD_CLUSTER_RAFT_NODE_ADDRS", "127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103,127.0.0.1:9104,127.0.0.1:9105")

	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}
	if cfg.Cluster.Engine != "raft" || cfg.Cluster.RaftNodeCount != 5 || cfg.Cluster.RaftPartitionCount != 128 || cfg.Cluster.RaftReplicaFactor != 3 || cfg.Cluster.RaftLocalNodeID != 4 {
		t.Fatalf("unexpected raft cluster overrides: %+v", cfg.Cluster)
	}
	if len(cfg.Cluster.RaftNodeAddrs) != 5 || cfg.Cluster.RaftNodeAddrs[3] != "127.0.0.1:9104" {
		t.Fatalf("unexpected raft node addrs: %+v", cfg.Cluster.RaftNodeAddrs)
	}
}

func TestLoadFromEnvRaftNodeAddrsValidation(t *testing.T) {
	t.Setenv("MYCELD_DATA_DIR", t.TempDir())
	t.Setenv("MYCELD_CLUSTER_ENGINE", "raft")
	t.Setenv("MYCELD_CLUSTER_RAFT_NODE_COUNT", "3")
	t.Setenv("MYCELD_CLUSTER_RAFT_NODE_ADDRS", "127.0.0.1:9101,127.0.0.1:9102")
	if _, err := LoadFromEnv(); err == nil {
		t.Fatal("expected raft node address count mismatch to fail")
	}
}

func TestLoadFromEnvAccessTokenTTLOverride(t *testing.T) {
	t.Setenv("MYCELD_DATA_DIR", t.TempDir())
	t.Setenv("MYCELD_ACCESS_TOKEN_TTL", "750ms")
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}
	if cfg.AccessTokenTTL != 750*time.Millisecond {
		t.Fatalf("unexpected access token TTL: %s", cfg.AccessTokenTTL)
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

func TestLoadFromEnvBackupOverrides(t *testing.T) {
	t.Setenv("MYCELD_DATA_DIR", t.TempDir())
	t.Setenv("MYCELD_BACKUP_ENABLED", "true")
	t.Setenv("MYCELD_BACKUP_DIR", "/tmp/mycel-backups")
	t.Setenv("MYCELD_BACKUP_INTERVAL", "2h")
	t.Setenv("MYCELD_BACKUP_RETENTION_COUNT", "9")
	t.Setenv("MYCELD_BACKUP_INCLUDE_LOGS", "true")
	t.Setenv("MYCELD_BACKUP_COMPRESSION", "zip")
	t.Setenv("MYCELD_BACKUP_QUIESCE_DRAIN_TIMEOUT", "3m")
	t.Setenv("MYCELD_BACKUP_TIMEOUT", "45m")
	t.Setenv("MYCELD_BACKUP_RETRY_AFTER", "7s")
	t.Setenv("MYCELD_BACKUP_STATUS_HISTORY_LIMIT", "12")
	t.Setenv("MYCELD_BACKUP_ALLOW_READS_DURING_BACKUP", "true")
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv() error = %v", err)
	}
	backup := cfg.Backup
	if !backup.Enabled || backup.BackupDir != "/tmp/mycel-backups" || backup.Interval != 2*time.Hour || backup.RetentionCount != 9 || !backup.IncludeLogs || backup.Compression != "zip" || backup.QuiesceDrainTimeout != 3*time.Minute || backup.BackupTimeout != 45*time.Minute || backup.RetryAfter != 7*time.Second || backup.StatusHistoryLimit != 12 || !backup.AllowReadsDuringBackup {
		t.Fatalf("unexpected backup overrides: %+v", backup)
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
		{DataDir: "/tmp/mycel", Mode: "standalone", LogLevel: "info", LogFormat: "text", GRPCAddr: DefaultGRPCAddr, Backup: BackupConfig{Interval: -1}},
		{DataDir: "/tmp/mycel", Mode: "standalone", LogLevel: "info", LogFormat: "text", GRPCAddr: DefaultGRPCAddr, Backup: BackupConfig{RetentionCount: -1}},
		{DataDir: "/tmp/mycel", Mode: "standalone", LogLevel: "info", LogFormat: "text", GRPCAddr: DefaultGRPCAddr, Backup: BackupConfig{Compression: "tar.gz"}},
	}
	for _, tc := range cases {
		if err := tc.Validate(); err == nil {
			t.Fatalf("Validate(%+v) expected error", tc)
		}
	}
}
