package systembackuptest

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/clustering/disrupttest"
)

const (
	DefaultDriver       = "k3s"
	DefaultProvisioner  = "k3d"
	DefaultProfile      = "backup-smoke"
	DefaultNamespace    = "mycel-system-backup-restore"
	DefaultSelector     = "app=myceld"
	DefaultService      = "myceld"
	DefaultStatefulSet  = "myceld"
	DefaultImage        = "myceldb/mycel:system-backup-restore-local"
	DefaultAdminUser    = "admin"
	DefaultPartitionCnt = 16
	DefaultNodeCount    = 3
	DefaultBackupDir    = "/tmp/mycel-system-backups"
)

type Config struct {
	Driver               string
	Provisioner          string
	ClusterName          string
	Namespace            string
	Selector             string
	Service              string
	StatefulSet          string
	Image                string
	AdminUsername        string
	Profile              string
	Workload             string
	ArtifactsDir         string
	BackupDir            string
	ArchiveFormat        string
	KeepClusterOnFailure bool
	ConfirmDestructive   bool
	DryRun               bool
	SetupOnly            bool
	NoBackup             bool
	PartitionCount       int
	NodeCount            int
	Now                  time.Time
}

type Profile struct {
	Name   string
	Writes int64
}

func DefaultConfig(now time.Time) Config {
	if now.IsZero() {
		now = time.Now().UTC()
	}
	suffix := now.Format("20060102-150405")
	return Config{
		Driver:         DefaultDriver,
		Provisioner:    DefaultProvisioner,
		ClusterName:    "mycel-sbr-" + suffix,
		Namespace:      DefaultNamespace,
		Selector:       DefaultSelector,
		Service:        DefaultService,
		StatefulSet:    DefaultStatefulSet,
		Image:          DefaultImage,
		AdminUsername:  DefaultAdminUser,
		Profile:        DefaultProfile,
		Workload:       disrupttest.WorkloadEdges,
		ArtifactsDir:   "artifacts/system-backup-restore",
		BackupDir:      DefaultBackupDir,
		ArchiveFormat:  "tar",
		PartitionCount: DefaultPartitionCnt,
		NodeCount:      DefaultNodeCount,
		Now:            now,
	}
}

func ConfigFromEnv(now time.Time) Config {
	cfg := DefaultConfig(now)
	setString(&cfg.Driver, "MYCEL_SYSTEM_BACKUP_DRIVER")
	setString(&cfg.Provisioner, "MYCEL_SYSTEM_BACKUP_PROVISIONER")
	setString(&cfg.ClusterName, "MYCEL_SYSTEM_BACKUP_CLUSTER_NAME")
	setString(&cfg.Namespace, "MYCEL_SYSTEM_BACKUP_NAMESPACE")
	setString(&cfg.Selector, "MYCEL_SYSTEM_BACKUP_SELECTOR")
	setString(&cfg.Service, "MYCEL_SYSTEM_BACKUP_SERVICE")
	setString(&cfg.StatefulSet, "MYCEL_SYSTEM_BACKUP_STATEFULSET")
	setString(&cfg.Image, "MYCEL_SYSTEM_BACKUP_IMAGE")
	setString(&cfg.AdminUsername, "MYCEL_ADMIN_USERNAME")
	setString(&cfg.Profile, "MYCEL_SYSTEM_BACKUP_PROFILE")
	setString(&cfg.Workload, "MYCEL_SYSTEM_BACKUP_WORKLOAD")
	setString(&cfg.ArtifactsDir, "MYCEL_SYSTEM_BACKUP_ARTIFACTS_DIR")
	setString(&cfg.BackupDir, "MYCEL_SYSTEM_BACKUP_DIR")
	setString(&cfg.ArchiveFormat, "MYCEL_SYSTEM_BACKUP_ARCHIVE_FORMAT")
	setInt(&cfg.PartitionCount, "MYCEL_SYSTEM_BACKUP_PARTITION_COUNT")
	setInt(&cfg.NodeCount, "MYCEL_SYSTEM_BACKUP_NODE_COUNT")
	cfg.KeepClusterOnFailure = envBool("MYCEL_KEEP_CLUSTER_ON_FAILURE", cfg.KeepClusterOnFailure)
	cfg.ConfirmDestructive = envBool("MYCEL_CONFIRM_DESTRUCTIVE", cfg.ConfirmDestructive)
	return cfg
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Driver) != "k3s" {
		return fmt.Errorf("unsupported driver %q", c.Driver)
	}
	if strings.TrimSpace(c.Provisioner) != "k3d" {
		return fmt.Errorf("unsupported provisioner %q", c.Provisioner)
	}
	if len(strings.TrimSpace(c.ClusterName)) > 32 {
		return fmt.Errorf("cluster name must be <= 32 characters for k3d")
	}
	for name, value := range map[string]string{"cluster name": c.ClusterName, "namespace": c.Namespace, "selector": c.Selector, "service": c.Service, "statefulset": c.StatefulSet, "image": c.Image, "admin username": c.AdminUsername, "profile": c.Profile, "artifacts dir": c.ArtifactsDir, "backup dir": c.BackupDir, "archive format": c.ArchiveFormat, "workload": c.Workload} {
		if strings.TrimSpace(value) == "" {
			return fmt.Errorf("%s is required", name)
		}
	}
	if c.NodeCount != 3 {
		return fmt.Errorf("node count must be 3 for the initial system backup/restore harness")
	}
	if c.PartitionCount <= 0 {
		return fmt.Errorf("partition count must be positive")
	}
	if _, err := ResolveProfile(c.Profile); err != nil {
		return err
	}
	if !disrupttest.IsSupportedWorkload(strings.TrimSpace(c.Workload)) {
		return fmt.Errorf("unsupported workload %q", c.Workload)
	}
	if _, err := archiveExtension(c.ArchiveFormat); err != nil {
		return err
	}
	if !c.ConfirmDestructive && !c.DryRun {
		return fmt.Errorf("--confirm-destructive is required for cluster create/delete/volume restore")
	}
	return nil
}

func ResolveProfile(name string) (Profile, error) {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "", "backup-smoke":
		return Profile{Name: "backup-smoke", Writes: 40}, nil
	case "backup-small":
		return Profile{Name: "backup-small", Writes: 200}, nil
	case "backup-multi-space":
		return Profile{Name: "backup-multi-space", Writes: 90}, nil
	default:
		return Profile{}, fmt.Errorf("unsupported backup/restore profile %q", name)
	}
}

func archiveExtension(format string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "tar":
		return ".tar", nil
	default:
		return "", fmt.Errorf("unsupported archive format %q: workload-driven restore currently supports tar", format)
	}
}

func setString(target *string, env string) {
	if value := strings.TrimSpace(os.Getenv(env)); value != "" {
		*target = value
	}
}

func setInt(target *int, env string) {
	value := strings.TrimSpace(os.Getenv(env))
	if value == "" {
		return
	}
	parsed, err := strconv.Atoi(value)
	if err == nil {
		*target = parsed
	}
}

func envBool(env string, fallback bool) bool {
	value := strings.ToLower(strings.TrimSpace(os.Getenv(env)))
	if value == "" {
		return fallback
	}
	return value == "1" || value == "true" || value == "yes"
}
