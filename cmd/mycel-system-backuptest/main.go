package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/clustering/disrupttest"
	"github.com/myceldb/mycel/internal/clustering/systembackuptest"
)

func main() {
	if err := run(context.Background(), os.Args[1:]); err != nil {
		var reported reportedError
		if !errors.As(err, &reported) {
			fmt.Fprintln(os.Stderr, err)
		}
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string) error {
	cfg := systembackuptest.ConfigFromEnv(time.Now().UTC())
	fs := flag.NewFlagSet("mycel-system-backuptest", flag.ContinueOnError)
	fs.SetOutput(os.Stderr)
	fs.StringVar(&cfg.Driver, "driver", cfg.Driver, "cluster driver: k3s")
	fs.StringVar(&cfg.Provisioner, "provisioner", cfg.Provisioner, "cluster provisioner: k3d")
	fs.StringVar(&cfg.ClusterName, "cluster-name", cfg.ClusterName, "disposable cluster name")
	fs.StringVar(&cfg.Namespace, "namespace", cfg.Namespace, "Kubernetes namespace inside disposable cluster")
	fs.StringVar(&cfg.Selector, "selector", cfg.Selector, "pod label selector")
	fs.StringVar(&cfg.Service, "service", cfg.Service, "client service name")
	fs.StringVar(&cfg.StatefulSet, "statefulset", cfg.StatefulSet, "myceld StatefulSet name")
	fs.StringVar(&cfg.Image, "image", cfg.Image, "myceld image to deploy")
	fs.StringVar(&cfg.AdminUsername, "admin-username", cfg.AdminUsername, "bootstrap admin username")
	fs.StringVar(&cfg.Profile, "profile", cfg.Profile, "backup/restore profile: backup-smoke, backup-small, backup-multi-space")
	fs.StringVar(&cfg.Workload, "workload", cfg.Workload, "workload name: nodes, edges, multi-space")
	fs.StringVar(&cfg.ArtifactsDir, "artifacts-dir", cfg.ArtifactsDir, "artifact output root")
	fs.StringVar(&cfg.BackupDir, "backup-dir", cfg.BackupDir, "backup directory inside each pod")
	fs.StringVar(&cfg.ArchiveFormat, "archive-format", cfg.ArchiveFormat, "backup archive format: tar")
	fs.IntVar(&cfg.PartitionCount, "partition-count", cfg.PartitionCount, "raft partition count")
	fs.BoolVar(&cfg.KeepClusterOnFailure, "keep-cluster-on-failure", cfg.KeepClusterOnFailure, "preserve disposable cluster when a failure occurs")
	fs.BoolVar(&cfg.ConfirmDestructive, "confirm-destructive", cfg.ConfirmDestructive, "acknowledge cluster create/delete/PVC restore")
	fs.BoolVar(&cfg.DryRun, "dry-run", cfg.DryRun, "print resolved config and exit without destructive actions")
	fs.BoolVar(&cfg.SetupOnly, "setup-only", cfg.SetupOnly, "create disposable cluster and deploy myceld, then tear down")
	fs.BoolVar(&cfg.NoBackup, "no-backup", cfg.NoBackup, "write workload and verify counts without backup/restore")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if cfg.DryRun {
		return printJSON(cfg)
	}
	harness, err := systembackuptest.NewHarness(cfg, nil)
	if err != nil {
		return err
	}
	summary, runErr := harness.Run(ctx)
	printSummary(summary)
	if runErr != nil {
		return reportedError{err: runErr}
	}
	return nil
}

type reportedError struct{ err error }

func (e reportedError) Error() string { return e.err.Error() }
func (e reportedError) Unwrap() error { return e.err }

func printJSON(value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	fmt.Println(string(data))
	return nil
}

func printSummary(s systembackuptest.Summary) {
	status := s.Status
	if status == "" {
		status = "FAIL"
	}
	fmt.Printf("\nSystem backup/restore test: %s\n", status)
	if s.Error != "" {
		fmt.Printf("Error: %s\n", s.Error)
	}
	fmt.Printf("Cluster: %s\n", s.ClusterName)
	fmt.Printf("Namespace: %s\n", s.Namespace)
	fmt.Printf("Profile: %s\n", s.Profile)
	fmt.Printf("Workload: %s\n", s.Workload)
	if s.AttemptedWrites > 0 || s.SuccessfulWrites > 0 || s.AmbiguousWrites > 0 {
		fmt.Printf("Writes: attempted=%d successful=%d ambiguous=%d\n", s.AttemptedWrites, s.SuccessfulWrites, s.AmbiguousWrites)
	}
	if s.BackupSetID != "" {
		fmt.Printf("Backup set: %s\n", s.BackupSetID)
	}
	if s.PVCsDeleted > 0 || s.PVCsRestored > 0 {
		fmt.Printf("PVC replacement: verified oldPVCs=%d newPVCs=%d\n", s.PVCsDeleted, s.PVCsRestored)
	}
	if s.RecoveryDurationMS > 0 {
		fmt.Printf("Recovery duration: %s\n", (time.Duration(s.RecoveryDurationMS) * time.Millisecond).Round(time.Millisecond))
	}
	if len(s.FinalCounts) > 0 {
		fmt.Println("Final restored counts:")
		for _, name := range sortedCountNames(s.FinalCounts) {
			count := s.FinalCounts[name]
			fmt.Printf("  %s: nodes=%d edges=%d\n", name, count.Nodes, count.Edges)
		}
	}
	if s.ArtifactDir != "" {
		fmt.Printf("Artifacts: %s\n", s.ArtifactDir)
		fmt.Printf("Result summary: %s/result-summary.json\n", strings.TrimRight(s.ArtifactDir, "/"))
		if status == "FAIL" {
			fmt.Printf("Full error: %s/error.txt\n", strings.TrimRight(s.ArtifactDir, "/"))
			fmt.Printf("Failure artifacts: %s/failure\n", strings.TrimRight(s.ArtifactDir, "/"))
		}
	}
}

func sortedCountNames(counts map[string]disrupttest.WorkloadCounts) []string {
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
