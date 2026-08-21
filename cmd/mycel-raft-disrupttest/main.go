package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/clustering/disrupttest"
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
	cfg := disrupttest.ConfigFromEnv(time.Now().UTC())
	fs := flag.NewFlagSet("mycel-raft-disrupttest", flag.ContinueOnError)
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
	fs.StringVar(&cfg.AdminPasswordFile, "admin-password-file", cfg.AdminPasswordFile, "reserved for future explicit password file")
	fs.StringVar(&cfg.Profile, "profile", cfg.Profile, "pressure profile: smoke, small, medium, soak")
	fs.StringVar(&cfg.RestartNode, "restart-node", cfg.RestartNode, "pod name/ordinal to restart in later phases")
	fs.StringVar(&cfg.ArtifactsDir, "artifacts-dir", cfg.ArtifactsDir, "artifact output root")
	fs.StringVar(&cfg.ScenarioFile, "scenario", cfg.ScenarioFile, "optional JSON scenario config")
	fs.StringVar(&cfg.Workload, "workload", cfg.Workload, "workload name: nodes, edges, multi-space")
	fs.IntVar(&cfg.PartitionCount, "partition-count", cfg.PartitionCount, "raft partition count")
	fs.BoolVar(&cfg.KeepClusterOnFailure, "keep-cluster-on-failure", cfg.KeepClusterOnFailure, "preserve disposable cluster when a failure occurs")
	fs.BoolVar(&cfg.ConfirmDestructive, "confirm-destructive", cfg.ConfirmDestructive, "acknowledge cluster create/delete/restart")
	fs.BoolVar(&cfg.DryRun, "dry-run", cfg.DryRun, "print resolved config and exit without preflight or destructive actions")
	fs.BoolVar(&cfg.SetupOnly, "setup-only", cfg.SetupOnly, "create disposable cluster and deploy myceld, then tear down")
	fs.BoolVar(&cfg.NoDisruption, "no-disruption", cfg.NoDisruption, "run workload without restarting pods")
	fs.BoolVar(&cfg.PreflightCreateDelete, "preflight-create-delete", cfg.PreflightCreateDelete, "create/delete disposable cluster without deploying workloads")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if cfg.DryRun {
		return printJSON(cfg)
	}
	harness, err := disrupttest.NewHarness(cfg, nil)
	if err != nil {
		return err
	}
	summary, runErr := harness.Run(ctx)
	result := buildResultSummary(summary, runErr)
	if writeErr := writeResultSummary(result); writeErr != nil {
		fmt.Fprintf(os.Stderr, "[raft-disrupt] failed to write result summary: %v\n", writeErr)
	}
	printResultSummary(result)
	if runErr != nil {
		return reportedError{err: runErr}
	}
	return nil
}

type reportedError struct {
	err error
}

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

type resultSummary struct {
	Status                string                                `json:"status"`
	Error                 string                                `json:"error,omitempty"`
	ErrorDetailsPath      string                                `json:"errorDetailsPath,omitempty"`
	rawError              string                                `json:"-"`
	ClusterName           string                                `json:"clusterName"`
	Namespace             string                                `json:"namespace"`
	Profile               string                                `json:"profile"`
	Workload              string                                `json:"workload,omitempty"`
	RestartNodes          []string                              `json:"restartNodes,omitempty"`
	AttemptedWrites       int64                                 `json:"attemptedWrites,omitempty"`
	SuccessfulWrites      int64                                 `json:"successfulWrites,omitempty"`
	AmbiguousWrites       int64                                 `json:"ambiguousWrites,omitempty"`
	TransientFailures     int64                                 `json:"transientFailures,omitempty"`
	PermanentFailures     int64                                 `json:"permanentFailures,omitempty"`
	ReadChecks            int64                                 `json:"readChecks,omitempty"`
	ReadFailures          int64                                 `json:"readFailures,omitempty"`
	ReadTransientFailures int64                                 `json:"readTransientFailures,omitempty"`
	ReadPermanentFailures int64                                 `json:"readPermanentFailures,omitempty"`
	RecoveryDurationMS    int64                                 `json:"recoveryDurationMs,omitempty"`
	FinalCounts           map[string]disrupttest.WorkloadCounts `json:"finalCounts,omitempty"`
	ArtifactDir           string                                `json:"artifactDir,omitempty"`
	ResultSummaryPath     string                                `json:"resultSummaryPath,omitempty"`
	ScenarioSummaryPath   string                                `json:"scenarioSummaryPath,omitempty"`
	WriteEventsPath       string                                `json:"writeEventsPath,omitempty"`
	ReadEventsPath        string                                `json:"readEventsPath,omitempty"`
	SetupDir              string                                `json:"setupDir,omitempty"`
	FailureDir            string                                `json:"failureDir,omitempty"`
}

func buildResultSummary(summary disrupttest.Summary, runErr error) resultSummary {
	status := "PASS"
	if runErr != nil {
		status = "FAIL"
	}
	result := resultSummary{
		Status:      status,
		ClusterName: summary.ClusterName,
		Namespace:   summary.Namespace,
		Profile:     summary.Profile,
		ArtifactDir: summary.ArtifactDir,
	}
	if runErr != nil {
		result.rawError = runErr.Error()
		result.Error = summarizeText(result.rawError, 500)
	}
	if summary.ArtifactDir != "" {
		result.ResultSummaryPath = filepath.Join(summary.ArtifactDir, "result-summary.json")
		result.ErrorDetailsPath = filepath.Join(summary.ArtifactDir, "error.txt")
		result.SetupDir = filepath.Join(summary.ArtifactDir, "setup")
		result.FailureDir = filepath.Join(summary.ArtifactDir, "failure")
		result.ScenarioSummaryPath = filepath.Join(summary.ArtifactDir, "scenario", "scenario-summary.json")
		result.WriteEventsPath = filepath.Join(summary.ArtifactDir, "scenario", "write-events.jsonl")
		result.ReadEventsPath = filepath.Join(summary.ArtifactDir, "scenario", "read-events.jsonl")
	}
	if summary.Scenario != nil {
		s := summary.Scenario
		result.Workload = s.Workload
		result.RestartNodes = append([]string(nil), s.RestartNodes...)
		result.AttemptedWrites = s.AttemptedWrites
		result.SuccessfulWrites = s.SuccessfulWrites
		result.AmbiguousWrites = s.AmbiguousWrites
		result.TransientFailures = s.TransientFailures
		result.PermanentFailures = s.PermanentFailures
		result.ReadChecks = s.ReadChecks
		result.ReadFailures = s.ReadFailures
		result.ReadTransientFailures = s.ReadTransientFailures
		result.ReadPermanentFailures = s.ReadPermanentFailures
		result.RecoveryDurationMS = s.RecoveryDurationMS
		result.FinalCounts = s.FinalCounts
	}
	return result
}

func writeResultSummary(result resultSummary) error {
	if result.ArtifactDir == "" {
		return nil
	}
	if err := os.MkdirAll(result.ArtifactDir, 0o755); err != nil {
		return err
	}
	if result.rawError != "" {
		if err := os.WriteFile(filepath.Join(result.ArtifactDir, "error.txt"), []byte(result.rawError+"\n"), 0o644); err != nil {
			return err
		}
	}
	data, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(result.ArtifactDir, "result-summary.json"), data, 0o644)
}

func printResultSummary(result resultSummary) {
	fmt.Printf("\nRaft disruption test: %s\n", result.Status)
	if result.Error != "" {
		fmt.Printf("Error: %s\n", result.Error)
	}
	fmt.Printf("Cluster: %s\n", result.ClusterName)
	fmt.Printf("Namespace: %s\n", result.Namespace)
	fmt.Printf("Profile: %s\n", result.Profile)
	if result.Workload != "" {
		fmt.Printf("Workload: %s\n", result.Workload)
	}
	if len(result.RestartNodes) > 0 {
		fmt.Printf("Restarted pods: %s\n", strings.Join(result.RestartNodes, ", "))
	}
	if result.AttemptedWrites > 0 || result.SuccessfulWrites > 0 || result.AmbiguousWrites > 0 || result.TransientFailures > 0 || result.PermanentFailures > 0 {
		fmt.Printf("Writes: attempted=%d successful=%d ambiguous=%d transientFailures=%d permanentFailures=%d\n", result.AttemptedWrites, result.SuccessfulWrites, result.AmbiguousWrites, result.TransientFailures, result.PermanentFailures)
	}
	if result.ReadChecks > 0 || result.ReadFailures > 0 {
		fmt.Printf("Committed read checks: checks=%d failures=%d transientFailures=%d permanentFailures=%d\n", result.ReadChecks, result.ReadFailures, result.ReadTransientFailures, result.ReadPermanentFailures)
	}
	if result.RecoveryDurationMS > 0 {
		fmt.Printf("Recovery duration: %s\n", (time.Duration(result.RecoveryDurationMS) * time.Millisecond).Round(time.Millisecond))
	}
	if len(result.FinalCounts) > 0 {
		fmt.Println("Final counts:")
		for _, name := range sortedCountNames(result.FinalCounts) {
			count := result.FinalCounts[name]
			fmt.Printf("  %s: nodes=%d edges=%d\n", name, count.Nodes, count.Edges)
		}
	}
	if result.ArtifactDir != "" {
		fmt.Printf("Artifacts: %s\n", result.ArtifactDir)
		fmt.Printf("Result summary: %s\n", result.ResultSummaryPath)
		if result.ScenarioSummaryPath != "" {
			fmt.Printf("Detailed scenario summary: %s\n", result.ScenarioSummaryPath)
			fmt.Printf("Write events: %s\n", result.WriteEventsPath)
			fmt.Printf("Read events: %s\n", result.ReadEventsPath)
		}
		if result.Status == "FAIL" {
			if result.ErrorDetailsPath != "" {
				fmt.Printf("Full error: %s\n", result.ErrorDetailsPath)
			}
			fmt.Printf("Failure artifacts: %s\n", result.FailureDir)
		}
	}
}

func summarizeText(text string, limit int) string {
	text = strings.Join(strings.Fields(text), " ")
	if limit > 0 && len(text) > limit {
		return text[:limit] + "..."
	}
	return text
}

func sortedCountNames(counts map[string]disrupttest.WorkloadCounts) []string {
	names := make([]string, 0, len(counts))
	for name := range counts {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}
