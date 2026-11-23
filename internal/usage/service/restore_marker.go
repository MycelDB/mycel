package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/fsperm"
)

const (
	CounterStateRelativePath       = "meta/usage/counters.json"
	RestoredUsageStateSchema       = "mycel.restored_usage_accounting"
	RestoredUsageStateVersion      = 1
	RestoredUsageStateRelativePath = "meta/usage/restored-usage-accounting.json"
)

type RestoredUsageStateOptions struct {
	BackupSetID        string
	RestoreOperationID string
	RestorePlanID      string
	ClusterID          string
	NodeID             string
	PodName            string
	Ordinal            int
	RestoredAt         time.Time
	Evidence           map[string]string
}

type RestoredUsageStateDocument struct {
	Schema                     string            `json:"schema"`
	Version                    int               `json:"version"`
	BackupSetID                string            `json:"backup_set_id,omitempty"`
	RestoreOperationID         string            `json:"restore_operation_id,omitempty"`
	RestorePlanID              string            `json:"restore_plan_id,omitempty"`
	ClusterID                  string            `json:"cluster_id,omitempty"`
	NodeID                     string            `json:"node_id,omitempty"`
	PodName                    string            `json:"pod_name,omitempty"`
	Ordinal                    int               `json:"ordinal"`
	RestoredAt                 time.Time         `json:"restored_at"`
	CounterStateIncluded       bool              `json:"counter_state_included"`
	CounterStateRelativePath   string            `json:"counter_state_relative_path"`
	CounterStateChecksumSHA256 string            `json:"counter_state_checksum_sha256,omitempty"`
	DesiredAccountingState     AccountingState   `json:"desired_accounting_state"`
	Reason                     string            `json:"reason,omitempty"`
	MetricVersions             map[string]uint32 `json:"metric_versions,omitempty"`
	Evidence                   map[string]string `json:"evidence,omitempty"`
}

func UsageCounterStatePath(dataDir string) string {
	return filepath.Join(dataDir, CounterStateRelativePath)
}

func RestoredUsageStatePath(dataDir string) string {
	return filepath.Join(dataDir, RestoredUsageStateRelativePath)
}

func UsageCounterStateChecksum(ctx context.Context, dataDir string) (string, bool, error) {
	path := UsageCounterStatePath(dataDir)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return "", false, nil
		}
		return "", false, err
	}
	defer file.Close()
	h := sha256.New()
	buf := make([]byte, 64*1024)
	for {
		if err := ctx.Err(); err != nil {
			return "", false, err
		}
		n, readErr := file.Read(buf)
		if n > 0 {
			if _, err := h.Write(buf[:n]); err != nil {
				return "", false, err
			}
		}
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			return "", false, readErr
		}
	}
	return hex.EncodeToString(h.Sum(nil)), true, nil
}

func BuildRestoredUsageState(ctx context.Context, dataDir string, opts RestoredUsageStateOptions) (RestoredUsageStateDocument, error) {
	restoredAt := opts.RestoredAt.UTC()
	if restoredAt.IsZero() {
		restoredAt = time.Now().UTC()
	}
	checksum, present, err := UsageCounterStateChecksum(ctx, dataDir)
	if err != nil {
		return RestoredUsageStateDocument{}, err
	}
	metricVersions := map[string]uint32{}
	for _, def := range DefaultRegistry().Definitions() {
		metricVersions[def.Name] = def.Version
	}
	evidence := copyRestoreEvidence(opts.Evidence)
	if evidence == nil {
		evidence = map[string]string{}
	}
	evidence["counter_state_present"] = fmt.Sprintf("%t", present)
	if checksum != "" {
		evidence["counter_state_checksum_sha256"] = checksum
	}
	desired := AccountingStateReconciling
	reason := "restored usage counters require reconciliation before quota-safe readiness"
	if !present {
		desired = AccountingStateRebuilding
		reason = "restored usage counter state is missing and must be rebuilt"
	}
	return RestoredUsageStateDocument{Schema: RestoredUsageStateSchema, Version: RestoredUsageStateVersion, BackupSetID: strings.TrimSpace(opts.BackupSetID), RestoreOperationID: strings.TrimSpace(opts.RestoreOperationID), RestorePlanID: strings.TrimSpace(opts.RestorePlanID), ClusterID: strings.TrimSpace(opts.ClusterID), NodeID: strings.TrimSpace(opts.NodeID), PodName: strings.TrimSpace(opts.PodName), Ordinal: opts.Ordinal, RestoredAt: restoredAt, CounterStateIncluded: present, CounterStateRelativePath: CounterStateRelativePath, CounterStateChecksumSHA256: checksum, DesiredAccountingState: desired, Reason: reason, MetricVersions: metricVersions, Evidence: evidence}, nil
}

func WriteRestoredUsageState(dataDir string, doc RestoredUsageStateDocument) error {
	if strings.TrimSpace(dataDir) == "" {
		return fmt.Errorf("data dir is required")
	}
	path := RestoredUsageStatePath(dataDir)
	if doc.Schema == "" {
		doc.Schema = RestoredUsageStateSchema
	}
	if doc.Version == 0 {
		doc.Version = RestoredUsageStateVersion
	}
	if doc.RestoredAt.IsZero() {
		doc.RestoredAt = time.Now().UTC()
	}
	if doc.CounterStateRelativePath == "" {
		doc.CounterStateRelativePath = CounterStateRelativePath
	}
	if doc.DesiredAccountingState == "" {
		if doc.CounterStateIncluded {
			doc.DesiredAccountingState = AccountingStateReconciling
		} else {
			doc.DesiredAccountingState = AccountingStateRebuilding
		}
	}
	raw, err := json.MarshalIndent(doc, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), fsperm.PrivateDir); err != nil {
		return err
	}
	tmp := path + ".tmp-" + fmt.Sprintf("%d", time.Now().UnixNano())
	if err := os.WriteFile(tmp, append(raw, '\n'), fsperm.PrivateFile); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		_ = os.Remove(tmp)
		return err
	}
	return nil
}

func ReadRestoredUsageState(dataDir string) (RestoredUsageStateDocument, bool, error) {
	path := RestoredUsageStatePath(dataDir)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return RestoredUsageStateDocument{}, false, nil
		}
		return RestoredUsageStateDocument{}, false, err
	}
	var doc RestoredUsageStateDocument
	if err := json.Unmarshal(raw, &doc); err != nil {
		return RestoredUsageStateDocument{}, true, err
	}
	if doc.Schema != RestoredUsageStateSchema {
		return RestoredUsageStateDocument{}, true, fmt.Errorf("unexpected restored usage accounting schema %q", doc.Schema)
	}
	if doc.Version != RestoredUsageStateVersion {
		return RestoredUsageStateDocument{}, true, fmt.Errorf("unsupported restored usage accounting version %d", doc.Version)
	}
	return doc, true, nil
}

func MetricVersionsEvidence() string {
	defs := DefaultRegistry().Definitions()
	sort.Slice(defs, func(i, j int) bool { return defs[i].Name < defs[j].Name })
	parts := make([]string, 0, len(defs))
	for _, def := range defs {
		parts = append(parts, fmt.Sprintf("%s@%d", def.Name, def.Version))
	}
	return strings.Join(parts, ",")
}

func copyRestoreEvidence(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
