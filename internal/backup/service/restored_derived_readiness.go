package service

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	clusterbackup "github.com/myceldb/mycel/internal/backup/cluster"
	"github.com/myceldb/mycel/internal/clustering"
)

type RestoredDerivedReadinessProvider interface {
	RestoredDerivedReadiness(context.Context) ([]clustering.DerivedSubsystemReadiness, error)
}

func (m *Module) RestoredDerivedReadiness(ctx context.Context) ([]clustering.DerivedSubsystemReadiness, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	doc, ok, err := clusterbackup.ReadRestoredDerivedReadiness(m.dataDir)
	if err != nil {
		return []clustering.DerivedSubsystemReadiness{{Subsystem: "RESTORED_DERIVED_READINESS", State: "unknown", Ready: false, Reason: fmt.Sprintf("read restored derived readiness marker: %v", err)}}, nil
	}
	if !ok {
		return nil, nil
	}
	out := make([]clustering.DerivedSubsystemReadiness, 0, len(doc.Subsystems))
	for _, row := range doc.Subsystems {
		out = append(out, clustering.DerivedSubsystemReadiness{Subsystem: strings.TrimSpace(row.Subsystem), State: strings.TrimSpace(row.State), Ready: row.Ready, RebuildRequired: row.RebuildRequired, Reason: strings.TrimSpace(row.Reason), ArtifactID: strings.TrimSpace(row.ArtifactID), PolicyMode: strings.TrimSpace(row.PolicyMode), RestorePolicy: strings.TrimSpace(row.RestorePolicy), ValidationState: strings.TrimSpace(row.ValidationState), SourceFrontierKind: strings.TrimSpace(row.SourceFrontierKind), SourceFrontierValue: strings.TrimSpace(row.SourceFrontierValue), Evidence: copyStringMapLocal(row.Evidence), UpdatedAt: formatTimeRFC3339(row.UpdatedAt)})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Subsystem == out[j].Subsystem {
			return out[i].ArtifactID < out[j].ArtifactID
		}
		return out[i].Subsystem < out[j].Subsystem
	})
	return out, nil
}

func formatTimeRFC3339(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339Nano)
}
