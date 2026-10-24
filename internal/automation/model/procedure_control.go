package model

import (
	"time"

	graph "github.com/myceldb/mycel/internal/graph/model"
)

// ProcedureControlBarrier reports the committed control epoch and every
// execution partition that acknowledged its start/publication fence.
type ProcedureControlBarrier struct {
	SpaceID                string         `json:"space_id,omitempty"`
	DomainID               graph.DomainID `json:"domain_id"`
	ProcedureID            string         `json:"procedure_id"`
	ProcedureVersion       int            `json:"procedure_version"`
	TargetStatus           string         `json:"target_status"`
	ControlEpoch           uint64         `json:"control_epoch"`
	ControllingPrincipalID string         `json:"controlling_principal_id,omitempty"`
	AffectedPartitions     []uint32       `json:"affected_partitions,omitempty"`
	PreparedPartitions     []uint32       `json:"prepared_partitions,omitempty"`
	AcknowledgedPartitions []uint32       `json:"acknowledged_partitions,omitempty"`
	Completed              bool           `json:"completed"`
	CompletionRevision     uint64         `json:"completion_revision,omitempty"`
	CommittedAt            time.Time      `json:"committed_at"`
	CompletedAt            time.Time      `json:"completed_at,omitempty"`
}

// ProcedureControlFence is committed to an execution partition before a
// control operation may report completion.
type ProcedureControlFence struct {
	PartitionID      uint32         `json:"partition_id"`
	SpaceID          string         `json:"space_id"`
	DomainID         graph.DomainID `json:"domain_id"`
	ProcedureID      string         `json:"procedure_id"`
	ProcedureVersion int            `json:"procedure_version"`
	TargetStatus     string         `json:"target_status"`
	ControlEpoch     uint64         `json:"control_epoch"`
	ControlledBy     string         `json:"controlled_by,omitempty"`
	CommittedAt      time.Time      `json:"committed_at"`
}
