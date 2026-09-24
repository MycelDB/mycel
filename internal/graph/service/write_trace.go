package service

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/myceldb/mycel/internal/clustering/consensus"
	graphchange "github.com/myceldb/mycel/internal/graph/change"
	graphstorage "github.com/myceldb/mycel/internal/graph/storage"
	"github.com/myceldb/mycel/internal/graph/writetrace"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
)

type graphStageTiming struct {
	Total          time.Duration
	RouteCheck     time.Duration
	NodeLookup     time.Duration
	SchemaValidate time.Duration
	Stage          time.Duration
	UpdateMask     []string
}

type graphCommitTiming struct {
	Total                 time.Duration
	EnterWrite            time.Duration
	OverlaySnapshot       time.Duration
	StoreOpen             time.Duration
	OverlayChanges        time.Duration
	GraphEvent            time.Duration
	RecordBuild           time.Duration
	BlobReferenceValidate time.Duration
	WALMarshal            time.Duration
	WALAppend             time.Duration
	WALSync               time.Duration
	WALApply              time.Duration
	WALMarkApplied        time.Duration
	RaftBuild             time.Duration
	RaftPropose           time.Duration
	RaftProposal          consensus.ProposalTiming
	DirectStorage         time.Duration
	ChangeSink            time.Duration
	StorageCommit         graphstorage.CommitTiming
	SinkTimings           []graphSinkTiming
	StorageMode           string
}

type graphSinkTiming struct {
	Name     string
	Duration time.Duration
	Error    string
}

type graphSinkTimingRecorder struct{ timings []graphSinkTiming }

func (r *graphSinkTimingRecorder) RecordGraphChangeSinkTiming(name string, duration time.Duration, err error) {
	entry := graphSinkTiming{Name: strings.TrimSpace(name), Duration: duration}
	if entry.Name == "" {
		entry.Name = "sink"
	}
	if err != nil {
		entry.Error = err.Error()
	}
	r.timings = append(r.timings, entry)
}

func (m *Module) logGraphStageTiming(operation string, tx daemonsession.GraphTransaction, counts map[string]int, timing graphStageTiming) {
	if m == nil || m.logger == nil || !m.writeTrace.ShouldLog(timing.Total) {
		return
	}
	attrs := []any{
		"event", "graph_write_stage_timing",
		"operation", operation,
		"space_id", tx.SpaceID,
		"domain_id", tx.DomainID,
		"transaction_id", tx.ID,
		"total_ms", writetrace.MS(timing.Total),
		"route_check_ms", writetrace.MS(timing.RouteCheck),
		"node_lookup_ms", writetrace.MS(timing.NodeLookup),
		"schema_validate_ms", writetrace.MS(timing.SchemaValidate),
		"stage_ms", writetrace.MS(timing.Stage),
	}
	for k, v := range counts {
		attrs = append(attrs, k, v)
	}
	if len(timing.UpdateMask) > 0 {
		attrs = append(attrs, "update_mask_paths", append([]string(nil), timing.UpdateMask...))
	}
	m.logger.Info("graph write stage timing", attrs...)
}

func (m *Module) logGraphCommitTiming(tx daemonsession.GraphTransaction, counts map[string]int, committedRevision int64, timing graphCommitTiming) {
	if m == nil || m.logger == nil || !m.writeTrace.ShouldLog(timing.Total) {
		return
	}
	attrs := []any{
		"event", "graph_write_commit_timing",
		"operation", "commit_transaction",
		"space_id", tx.SpaceID,
		"domain_id", tx.DomainID,
		"transaction_id", tx.ID,
		"committed_revision", committedRevision,
		"storage_mode", timing.StorageMode,
		"total_ms", writetrace.MS(timing.Total),
		"enter_write_ms", writetrace.MS(timing.EnterWrite),
		"overlay_snapshot_ms", writetrace.MS(timing.OverlaySnapshot),
		"store_open_ms", writetrace.MS(timing.StoreOpen),
		"overlay_changes_ms", writetrace.MS(timing.OverlayChanges),
		"graph_event_ms", writetrace.MS(timing.GraphEvent),
		"record_build_ms", writetrace.MS(timing.RecordBuild),
		"blob_reference_validate_ms", writetrace.MS(timing.BlobReferenceValidate),
		"wal_marshal_ms", writetrace.MS(timing.WALMarshal),
		"wal_append_ms", writetrace.MS(timing.WALAppend),
		"wal_sync_ms", writetrace.MS(timing.WALSync),
		"wal_apply_ms", writetrace.MS(timing.WALApply),
		"wal_mark_applied_ms", writetrace.MS(timing.WALMarkApplied),
		"raft_build_ms", writetrace.MS(timing.RaftBuild),
		"raft_propose_ms", writetrace.MS(timing.RaftPropose),
		"raft_validate_ms", writetrace.MS(timing.RaftProposal.Validate),
		"raft_encode_ms", writetrace.MS(timing.RaftProposal.Encode),
		"raft_waiter_register_ms", writetrace.MS(timing.RaftProposal.WaiterRegister),
		"raft_node_propose_ms", writetrace.MS(timing.RaftProposal.NodePropose),
		"raft_wait_apply_ms", writetrace.MS(timing.RaftProposal.WaitApply),
		"raft_storage_append_ms", writetrace.MS(timing.RaftProposal.StorageAppend),
		"raft_message_send_ms", writetrace.MS(timing.RaftProposal.MessageSend),
		"raft_state_machine_apply_ms", writetrace.MS(timing.RaftProposal.StateMachineApply),
		"raft_ready_entries", timing.RaftProposal.ReadyEntries,
		"raft_ready_messages", timing.RaftProposal.ReadyMessages,
		"direct_storage_ms", writetrace.MS(timing.DirectStorage),
		"change_sink_total_ms", writetrace.MS(timing.ChangeSink),
	}
	for k, v := range counts {
		attrs = append(attrs, k, v)
	}
	attrs = appendStorageTimingAttrs(attrs, timing.StorageCommit)
	if len(timing.SinkTimings) > 0 {
		for _, sink := range timing.SinkTimings {
			group := slog.Group("sink", "name", sink.Name, "duration_ms", writetrace.MS(sink.Duration), "error", sink.Error)
			attrs = append(attrs, group)
		}
	}
	m.logger.Info("graph write commit timing", attrs...)
}

func appendStorageTimingAttrs(attrs []any, timing graphstorage.CommitTiming) []any {
	return append(attrs,
		"storage_commit_total_ms", writetrace.MS(timing.Total),
		"storage_wait_lock_ms", writetrace.MS(timing.WaitLock),
		"storage_ensure_ready_ms", writetrace.MS(timing.EnsureReady),
		"storage_conflict_check_ms", writetrace.MS(timing.ConflictCheck),
		"storage_index_validation_ms", writetrace.MS(timing.IndexValidation),
		"storage_txn_begin_append_ms", writetrace.MS(timing.TxnBeginAppend),
		"storage_node_append_ms", writetrace.MS(timing.NodeAppend),
		"storage_edge_append_ms", writetrace.MS(timing.EdgeAppend),
		"storage_txn_commit_append_ms", writetrace.MS(timing.TxnCommitAppend),
		"storage_node_sync_ms", writetrace.MS(timing.NodeSync),
		"storage_edge_sync_ms", writetrace.MS(timing.EdgeSync),
		"storage_txn_sync_ms", writetrace.MS(timing.TxnSync),
		"storage_in_memory_apply_ms", writetrace.MS(timing.InMemoryApply),
		"storage_touched_domains", timing.TouchedDomains,
	)
}

func withGraphSinkTimingRecorder(ctx context.Context, recorder *graphSinkTimingRecorder) context.Context {
	return graphchange.WithSinkTimingRecorder(ctx, recorder)
}
