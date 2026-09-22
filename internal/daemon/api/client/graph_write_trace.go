package client

import (
	"time"

	"github.com/myceldb/mycel/internal/graph/writetrace"
)

type graphRPCWriteTiming struct {
	Total       time.Duration
	Route       time.Duration
	TxLookup    time.Duration
	RequestMap  time.Duration
	GraphCall   time.Duration
	ResponseMap time.Duration
	Forwarded   bool
}

func (s *GraphService) logGraphRPCWriteTiming(operation string, transactionID string, counts map[string]int, timing graphRPCWriteTiming) {
	if s == nil || s.logger == nil || !s.writeTrace.ShouldLog(timing.Total) {
		return
	}
	attrs := []any{
		"event", "graph_write_rpc_timing",
		"operation", operation,
		"transaction_id", transactionID,
		"forwarded", timing.Forwarded,
		"total_ms", writetrace.MS(timing.Total),
		"route_ms", writetrace.MS(timing.Route),
		"tx_lookup_ms", writetrace.MS(timing.TxLookup),
		"request_map_ms", writetrace.MS(timing.RequestMap),
		"graph_call_ms", writetrace.MS(timing.GraphCall),
		"response_map_ms", writetrace.MS(timing.ResponseMap),
	}
	for k, v := range counts {
		attrs = append(attrs, k, v)
	}
	s.logger.Info("graph write rpc timing", attrs...)
}
