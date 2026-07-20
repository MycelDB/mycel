package registration

import (
	"context"
	"log/slog"
	"time"

	"github.com/myceldb/mycel/internal/clustering/model"
	"github.com/myceldb/mycel/internal/clustering/topology"
)

type BackendClient interface {
	RegisterNode(ctx context.Context, addr string, in RegisterNodeInput) (RegisterNodeResult, error)
}

type RegisterNodeInput struct {
	Identity                 model.NodeIdentity
	State                    model.NodeState
	KnownPeers               []model.Peer
	NodePublicKeyFingerprint string
}

type RegisterNodeResult struct {
	Accepted bool
	Reason   string
	Snapshot model.Snapshot
}

type Handler struct {
	Topology   *topology.Registry
	Client     BackendClient
	Seeds      []string
	Identity   model.NodeIdentity
	State      model.NodeState
	Interval   time.Duration
	Timeout    time.Duration
	Logger     *slog.Logger
	OnAdmitted func(clusterID string) error
}

func (h *Handler) Run(ctx context.Context) error {
	if len(h.Seeds) == 0 || h.Client == nil || h.Topology == nil {
		return nil
	}
	if h.Interval <= 0 {
		h.Interval = 5 * time.Second
	}
	if h.TryOnce(ctx) {
		return nil
	}
	ticker := time.NewTicker(h.Interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if h.TryOnce(ctx) {
				return nil
			}
		}
	}
}

func (h *Handler) TryOnce(ctx context.Context) bool {
	for _, seed := range h.Seeds {
		callCtx := ctx
		cancel := func() {}
		if h.Timeout > 0 {
			callCtx, cancel = context.WithTimeout(ctx, h.Timeout)
		}
		result, err := h.Client.RegisterNode(callCtx, seed, RegisterNodeInput{Identity: h.Identity, State: h.State, KnownPeers: h.Topology.Snapshot().Peers, NodePublicKeyFingerprint: h.Identity.NodePublicKeyFingerprint})
		cancel()
		if err != nil || !result.Accepted {
			if h.Logger != nil {
				if err != nil {
					h.Logger.Warn("cluster registration failed", "seed", seed, "error", err)
				} else {
					h.Logger.Warn("cluster registration rejected", "seed", seed, "reason", result.Reason)
				}
			}
			continue
		}
		if !h.Identity.ClusterAdmitted && h.OnAdmitted != nil {
			clusterID := ""
			for _, peer := range result.Snapshot.Peers {
				if peer.State == model.PeerStateSelf {
					clusterID = peer.ClusterID
					break
				}
			}
			if clusterID != "" {
				if err := h.OnAdmitted(clusterID); err != nil {
					return false
				}
			}
		}
		if err := h.Topology.Merge(ctx, result.Snapshot); err != nil {
			if h.Logger != nil {
				h.Logger.Warn("cluster registration merge failed", "seed", seed, "error", err)
			}
			return false
		}
		if h.Logger != nil {
			h.Logger.Info("cluster registration complete", "seed", seed, "peer_count", len(result.Snapshot.Peers))
		}
		return true
	}
	return false
}
