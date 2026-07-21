package clustering

import (
	"context"
	"log/slog"
	"time"
)

type DiscoveryOptions struct {
	DataDir  string
	Identity NodeIdentity
	State    NodeState
	Seeds    []string
	Logger   *slog.Logger
	Timeout  time.Duration
}

func DiscoverSeeds(ctx context.Context, opts DiscoveryOptions) bool {
	if opts.Timeout <= 0 {
		opts.Timeout = 2 * time.Second
	}
	for _, seed := range opts.Seeds {
		seedCtx, cancel := context.WithTimeout(ctx, opts.Timeout)
		res, err := ExchangeWithPeer(seedCtx, seed, opts.Identity, opts.State)
		cancel()
		now := time.Now().UTC()
		if err != nil {
			_ = UpsertPeer(opts.DataDir, Peer{BackendAdvertiseAddr: seed, State: PeerStateUnreachable, Source: PeerSourceSeed}, now)
			if opts.Logger != nil {
				opts.Logger.Warn("cluster seed exchange failed", "seed", seed, "error", err)
			}
			continue
		}
		mergeExchangeResponse(opts, res, now)
		if opts.Logger != nil {
			opts.Logger.Info("cluster seed exchange complete", "seed", seed, "peer_count", len(res.Peers))
		}
		return true
	}
	return false
}

func RunDiscoveryLoop(ctx context.Context, opts DiscoveryOptions, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	if DiscoverSeeds(ctx, opts) {
		return
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if DiscoverSeeds(ctx, opts) {
				return
			}
		}
	}
}

func mergeExchangeResponse(opts DiscoveryOptions, res *ExchangeResponse, now time.Time) {
	if res.Identity.BackendAdvertiseAddr != "" && res.Identity.BackendAdvertiseAddr != opts.Identity.BackendAdvertiseAddr {
		seen := now
		_ = UpsertPeer(opts.DataDir, Peer{NodeID: res.Identity.NodeID, NodeName: res.Identity.NodeName, ClusterID: res.Identity.ClusterID, ClusterName: res.Identity.ClusterName, BackendAdvertiseAddr: res.Identity.BackendAdvertiseAddr, State: PeerStateActive, Source: PeerSourceDiscovered, LastSeenAt: &seen}, now)
	}
	for _, peer := range res.Peers {
		if peer.BackendAdvertiseAddr == "" || peer.BackendAdvertiseAddr == opts.Identity.BackendAdvertiseAddr {
			continue
		}
		if peer.State == "" || peer.State == PeerStateSelf || peer.State == PeerStateSeed || peer.State == PeerStateUnreachable {
			peer.State = PeerStateActive
		}
		if peer.Source == "" || peer.Source == PeerSourceSelf || peer.Source == PeerSourceSeed {
			peer.Source = PeerSourceDiscovered
		}
		if peer.LastSeenAt == nil && peer.NodeID != "" {
			seen := now
			peer.LastSeenAt = &seen
		}
		_ = UpsertPeer(opts.DataDir, peer, now)
	}
}
