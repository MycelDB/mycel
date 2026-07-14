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

func DiscoverSeeds(ctx context.Context, opts DiscoveryOptions) {
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
		if res.Identity.BackendAdvertiseAddr != "" {
			seen := now
			_ = UpsertPeer(opts.DataDir, Peer{NodeID: res.Identity.NodeID, NodeName: res.Identity.NodeName, ClusterID: res.Identity.ClusterID, ClusterName: res.Identity.ClusterName, BackendAdvertiseAddr: res.Identity.BackendAdvertiseAddr, State: PeerStateActive, Source: PeerSourceDiscovered, LastSeenAt: &seen}, now)
		}
		for _, peer := range res.Peers {
			if peer.BackendAdvertiseAddr == "" || peer.BackendAdvertiseAddr == opts.Identity.BackendAdvertiseAddr {
				continue
			}
			if peer.State == "" {
				peer.State = PeerStateActive
			}
			if peer.Source == "" || peer.Source == PeerSourceSelf {
				peer.Source = PeerSourceDiscovered
			}
			_ = UpsertPeer(opts.DataDir, peer, now)
		}
		if opts.Logger != nil {
			opts.Logger.Info("cluster seed exchange complete", "seed", seed, "peer_count", len(res.Peers))
		}
	}
}
