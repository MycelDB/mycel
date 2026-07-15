package registration

import (
	"context"

	"github.com/myceldb/mycel/internal/clustering/backend"
)

type BackendAdapter struct{ Client backend.Client }

func (a BackendAdapter) RegisterNode(ctx context.Context, addr string, in RegisterNodeInput) (RegisterNodeResult, error) {
	res, err := a.Client.RegisterNode(ctx, addr, backend.RegisterNodeInput{Identity: in.Identity, State: in.State, KnownPeers: in.KnownPeers, JoinToken: in.JoinToken, NodePublicKeyFingerprint: in.NodePublicKeyFingerprint})
	if err != nil {
		return RegisterNodeResult{}, err
	}
	return RegisterNodeResult{Accepted: res.Accepted, Reason: res.Reason, Snapshot: res.Snapshot}, nil
}
