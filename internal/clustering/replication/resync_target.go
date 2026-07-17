package replication

import (
	"context"
	"strings"

	"github.com/myceldb/mycel/internal/clustering"
	"github.com/myceldb/mycel/internal/clustering/membership"
	"github.com/myceldb/mycel/internal/clustering/replsnapshot"
)

func ResolveFollowerTarget(ctx context.Context, manager *clustering.Manager, target string) (replsnapshot.ResyncTarget, error) {
	if err := ctx.Err(); err != nil {
		return replsnapshot.ResyncTarget{}, err
	}
	needle := strings.TrimSpace(target)
	if manager == nil || needle == "" || manager.Membership() == nil {
		return replsnapshot.ResyncTarget{}, replsnapshot.ErrResyncTargetNotFound
	}
	authority, _ := manager.Authority()
	data, err := manager.Membership().Load(ctx)
	if err != nil {
		return replsnapshot.ResyncTarget{}, err
	}
	for _, m := range data.Members {
		if m.NodeID != needle && m.NodeName != needle {
			continue
		}
		if m.State != membership.MemberStateActive || m.NodeID == "" || m.NodeID == manager.Identity().NodeID || m.NodeID == authority.Primary.NodeID {
			return replsnapshot.ResyncTarget{}, replsnapshot.ErrResyncTargetNotFollower
		}
		if strings.TrimSpace(m.BackendAdvertiseAddr) == "" {
			return replsnapshot.ResyncTarget{}, replsnapshot.ErrResyncTargetNotFollower
		}
		return replsnapshot.ResyncTarget{NodeID: m.NodeID, NodeName: m.NodeName, BackendAdvertiseAddr: m.BackendAdvertiseAddr}, nil
	}
	return replsnapshot.ResyncTarget{}, replsnapshot.ErrResyncTargetNotFound
}
