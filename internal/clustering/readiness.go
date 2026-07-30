package clustering

import "strings"

type ClusterReadiness struct {
	ClientReady            bool
	MetadataApplied        bool
	MetadataValidated      bool
	PartitionGroupsStarted bool
	AuthoritativeClusterID string
	LocalClusterID         string
	ExpectedMemberCount    int
	ActiveMemberCount      int
	ReadinessBlockers      []string
}

func (r ClusterReadiness) withBlocker(blocker string) ClusterReadiness {
	blocker = strings.TrimSpace(blocker)
	if blocker == "" {
		return r
	}
	for _, existing := range r.ReadinessBlockers {
		if existing == blocker {
			return r
		}
	}
	r.ReadinessBlockers = append(r.ReadinessBlockers, blocker)
	r.ClientReady = false
	return r
}
