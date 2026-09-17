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
	ProcessReady           bool
	MetadataReady          bool
	RaftReady              bool
	ReadReady              bool
	WriteReady             bool
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
	r.ReadReady = false
	r.WriteReady = false
	return r
}
