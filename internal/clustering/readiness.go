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
	DerivedReady           bool
	RestoredClusterReady   bool
	DerivedSubsystems      []DerivedSubsystemReadiness
}

type DerivedSubsystemReadiness struct {
	Subsystem           string
	State               string
	Ready               bool
	RebuildRequired     bool
	Reason              string
	ArtifactID          string
	PolicyMode          string
	RestorePolicy       string
	ValidationState     string
	SourceFrontierKind  string
	SourceFrontierValue string
	Evidence            map[string]string
	UpdatedAt           string
}

func cloneDerivedSubsystemReadiness(in []DerivedSubsystemReadiness) []DerivedSubsystemReadiness {
	out := make([]DerivedSubsystemReadiness, 0, len(in))
	for _, row := range in {
		cp := row
		if row.Evidence != nil {
			cp.Evidence = make(map[string]string, len(row.Evidence))
			for k, v := range row.Evidence {
				cp.Evidence[k] = v
			}
		}
		out = append(out, cp)
	}
	return out
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
