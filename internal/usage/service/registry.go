package service

import "fmt"

type Registry struct {
	defs map[string]MetricDefinition
}

func NewRegistry(defs ...MetricDefinition) Registry {
	r := Registry{defs: map[string]MetricDefinition{}}
	for _, def := range defs {
		if def.Version == 0 {
			def.Version = 1
		}
		r.defs[definitionKey(def.Name, def.Version)] = def
	}
	return r
}

func DefaultRegistry() Registry {
	return NewRegistry(
		MetricDefinition{Name: MetricGraphNodeCount, Version: 1, Unit: UnitCount, Authority: AuthorityRaftCommittedExact, QuotaSafe: true, HardLimitEligible: true},
		MetricDefinition{Name: MetricGraphEdgeCount, Version: 1, Unit: UnitCount, Authority: AuthorityRaftCommittedExact, QuotaSafe: true, HardLimitEligible: true},
		MetricDefinition{Name: MetricBlobActiveLogicalBytes, Version: 1, Unit: UnitBytes, Authority: AuthorityRaftCommittedExact, QuotaSafe: true, HardLimitEligible: true},
		MetricDefinition{Name: MetricBlobActiveCount, Version: 1, Unit: UnitCount, Authority: AuthorityRaftCommittedExact, QuotaSafe: true, HardLimitEligible: true},
		MetricDefinition{Name: MetricBlobLocalActiveLogicalBytes, Version: 1, Unit: UnitBytes, Authority: AuthorityRaftCommittedExact, QuotaSafe: true, HardLimitEligible: true},
		MetricDefinition{Name: MetricBlobLocalActiveCount, Version: 1, Unit: UnitCount, Authority: AuthorityRaftCommittedExact, QuotaSafe: true, HardLimitEligible: true},
		MetricDefinition{Name: MetricBlobObjectActiveLogicalBytes, Version: 1, Unit: UnitBytes, Authority: AuthorityRaftCommittedExact, QuotaSafe: true, HardLimitEligible: true},
		MetricDefinition{Name: MetricBlobObjectActiveCount, Version: 1, Unit: UnitCount, Authority: AuthorityRaftCommittedExact, QuotaSafe: true, HardLimitEligible: true},
		MetricDefinition{Name: MetricBlobPendingDeleteLogicalBytes, Version: 1, Unit: UnitBytes, Authority: AuthorityRaftCommittedExact, QuotaSafe: true, HardLimitEligible: false},
		MetricDefinition{Name: MetricBlobPendingDeleteCount, Version: 1, Unit: UnitCount, Authority: AuthorityRaftCommittedExact, QuotaSafe: true, HardLimitEligible: false},
		MetricDefinition{Name: MetricDiagnosticLocalScopePhysicalBytes, Version: 1, Unit: UnitBytes, Authority: AuthorityObservedApproximate, QuotaSafe: false, HardLimitEligible: false},
		MetricDefinition{Name: MetricDiagnosticLocalGraphStoreBytes, Version: 1, Unit: UnitBytes, Authority: AuthorityObservedApproximate, QuotaSafe: false, HardLimitEligible: false},
		MetricDefinition{Name: MetricDiagnosticLocalBlobStoreBytes, Version: 1, Unit: UnitBytes, Authority: AuthorityObservedApproximate, QuotaSafe: false, HardLimitEligible: false},
		MetricDefinition{Name: MetricDiagnosticLocalBlobMetadataBytes, Version: 1, Unit: UnitBytes, Authority: AuthorityObservedApproximate, QuotaSafe: false, HardLimitEligible: false},
		MetricDefinition{Name: MetricDiagnosticLocalLexicalStoreBytes, Version: 1, Unit: UnitBytes, Authority: AuthorityObservedApproximate, QuotaSafe: false, HardLimitEligible: false},
		MetricDefinition{Name: MetricDiagnosticLocalSemanticStoreBytes, Version: 1, Unit: UnitBytes, Authority: AuthorityObservedApproximate, QuotaSafe: false, HardLimitEligible: false},
		MetricDefinition{Name: MetricDiagnosticLocalProjectionStoreBytes, Version: 1, Unit: UnitBytes, Authority: AuthorityObservedApproximate, QuotaSafe: false, HardLimitEligible: false},
	)
}

func (r Registry) Definition(name string, version uint32) (MetricDefinition, bool) {
	if version == 0 {
		version = 1
	}
	def, ok := r.defs[definitionKey(name, version)]
	return def, ok
}

func (r Registry) MustDefinition(name string, version uint32) (MetricDefinition, error) {
	def, ok := r.Definition(name, version)
	if !ok {
		return MetricDefinition{}, fmt.Errorf("usage metric %q version %d is not registered", name, version)
	}
	return def, nil
}

func (r Registry) Definitions() []MetricDefinition {
	out := make([]MetricDefinition, 0, len(r.defs))
	for _, def := range r.defs {
		out = append(out, def)
	}
	return out
}

func definitionKey(name string, version uint32) string {
	return fmt.Sprintf("%s@%d", name, version)
}
