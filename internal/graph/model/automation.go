package graph

// AutomationOutputFenceValidation is the graph state machine's view of an
// automation-generated graph mutation. It is shared without coupling graph to
// the automation subsystem or its storage model.
type AutomationOutputFenceValidation struct {
	SpaceID                     string
	DomainID                    DomainID
	EntityKind                  string
	EntityID                    string
	AutomationID                string
	BindingID                   string
	RunID                       string
	InvocationID                string
	TargetNodeID                string
	ClaimOwnerNodeID            uint64
	ClaimVersion                uint64
	ClaimToken                  string
	OutputIdempotencyKey        string
	ProcedureID                 string
	AuthorizationRevision       uint64
	GlobalAuthorizationRevision uint64
	SpaceAuthorizationRevision  uint64
}

// HasFenceMetadata reports whether any claim or idempotency field is present.
func (v AutomationOutputFenceValidation) HasFenceMetadata() bool {
	return v.InvocationID != "" || v.ClaimToken != "" || v.ClaimVersion != 0 || v.ClaimOwnerNodeID != 0 || v.OutputIdempotencyKey != ""
}
