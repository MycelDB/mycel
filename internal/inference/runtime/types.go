package runtime

import (
	"errors"

	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

// ErrDenied indicates that inference policy rejected an invocation.
var ErrDenied = errors.New("inference request denied")

// ResolveRequest describes the runtime context used to select an inference
// capability, credential, and policy decision.
type ResolveRequest struct {
	SpaceID               string
	DomainID              string
	SemanticRuleID        string
	EmbeddingBindingKey   string
	SemanticIndexID       string
	NodeID                string
	NodeAncestorIDs       []string
	Operation             domaininference.Operation
	UsageMode             domaininference.UsageMode
	ProfileRef            string
	ProfileID             domaininference.ProfileID
	CapabilityRef         string
	CapabilityID          domaininference.CapabilityID
	EndpointRef           string
	EndpointID            domaininference.EndpointID
	ModelRef              string
	ModelID               domaininference.ModelID
	RequiredFeatures      []string
	Parameters            domaininference.Parameters
	ActorPrincipalID      string
	OnBehalfOfPrincipalID string
	Metadata              map[string]any
}

type ResolveResult struct {
	Allowed         bool
	Decision        domaininference.PolicyDecision
	Profile         domaininference.Profile
	Capability      domaininference.Capability
	Endpoint        domaininference.Endpoint
	Model           domaininference.Model
	Credential      domaininference.Credential
	Secret          domaininference.Secret
	CredentialGrant domaininference.CredentialGrant
}

type InvokeRequest struct {
	Resolve             ResolveRequest
	Messages            []domaininference.Message
	ImageInputs         []domaininference.ImageInput
	Prompt              string
	Input               string
	RequestID           string
	AutomationID        string
	AutomationRunID     string
	SemanticRuleID      string
	EmbeddingBindingKey string
	SemanticIndexID     string
	Metadata            map[string]any
}

type InvokeResponse struct {
	Allowed           bool
	Decision          domaininference.PolicyDecision
	ProviderRequestID string
	Text              string
	JSON              map[string]any
	Embedding         []float64
	Usage             domaininference.Usage
	Status            domaininference.UsageStatus
}
