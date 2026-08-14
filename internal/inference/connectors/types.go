package connectors

import (
	"context"
	"errors"
	"fmt"
	"strings"

	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

type Message struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type Usage struct {
	InputTokens      int64
	OutputTokens     int64
	TotalTokens      int64
	TokenCountSource string
}

type EmbeddingRequest struct {
	Endpoint   domaininference.Endpoint
	Model      domaininference.Model
	Capability domaininference.Capability
	Credential domaininference.Credential
	Secret     string
	Input      string
}

type EmbeddingResponse struct {
	Vector            []float64
	ProviderRequestID string
	Usage             Usage
}

type ChatRequest struct {
	Endpoint   domaininference.Endpoint
	Model      domaininference.Model
	Capability domaininference.Capability
	Credential domaininference.Credential
	Secret     string
	Messages   []Message
	Parameters domaininference.Parameters
}

type ChatResponse struct {
	Text              string
	JSON              map[string]any
	ProviderRequestID string
	Usage             Usage
}

type EmbeddingConnector interface {
	Embed(ctx context.Context, req EmbeddingRequest) (EmbeddingResponse, error)
}

type ChatConnector interface {
	Chat(ctx context.Context, req ChatRequest) (ChatResponse, error)
}

type Connector interface {
	EmbeddingConnector
	ChatConnector
}

type ConnectorError struct {
	Code      string
	Retryable bool
	Err       error
}

func (e ConnectorError) Error() string {
	if e.Err == nil {
		return strings.TrimSpace(e.Code)
	}
	if strings.TrimSpace(e.Code) == "" {
		return e.Err.Error()
	}
	return fmt.Sprintf("%s: %v", e.Code, e.Err)
}

func (e ConnectorError) Unwrap() error { return e.Err }

func ErrorCode(err error) string {
	if err == nil {
		return ""
	}
	var connectorErr ConnectorError
	if AsConnectorError(err, &connectorErr) && strings.TrimSpace(connectorErr.Code) != "" {
		return connectorErr.Code
	}
	return "connector_error"
}

func AsConnectorError(err error, target *ConnectorError) bool {
	if err == nil || target == nil {
		return false
	}
	if errors.As(err, target) {
		return true
	}
	return false
}

func EstimateTokens(s string) int64 {
	words := strings.Fields(s)
	if len(words) == 0 {
		return 0
	}
	return int64(len(words))
}
