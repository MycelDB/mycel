package connectors

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

func TestOpenAICompatibleEmbeddingRequestShape(t *testing.T) {
	var gotPath, gotAuth string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		w.Header().Set("X-Request-Id", "req-embed")
		_, _ = w.Write([]byte(`{"data":[{"embedding":[0.1,0.2]}],"usage":{"prompt_tokens":3,"total_tokens":3}}`))
	}))
	defer srv.Close()

	resp, err := OpenAICompatible{}.Embed(context.Background(), EmbeddingRequest{Endpoint: endpoint(srv.URL, domaininference.OperationEmbeddings), Model: model(domaininference.OperationEmbeddings), Capability: capability(domaininference.OperationEmbeddings), Credential: credential(domaininference.CredentialAuthAPIKey), Secret: "secret", Input: "hello world"})
	if err != nil {
		t.Fatalf("Embed() error = %v", err)
	}
	if gotPath != "/embeddings" || gotAuth != "Bearer secret" || gotBody["model"] != "provider-model" || gotBody["input"] != "hello world" {
		t.Fatalf("unexpected request path=%s auth=%s body=%#v", gotPath, gotAuth, gotBody)
	}
	if resp.ProviderRequestID != "req-embed" || len(resp.Vector) != 2 || resp.Usage.TotalTokens != 3 {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

func TestOpenAICompatibleChatRequestShape(t *testing.T) {
	var gotPath, gotAuth string
	var gotBody map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&gotBody); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		_, _ = w.Write([]byte(`{"id":"chatcmpl-test","choices":[{"message":{"content":"{\"ok\":true}"}}],"usage":{"prompt_tokens":2,"completion_tokens":4,"total_tokens":6}}`))
	}))
	defer srv.Close()
	temp := 0.25
	resp, err := OpenAICompatible{}.Chat(context.Background(), ChatRequest{Endpoint: endpoint(srv.URL, domaininference.OperationChat), Model: model(domaininference.OperationChat), Capability: capability(domaininference.OperationChat), Credential: credential(domaininference.CredentialAuthBearer), Secret: "secret", Messages: []Message{{Role: "user", Content: "hello"}}, Parameters: domaininference.Parameters{Temperature: &temp, MaxOutputTokens: 32, ResponseFormat: "json"}})
	if err != nil {
		t.Fatalf("Chat() error = %v", err)
	}
	if gotPath != "/chat/completions" || gotAuth != "Bearer secret" || gotBody["model"] != "provider-model" || gotBody["max_tokens"].(float64) != 32 {
		t.Fatalf("unexpected request path=%s auth=%s body=%#v", gotPath, gotAuth, gotBody)
	}
	if _, ok := gotBody["response_format"].(map[string]any); !ok {
		t.Fatalf("expected response_format json_object, body=%#v", gotBody)
	}
	if resp.ProviderRequestID != "chatcmpl-test" || resp.Text == "" || resp.JSON["ok"] != true || resp.Usage.TotalTokens != 6 {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

func TestFakeConnectorCountsCalls(t *testing.T) {
	fake := &FakeConnector{Text: "done", Vector: []float64{0.5}}
	if _, err := fake.Embed(context.Background(), EmbeddingRequest{Input: "hello"}); err != nil {
		t.Fatalf("Embed() error = %v", err)
	}
	if _, err := fake.Chat(context.Background(), ChatRequest{Messages: []Message{{Role: "user", Content: "hello"}}}); err != nil {
		t.Fatalf("Chat() error = %v", err)
	}
	embed, chat := fake.Calls()
	if embed != 1 || chat != 1 {
		t.Fatalf("unexpected call counts embed=%d chat=%d", embed, chat)
	}
}

func endpoint(baseURL string, op domaininference.Operation) domaininference.Endpoint {
	return domaininference.Endpoint{ID: fixedEndpointID, Key: "endpoint", ConnectorType: domaininference.ConnectorOpenAICompatible, BaseURL: baseURL, NetworkClass: domaininference.NetworkClassPublicInternet, PrivacyClass: domaininference.PrivacyClassThirdParty, Operations: []domaininference.Operation{op}, Enabled: true}
}

func model(op domaininference.Operation) domaininference.Model {
	return domaininference.Model{ID: fixedModelID, Key: "model", Operation: op, ProviderModelName: "provider-model", Enabled: true}
}

func capability(op domaininference.Operation) domaininference.Capability {
	return domaininference.Capability{ID: uuid.New(), EndpointID: fixedEndpointID, ModelID: fixedModelID, Operation: op, Enabled: true}
}

func credential(auth domaininference.CredentialAuthType) domaininference.Credential {
	return domaininference.Credential{ID: uuid.New(), EndpointID: fixedEndpointID, AuthType: auth, Status: domaininference.CredentialStatusActive}
}

var (
	fixedEndpointID = uuid.New()
	fixedModelID    = uuid.New()
)
