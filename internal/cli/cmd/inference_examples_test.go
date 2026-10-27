package cmd

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	adminv1 "github.com/myceldb/mycel/internal/gen/mycel/admin/v1"
)

func TestInferenceExamplePackagesParse(t *testing.T) {
	dir := inferenceExamplesDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read examples/inference: %v", err)
	}
	found := 0
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		found++
		path := filepath.Join(dir, entry.Name())
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		var doc inferencePackageDocument
		if err := unmarshalYAMLWithJSONTags(raw, &doc); err != nil {
			t.Fatalf("parse %s: %v", path, err)
		}
		if strings.TrimSpace(doc.Name) == "" || strings.TrimSpace(doc.Version) == "" {
			t.Fatalf("%s: name and version are required", path)
		}
		endpoints := map[string]bool{}
		for _, endpoint := range doc.ModelEndpoints {
			if strings.TrimSpace(endpoint.Key) == "" || strings.TrimSpace(string(endpoint.ConnectorType)) == "" {
				t.Fatalf("%s: endpoint key and connector_type are required: %#v", path, endpoint)
			}
			endpoints[endpoint.Key] = true
		}
		models := map[string]bool{}
		for _, model := range doc.Models {
			if strings.TrimSpace(model.Key) == "" || strings.TrimSpace(string(model.Kind)) == "" || strings.TrimSpace(model.ModelName) == "" {
				t.Fatalf("%s: model key, kind, and model_name are required: %#v", path, model)
			}
			models[model.Key] = true
		}
		for _, capability := range doc.ModelEndpointCapabilities {
			if !endpoints[capability.ModelEndpoint] {
				t.Fatalf("%s: capability references unknown endpoint %q", path, capability.ModelEndpoint)
			}
			if !models[capability.Model] {
				t.Fatalf("%s: capability references unknown model %q", path, capability.Model)
			}
		}
		if strings.Contains(string(raw), "sk-") || strings.Contains(strings.ToLower(string(raw)), "api_key_ciphertext") {
			t.Fatalf("%s: examples must not contain secret material", path)
		}
	}
	if found == 0 {
		t.Fatalf("expected inference package examples under %s", dir)
	}
}

func TestStandardOpenAIExampleCatalog(t *testing.T) {
	chat := readInferenceExamplePackage(t, "standard-openai-chat.json")
	if chat.Version != "2026-10" {
		t.Fatalf("chat package version = %q, want 2026-10", chat.Version)
	}
	if len(chat.ModelEndpoints) != 1 || chat.ModelEndpoints[0].Metadata["verified_at"] != "2026-10-27" || chat.ModelEndpoints[0].Metadata["connector_supports_json_mode"] != true || chat.ModelEndpoints[0].Metadata["connector_supports_tool_calls"] != false {
		t.Fatalf("chat endpoint verification/connector metadata is incomplete: %#v", chat.ModelEndpoints)
	}
	expectedChatModels := map[string]string{
		"openai/gpt-5.6-sol":   "gpt-5.6-sol",
		"openai/gpt-5.6-terra": "gpt-5.6-terra",
		"openai/gpt-5.6-luna":  "gpt-5.6-luna",
	}
	if len(chat.Models) != len(expectedChatModels) {
		t.Fatalf("chat model count = %d, want %d", len(chat.Models), len(expectedChatModels))
	}
	for _, model := range chat.Models {
		providerName, ok := expectedChatModels[model.Key]
		if !ok {
			t.Fatalf("unexpected bundled chat model %q", model.Key)
		}
		if model.ModelName != providerName {
			t.Errorf("%s provider model = %q, want %q", model.Key, model.ModelName, providerName)
		}
		if string(model.Kind) != "generative" || !containsText(model.InputModalities, "text") || !containsText(model.InputModalities, "image") || !containsText(model.OutputModalities, "text") || !containsText(model.OutputModalities, "json") {
			t.Errorf("%s has unexpected kind/modalities: %#v", model.Key, model)
		}
		assertMetadataNumber(t, model.Key, model.Metadata, "context_tokens", 1050000)
		assertMetadataNumber(t, model.Key, model.Metadata, "max_output_tokens", 128000)
		if got := model.Metadata["output_token_parameter"]; got != "max_completion_tokens" {
			t.Errorf("%s output_token_parameter = %#v, want max_completion_tokens", model.Key, got)
		}
		if got, ok := model.Metadata["documentation_url"].(string); !ok || got == "" {
			t.Errorf("%s documentation_url = %#v, want official source URL", model.Key, model.Metadata["documentation_url"])
		}
	}

	expectedOperations := map[string]bool{"chat": true, "summarize": true, "classify": true, "image_analysis": true}
	operationsByModel := make(map[string]map[string]bool, len(expectedChatModels))
	for _, capability := range chat.ModelEndpointCapabilities {
		if capability.ModelEndpoint != "openai" {
			t.Errorf("chat capability endpoint = %q, want openai", capability.ModelEndpoint)
		}
		if _, ok := expectedChatModels[capability.Model]; !ok {
			t.Fatalf("capability references unexpected chat model %q", capability.Model)
		}
		operation := string(capability.Operation)
		if !expectedOperations[operation] {
			t.Fatalf("%s has unsupported bundled operation %q", capability.Model, operation)
		}
		if operationsByModel[capability.Model] == nil {
			operationsByModel[capability.Model] = map[string]bool{}
		}
		if operationsByModel[capability.Model][operation] {
			t.Fatalf("duplicate %s capability for %s", operation, capability.Model)
		}
		operationsByModel[capability.Model][operation] = true
		assertMetadataNumber(t, capability.Model+"/"+operation, capability.Metadata, "max_input_tokens", 1050000)
		assertMetadataNumber(t, capability.Model+"/"+operation, capability.Metadata, "max_output_tokens", 128000)
		if got := capability.Metadata["output_token_parameter"]; got != "max_completion_tokens" {
			t.Errorf("%s/%s output_token_parameter = %#v, want max_completion_tokens", capability.Model, operation, got)
		}
	}
	for model := range expectedChatModels {
		if len(operationsByModel[model]) != len(expectedOperations) {
			t.Errorf("%s operations = %#v, want %#v", model, operationsByModel[model], expectedOperations)
		}
	}

	embeddings := readInferenceExamplePackage(t, "standard-openai-embeddings.json")
	if embeddings.Version != "2026-10" {
		t.Fatalf("embedding package version = %q, want 2026-10", embeddings.Version)
	}
	if len(embeddings.ModelEndpoints) != 1 || embeddings.ModelEndpoints[0].Metadata["verified_at"] != "2026-10-27" || embeddings.ModelEndpoints[0].Metadata["connector_uses_fixed_dimensions"] != true {
		t.Fatalf("embedding endpoint verification/connector metadata is incomplete: %#v", embeddings.ModelEndpoints)
	}
	expectedEmbeddingModels := map[string]int{
		"openai/text-embedding-3-small": 1536,
		"openai/text-embedding-3-large": 3072,
	}
	if len(embeddings.Models) != len(expectedEmbeddingModels) {
		t.Fatalf("embedding model count = %d, want %d", len(embeddings.Models), len(expectedEmbeddingModels))
	}
	for _, model := range embeddings.Models {
		dimensions, ok := expectedEmbeddingModels[model.Key]
		if !ok {
			t.Fatalf("unexpected bundled embedding model %q", model.Key)
		}
		if model.ModelName != strings.TrimPrefix(model.Key, "openai/") || model.Dimensions != dimensions || model.VectorSpaceKey != model.Key {
			t.Errorf("%s has inconsistent provider name/dimensions/vector space: %#v", model.Key, model)
		}
		assertMetadataNumber(t, model.Key, model.Metadata, "max_input_tokens", 8192)
		assertMetadataNumber(t, model.Key, model.Metadata, "default_dimensions", dimensions)
		if model.Metadata["provider_supports_adjustable_dimensions"] != true || model.Metadata["connector_uses_fixed_dimensions"] != true {
			t.Errorf("%s adjustable/fixed dimension metadata is incomplete: %#v", model.Key, model.Metadata)
		}
		if got, ok := model.Metadata["documentation_url"].(string); !ok || got == "" {
			t.Errorf("%s documentation_url = %#v, want official source URL", model.Key, model.Metadata["documentation_url"])
		}
	}
	if len(embeddings.ModelEndpointCapabilities) != len(expectedEmbeddingModels) {
		t.Fatalf("embedding capability count = %d, want %d", len(embeddings.ModelEndpointCapabilities), len(expectedEmbeddingModels))
	}
	for _, capability := range embeddings.ModelEndpointCapabilities {
		dimensions, ok := expectedEmbeddingModels[capability.Model]
		if !ok || capability.ModelEndpoint != "openai" || string(capability.Operation) != "embeddings" {
			t.Fatalf("unexpected embedding capability: %#v", capability)
		}
		assertMetadataNumber(t, capability.Model, capability.Metadata, "max_input_tokens", 8192)
		assertMetadataNumber(t, capability.Model, capability.Metadata, "dimensions", dimensions)
	}
}

func TestInferenceExamplePackagesApplyViaDaemonCLI(t *testing.T) {
	_, addr, adminPassword, cleanup := startDaemonAdminGRPC(t)
	defer cleanup()

	for _, path := range inferenceExamplePackagePaths(t) {
		t.Run(filepath.Base(path), func(t *testing.T) {
			out, err := runCLI(t, "--daemon-addr", addr, "-u", "admin", "-p", adminPassword, "--output", "json", "inference", "package", "apply", path)
			if err != nil {
				t.Fatalf("inference package apply failed: %v\n%s", err, out)
			}
			var res adminv1.AdminInferenceCatalogServiceApplyInferencePackageResponse
			if err := json.Unmarshal([]byte(out), &res); err != nil {
				t.Fatalf("decode apply response: %v\n%s", err, out)
			}
			if res.GetPackage().GetName() == "" || len(res.GetModelEndpoints()) == 0 || len(res.GetModels()) == 0 || len(res.GetModelEndpointCapabilities()) == 0 {
				t.Fatalf("unexpected apply response for %s: %#v", path, &res)
			}
			switch filepath.Base(path) {
			case "standard-openai-chat.json":
				expected := map[string]bool{"gpt-5.6-sol": true, "gpt-5.6-terra": true, "gpt-5.6-luna": true}
				if len(res.GetModels()) != len(expected) || len(res.GetModelEndpointCapabilities()) != 12 {
					t.Fatalf("unexpected imported chat catalog sizes: models=%d capabilities=%d", len(res.GetModels()), len(res.GetModelEndpointCapabilities()))
				}
				for _, model := range res.GetModels() {
					if !expected[model.GetModelName()] {
						t.Fatalf("unexpected imported chat provider model %q", model.GetModelName())
					}
					metadata := model.GetMetadata().AsMap()
					assertMetadataNumber(t, model.GetModelName(), metadata, "context_tokens", 1050000)
					assertMetadataNumber(t, model.GetModelName(), metadata, "max_output_tokens", 128000)
				}
			case "standard-openai-embeddings.json":
				expected := map[string]int32{"text-embedding-3-small": 1536, "text-embedding-3-large": 3072}
				if len(res.GetModels()) != len(expected) || len(res.GetModelEndpointCapabilities()) != 2 {
					t.Fatalf("unexpected imported embedding catalog sizes: models=%d capabilities=%d", len(res.GetModels()), len(res.GetModelEndpointCapabilities()))
				}
				for _, model := range res.GetModels() {
					if dimensions, ok := expected[model.GetModelName()]; !ok || model.GetDimensions() != dimensions {
						t.Fatalf("unexpected imported embedding model: %#v", model)
					}
				}
			}
		})
	}
}

func readInferenceExamplePackage(t *testing.T, name string) inferencePackageDocument {
	t.Helper()
	path := filepath.Join(inferenceExamplesDir(), name)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var doc inferencePackageDocument
	if err := unmarshalYAMLWithJSONTags(raw, &doc); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	return doc
}

func assertMetadataNumber(t *testing.T, resource string, metadata map[string]any, key string, want int) {
	t.Helper()
	got, ok := metadata[key].(float64)
	if !ok || got != float64(want) {
		t.Errorf("%s metadata %s = %#v, want %d", resource, key, metadata[key], want)
	}
}

func containsText(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func inferenceExamplesDir() string {
	return filepath.Join("..", "..", "..", "examples", "inference")
}

func inferenceExamplePackagePaths(t *testing.T) []string {
	t.Helper()
	dir := inferenceExamplesDir()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read examples/inference: %v", err)
	}
	paths := []string{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		paths = append(paths, filepath.Join(dir, entry.Name()))
	}
	if len(paths) == 0 {
		t.Fatalf("expected inference package examples under %s", dir)
	}
	return paths
}
