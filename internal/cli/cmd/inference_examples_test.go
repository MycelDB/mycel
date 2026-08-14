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
			for _, operation := range endpoint.Operations {
				if strings.EqualFold(strings.TrimSpace(string(operation)), "chat") {
					t.Fatalf("%s: MycelDB example packages must not include chat endpoint operations", path)
				}
			}
			endpoints[endpoint.Key] = true
		}
		models := map[string]bool{}
		for _, model := range doc.Models {
			if strings.TrimSpace(model.Key) == "" || strings.TrimSpace(string(model.Operation)) == "" || strings.TrimSpace(model.ModelName) == "" {
				t.Fatalf("%s: model key, operation, and model_name are required: %#v", path, model)
			}
			if strings.EqualFold(strings.TrimSpace(string(model.Operation)), "chat") {
				t.Fatalf("%s: MycelDB example packages must not include chat models", path)
			}
			models[model.Key] = true
		}
		for _, capability := range doc.ModelEndpointCapabilities {
			if strings.EqualFold(strings.TrimSpace(string(capability.Operation)), "chat") {
				t.Fatalf("%s: MycelDB example packages must not include chat capabilities", path)
			}
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
		})
	}
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
