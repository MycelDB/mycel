package catalog

import (
	"embed"
	"encoding/json"
	"fmt"
	"strings"

	domainembedding "github.com/myceldb/mycel/domain/embedding"
)

//go:embed embedding_catalog.json
var catalogFS embed.FS

// Load returns the built-in embedding provider/model catalog.
func Load() (domainembedding.Catalog, error) {
	raw, err := catalogFS.ReadFile("embedding_catalog.json")
	if err != nil {
		return domainembedding.Catalog{}, err
	}
	return Decode(raw)
}

// Decode parses and validates catalog JSON.
func Decode(raw []byte) (domainembedding.Catalog, error) {
	var c domainembedding.Catalog
	if err := json.Unmarshal(raw, &c); err != nil {
		return domainembedding.Catalog{}, err
	}
	providers := map[string]struct{}{}
	models := map[string]struct{}{}
	for pi, provider := range c.Providers {
		if strings.TrimSpace(provider.ID) == "" {
			return domainembedding.Catalog{}, fmt.Errorf("provider %d has empty id", pi)
		}
		if _, exists := providers[provider.ID]; exists {
			return domainembedding.Catalog{}, fmt.Errorf("duplicate provider id %q", provider.ID)
		}
		providers[provider.ID] = struct{}{}
		if strings.TrimSpace(provider.Protocol) == "" {
			return domainembedding.Catalog{}, fmt.Errorf("provider %q has empty protocol", provider.ID)
		}
		for mi, model := range provider.Models {
			if strings.TrimSpace(model.ID) == "" {
				return domainembedding.Catalog{}, fmt.Errorf("provider %q model %d has empty id", provider.ID, mi)
			}
			if _, exists := models[model.ID]; exists {
				return domainembedding.Catalog{}, fmt.Errorf("duplicate model id %q", model.ID)
			}
			models[model.ID] = struct{}{}
			if model.ProviderID != provider.ID {
				return domainembedding.Catalog{}, fmt.Errorf("model %q provider_id mismatch", model.ID)
			}
			if strings.TrimSpace(model.Model) == "" {
				return domainembedding.Catalog{}, fmt.Errorf("model %q has empty provider model name", model.ID)
			}
			if model.Dimensions <= 0 {
				return domainembedding.Catalog{}, fmt.Errorf("model %q must declare positive dimensions", model.ID)
			}
		}
	}
	return c, nil
}

func FindProvider(c domainembedding.Catalog, providerID string) (domainembedding.ProviderDefinition, bool) {
	providerID = strings.TrimSpace(providerID)
	for _, provider := range c.Providers {
		if provider.ID == providerID {
			return provider, true
		}
	}
	return domainembedding.ProviderDefinition{}, false
}

func FindModel(c domainembedding.Catalog, providerID, modelID string) (domainembedding.ProviderDefinition, domainembedding.ModelDefinition, bool) {
	providerID = strings.TrimSpace(providerID)
	modelID = strings.TrimSpace(modelID)
	for _, provider := range c.Providers {
		if providerID != "" && provider.ID != providerID {
			continue
		}
		for _, model := range provider.Models {
			if model.ID == modelID || model.Model == modelID {
				return provider, model, true
			}
		}
	}
	return domainembedding.ProviderDefinition{}, domainembedding.ModelDefinition{}, false
}

func DefaultModel(c domainembedding.Catalog) (domainembedding.ProviderDefinition, domainembedding.ModelDefinition, bool) {
	for _, provider := range c.Providers {
		for _, model := range provider.Models {
			if model.Default {
				return provider, model, true
			}
		}
	}
	if len(c.Providers) == 0 || len(c.Providers[0].Models) == 0 {
		return domainembedding.ProviderDefinition{}, domainembedding.ModelDefinition{}, false
	}
	return c.Providers[0], c.Providers[0].Models[0], true
}
