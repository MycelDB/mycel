package service

import (
	"sync"

	graph "github.com/myceldb/mycel/internal/graph/model"
	schemacompile "github.com/myceldb/mycel/internal/schema/compile"
)

type validationCache struct {
	mu       sync.RWMutex
	byDomain map[graph.DomainID]string
	byHash   map[string]*schemacompile.CompiledSchema
}

func newValidationCache() *validationCache {
	return &validationCache{byDomain: map[graph.DomainID]string{}, byHash: map[string]*schemacompile.CompiledSchema{}}
}

func (c *validationCache) put(domainID graph.DomainID, compiled *schemacompile.CompiledSchema) {
	if c == nil || compiled == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.byDomain == nil {
		c.byDomain = map[graph.DomainID]string{}
	}
	if c.byHash == nil {
		c.byHash = map[string]*schemacompile.CompiledSchema{}
	}
	c.byDomain[domainID] = compiled.Hash
	c.byHash[compiled.Hash] = compiled
}

func (c *validationCache) delete(domainID graph.DomainID) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.byDomain, domainID)
}

func (c *validationCache) get(domainID graph.DomainID) (*schemacompile.CompiledSchema, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	hash, ok := c.byDomain[domainID]
	if !ok {
		return nil, false
	}
	compiled, ok := c.byHash[hash]
	return compiled, ok
}
