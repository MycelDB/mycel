package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/myceldb/mycel/internal/search/lexical/index"
)

var (
	ErrNotOwner         = errors.New("lexical index scope is not owned by this node")
	ErrOwnerUnavailable = errors.New("lexical index owner unavailable")
)

type Owner struct {
	Known   bool
	NodeID  string
	IsLocal bool
}

type OwnerResolver interface {
	ResolveLexicalOwner(ctx context.Context, spaceID, domainID string) (Owner, error)
}

type Forwarder interface {
	ForwardSearch(ctx context.Context, ownerNodeID string, req SearchRequest) (index.SearchResponse, error)
	ForwardStatus(ctx context.Context, ownerNodeID string, req StatusRequest) (Status, error)
}

type SearchRequest struct {
	SpaceID  string
	DomainID string
	Query    string
	Options  index.SearchOptions
}

type StatusRequest struct {
	SpaceID                  string
	DomainID                 string
	LatestKnownGraphRevision uint64
}

type Coordinator struct {
	local     *Service
	resolver  OwnerResolver
	forwarder Forwarder
}

func NewCoordinator(local *Service, resolver OwnerResolver, forwarder Forwarder) *Coordinator {
	return &Coordinator{local: local, resolver: resolver, forwarder: forwarder}
}

func (c *Coordinator) Search(ctx context.Context, req SearchRequest) (index.SearchResponse, error) {
	owner, err := c.resolve(ctx, req.SpaceID, req.DomainID)
	if err != nil {
		return index.SearchResponse{}, err
	}
	if owner.IsLocal {
		return c.local.Search(req.Query, req.Options)
	}
	if c.forwarder == nil {
		return index.SearchResponse{}, fmt.Errorf("%w: no forwarder configured for owner %s", ErrOwnerUnavailable, owner.NodeID)
	}
	return c.forwarder.ForwardSearch(ctx, owner.NodeID, req)
}

func (c *Coordinator) Status(ctx context.Context, req StatusRequest) (Status, error) {
	owner, err := c.resolve(ctx, req.SpaceID, req.DomainID)
	if err != nil {
		return Status{}, err
	}
	if owner.IsLocal {
		return c.local.Status(req.LatestKnownGraphRevision), nil
	}
	if c.forwarder == nil {
		return Status{}, fmt.Errorf("%w: no forwarder configured for owner %s", ErrOwnerUnavailable, owner.NodeID)
	}
	return c.forwarder.ForwardStatus(ctx, owner.NodeID, req)
}

func (c *Coordinator) ApplyDocuments(ctx context.Context, spaceID, domainID string, docs []index.IndexedDocument, revision uint64) error {
	owner, err := c.resolve(ctx, spaceID, domainID)
	if err != nil {
		return err
	}
	if !owner.IsLocal {
		return fmt.Errorf("%w: owner is %s", ErrNotOwner, owner.NodeID)
	}
	return c.local.ApplyDocuments(docs, revision)
}

func (c *Coordinator) Rebuild(ctx context.Context, spaceID, domainID string, docs []index.IndexedDocument, latestRevision uint64) error {
	owner, err := c.resolve(ctx, spaceID, domainID)
	if err != nil {
		return err
	}
	if !owner.IsLocal {
		return fmt.Errorf("%w: owner is %s", ErrNotOwner, owner.NodeID)
	}
	return c.local.Rebuild(docs, latestRevision)
}

func (c *Coordinator) resolve(ctx context.Context, spaceID, domainID string) (Owner, error) {
	if c.local == nil {
		return Owner{}, fmt.Errorf("%w: local lexical service is not configured", ErrOwnerUnavailable)
	}
	if c.resolver == nil {
		return Owner{Known: true, IsLocal: true}, nil
	}
	owner, err := c.resolver.ResolveLexicalOwner(ctx, spaceID, domainID)
	if err != nil {
		return Owner{}, err
	}
	if !owner.Known || owner.NodeID == "" && !owner.IsLocal {
		return Owner{}, fmt.Errorf("%w: no safe owner route for space %s domain %s", ErrOwnerUnavailable, spaceID, domainID)
	}
	return owner, nil
}
