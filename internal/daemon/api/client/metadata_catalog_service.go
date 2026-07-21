package client

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	domaingraph "github.com/myceldb/mycel/internal/graph/model"
	daegraph "github.com/myceldb/mycel/internal/graph/service"
	daemonsession "github.com/myceldb/mycel/internal/session/service"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const metadataCatalogMaxPageSize = 500

type MetadataCatalogService struct {
	clientv1.UnimplementedMetadataCatalogServiceServer
	sessions daemonsession.Manager
	graphs   daegraph.Manager
}

func NewMetadataCatalogService(sessions daemonsession.Manager, graphs daegraph.Manager) *MetadataCatalogService {
	return &MetadataCatalogService{sessions: sessions, graphs: graphs}
}

func (s *MetadataCatalogService) ListTags(ctx context.Context, req *clientv1.ListTagsRequest) (*clientv1.ListTagsResponse, error) {
	tx, err := s.readableTransaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	nodes, err := allExportNodes(ctx, s.graphs, tx)
	if err != nil {
		return nil, mapGraphError(err, "list metadata tags")
	}
	counts := map[string]int64{}
	for _, node := range nodes {
		tags, err := domaingraph.NormalizeTagsValue(node.Props[domaingraph.NodePropTags])
		if err != nil {
			continue
		}
		for _, tag := range tags {
			counts[tag]++
		}
	}
	items := make([]*clientv1.TagSummary, 0, len(counts))
	for name, count := range counts {
		items = append(items, &clientv1.TagSummary{Name: name, NodeCount: count})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].NodeCount != items[j].NodeCount {
			return items[i].NodeCount > items[j].NodeCount
		}
		return items[i].Name < items[j].Name
	})
	page, next, err := paginateMetadata(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &clientv1.ListTagsResponse{Tags: page, NextPageToken: next}, nil
}

func (s *MetadataCatalogService) ListPropertyNames(ctx context.Context, req *clientv1.ListPropertyNamesRequest) (*clientv1.ListPropertyNamesResponse, error) {
	tx, err := s.readableTransaction(ctx, req.GetTransactionId())
	if err != nil {
		return nil, err
	}
	nodes, err := allExportNodes(ctx, s.graphs, tx)
	if err != nil {
		return nil, mapGraphError(err, "list metadata properties")
	}
	counts := map[string]int64{}
	for _, node := range nodes {
		properties, err := domaingraph.NormalizeCustomPropertiesValue(node.Props[domaingraph.NodePropCustomProperties])
		if err != nil {
			continue
		}
		for name := range properties {
			counts[name]++
		}
	}
	items := make([]*clientv1.PropertySummary, 0, len(counts))
	for name, count := range counts {
		items = append(items, &clientv1.PropertySummary{Name: name, NodeCount: count})
	}
	sort.Slice(items, func(i, j int) bool {
		if items[i].NodeCount != items[j].NodeCount {
			return items[i].NodeCount > items[j].NodeCount
		}
		return items[i].Name < items[j].Name
	})
	page, next, err := paginateMetadata(items, int(req.GetPageSize()), req.GetPageToken())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	return &clientv1.ListPropertyNamesResponse{Properties: page, NextPageToken: next}, nil
}

func (s *MetadataCatalogService) readableTransaction(ctx context.Context, transactionID string) (daemonsession.GraphTransaction, error) {
	principal, err := spaceUserPrincipalFromContext(ctx)
	if err != nil {
		return daemonsession.GraphTransaction{}, err
	}
	tx, err := s.sessions.GetTransaction(ctx, principal.UserID, transactionID)
	if err != nil {
		return daemonsession.GraphTransaction{}, mapSessionError(err, "metadata catalog")
	}
	if tx.State != daemonsession.TransactionStateActive {
		return daemonsession.GraphTransaction{}, status.Error(codes.FailedPrecondition, "transaction is not active")
	}
	return tx, nil
}

func paginateMetadata[T any](items []T, pageSize int, pageToken string) ([]T, string, error) {
	start := 0
	if strings.TrimSpace(pageToken) != "" {
		value, err := strconv.Atoi(pageToken)
		if err != nil || value < 0 {
			return nil, "", fmt.Errorf("invalid page_token")
		}
		start = value
	}
	if pageSize <= 0 || pageSize > metadataCatalogMaxPageSize {
		pageSize = metadataCatalogMaxPageSize
	}
	if start >= len(items) {
		return []T{}, "", nil
	}
	end := start + pageSize
	if end > len(items) {
		end = len(items)
	}
	next := ""
	if end < len(items) {
		next = strconv.Itoa(end)
	}
	return items[start:end], next, nil
}
