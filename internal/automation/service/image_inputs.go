package service

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	automation "github.com/myceldb/mycel/internal/automation/model"
	blobservice "github.com/myceldb/mycel/internal/blob/service"
	graph "github.com/myceldb/mycel/internal/graph/model"
	"github.com/myceldb/mycel/internal/inference/connectors"
	domaininference "github.com/myceldb/mycel/internal/inference/model"
)

const (
	maxImageAnalysisImages     = 4
	maxImageAnalysisImageBytes = 20 * 1024 * 1024
	maxImageAnalysisTotalBytes = 40 * 1024 * 1024
)

func (m *AutomationManager) imageInputsForProcedure(ctx context.Context, inv automation.Invocation, def automation.Definition, run automation.Run, changed graph.Node, aliases map[string]any, collections map[string][]map[string]any) ([]connectors.ImageInput, error) {
	operation := domaininference.Operation(strings.ToLower(strings.TrimSpace(def.Inference.Operation)))
	if operation == "" {
		operation = domaininference.OperationChat
	}
	if operation != domaininference.OperationImageAnalysis {
		return nil, nil
	}
	if m.blobs == nil {
		return nil, nil
	}
	nodes := imageCandidateNodes(run.TargetAlias, changed, aliases, collections)
	seen := map[string]struct{}{}
	images := make([]connectors.ImageInput, 0, min(len(nodes), maxImageAnalysisImages))
	var totalBytes int64
	for _, node := range nodes {
		blobID := nodeBlobID(node)
		if blobID == "" {
			continue
		}
		if _, ok := seen[blobID]; ok {
			continue
		}
		seen[blobID] = struct{}{}
		if len(images) >= maxImageAnalysisImages {
			return nil, fmt.Errorf("image_analysis automation has more than %d image blob inputs", maxImageAnalysisImages)
		}
		image, err := m.loadImageBlobInput(ctx, inv.SpaceID, inv.DomainID.String(), blobID, node)
		if err != nil {
			return nil, err
		}
		if totalBytes+image.SizeBytes > maxImageAnalysisTotalBytes {
			return nil, fmt.Errorf("image_analysis automation image inputs exceed total size limit of %d bytes", maxImageAnalysisTotalBytes)
		}
		totalBytes += image.SizeBytes
		images = append(images, image)
	}
	if len(images) == 0 {
		return nil, fmt.Errorf("image_analysis automation requires at least one image blob input")
	}
	return images, nil
}

func imageCandidateNodes(targetAlias string, changed graph.Node, aliases map[string]any, collections map[string][]map[string]any) []graph.Node {
	out := []graph.Node{}
	addNode := func(node graph.Node) {
		out = append(out, node)
	}
	if targetAlias == "" || targetAlias == "changed" {
		addNode(changed)
	} else if node, ok := aliasGraphNode(aliases[targetAlias]); ok {
		addNode(node)
	} else {
		addNode(changed)
	}
	if targetAlias != "" && targetAlias != "changed" {
		addNode(changed)
	}
	aliasKeys := make([]string, 0, len(aliases))
	for key := range aliases {
		aliasKeys = append(aliasKeys, key)
	}
	sort.Strings(aliasKeys)
	for _, key := range aliasKeys {
		if node, ok := aliasGraphNode(aliases[key]); ok {
			addNode(node)
		}
	}
	collectionKeys := make([]string, 0, len(collections))
	for key := range collections {
		collectionKeys = append(collectionKeys, key)
	}
	sort.Strings(collectionKeys)
	for _, collectionKey := range collectionKeys {
		for _, row := range collections[collectionKey] {
			rowKeys := make([]string, 0, len(row))
			for key := range row {
				rowKeys = append(rowKeys, key)
			}
			sort.Strings(rowKeys)
			for _, key := range rowKeys {
				if node, ok := aliasGraphNode(row[key]); ok {
					addNode(node)
				}
			}
		}
	}
	return out
}

func (m *AutomationManager) loadImageBlobInput(ctx context.Context, spaceID string, domainID string, blobID string, node graph.Node) (connectors.ImageInput, error) {
	meta, reader, err := m.blobs.OpenBlobInDomain(ctx, spaceID, domainID, blobID)
	if err != nil {
		if errors.Is(err, blobservice.ErrNotFound) {
			return connectors.ImageInput{}, fmt.Errorf("image_analysis blob %q was not found", blobID)
		}
		return connectors.ImageInput{}, fmt.Errorf("open image_analysis blob %q: %w", blobID, err)
	}
	defer reader.Close()
	mimeType := normalizeImageInputMimeType(firstNonEmptyString(meta.MimeType, meta.DeclaredMimeType, nodePayloadString(node.Payload, "mime_type"), nodePayloadString(node.Payload, "declared_mime_type")))
	if !strings.HasPrefix(mimeType, "image/") {
		return connectors.ImageInput{}, fmt.Errorf("image_analysis blob %q has unsupported MIME type %q", blobID, mimeType)
	}
	if meta.SizeBytes > maxImageAnalysisImageBytes {
		return connectors.ImageInput{}, fmt.Errorf("image_analysis blob %q exceeds per-image size limit of %d bytes", blobID, maxImageAnalysisImageBytes)
	}
	data, err := io.ReadAll(io.LimitReader(reader, maxImageAnalysisImageBytes+1))
	if err != nil {
		return connectors.ImageInput{}, fmt.Errorf("read image_analysis blob %q: %w", blobID, err)
	}
	if int64(len(data)) > maxImageAnalysisImageBytes {
		return connectors.ImageInput{}, fmt.Errorf("image_analysis blob %q exceeds per-image size limit of %d bytes", blobID, maxImageAnalysisImageBytes)
	}
	sizeBytes := meta.SizeBytes
	if sizeBytes <= 0 {
		sizeBytes = int64(len(data))
	}
	return connectors.ImageInput{MimeType: mimeType, Data: data, SizeBytes: sizeBytes, Source: "graph_blob", BlobID: blobID}, nil
}

func nodeBlobID(node graph.Node) string {
	if node.BlobRef != nil {
		return strings.TrimSpace(string(*node.BlobRef))
	}
	return nodePayloadString(node.Payload, "blob_id")
}

func nodePayloadString(payload map[string]any, key string) string {
	if payload == nil {
		return ""
	}
	value, _ := payload[key].(string)
	return strings.TrimSpace(value)
}

func normalizeImageInputMimeType(value string) string {
	value = strings.TrimSpace(strings.ToLower(value))
	if idx := strings.Index(value, ";"); idx >= 0 {
		value = strings.TrimSpace(value[:idx])
	}
	return value
}
