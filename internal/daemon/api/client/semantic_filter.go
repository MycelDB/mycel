package client

import (
	"fmt"
	"sort"
	"strings"

	"github.com/google/uuid"
	clientv1 "github.com/myceldb/mycel/internal/gen/mycel/client/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	semanticMaxFilterLabels     = 32
	semanticMaxFilterNodeIDs    = 1024
	semanticMaxFilterProperties = 32
	semanticMaxFilterKeyBytes   = 256
	semanticMaxFilterValueBytes = 4096
)

func normalizeSemanticFilter(filter *clientv1.SemanticEligibilityFilter) (*clientv1.SemanticEligibilityFilter, error) {
	if filter == nil {
		return nil, nil
	}
	if len(filter.GetNodeLabels()) > semanticMaxFilterLabels || len(filter.GetNodeIds()) > semanticMaxFilterNodeIDs || len(filter.GetProperties()) > semanticMaxFilterProperties {
		return nil, fmt.Errorf("semantic filter exceeds label, node ID, or property limits")
	}
	out := &clientv1.SemanticEligibilityFilter{}
	labelSet := map[string]bool{}
	for _, raw := range filter.GetNodeLabels() {
		label := strings.TrimSpace(raw)
		if label == "" {
			return nil, fmt.Errorf("semantic filter node label is empty")
		}
		labelSet[label] = true
	}
	for label := range labelSet {
		out.NodeLabels = append(out.NodeLabels, label)
	}
	sort.Strings(out.NodeLabels)
	nodeSet := map[string]bool{}
	for _, raw := range filter.GetNodeIds() {
		id, err := uuid.Parse(strings.TrimSpace(raw))
		if err != nil || id == uuid.Nil {
			return nil, fmt.Errorf("semantic filter node ID must be a UUID")
		}
		nodeSet[id.String()] = true
	}
	for id := range nodeSet {
		out.NodeIds = append(out.NodeIds, id)
	}
	sort.Strings(out.NodeIds)
	for _, predicate := range filter.GetProperties() {
		if predicate == nil {
			return nil, fmt.Errorf("semantic filter property predicate is required")
		}
		key := strings.TrimSpace(predicate.GetKey())
		if key == "" || len(key) > semanticMaxFilterKeyBytes || strings.ContainsAny(key, "\x00\r\n") {
			return nil, fmt.Errorf("semantic filter property key is invalid")
		}
		switch predicate.GetOperator() {
		case clientv1.SemanticPropertyOperator_SEMANTIC_PROPERTY_OPERATOR_EQUAL, clientv1.SemanticPropertyOperator_SEMANTIC_PROPERTY_OPERATOR_NOT_EQUAL:
			if predicate.GetValue() == nil {
				return nil, fmt.Errorf("semantic equality filter requires a value")
			}
			if len(predicate.GetValue().String()) > semanticMaxFilterValueBytes {
				return nil, fmt.Errorf("semantic filter property value is too large")
			}
		case clientv1.SemanticPropertyOperator_SEMANTIC_PROPERTY_OPERATOR_EXISTS, clientv1.SemanticPropertyOperator_SEMANTIC_PROPERTY_OPERATOR_MISSING:
			if predicate.GetValue() != nil {
				return nil, fmt.Errorf("semantic exists or missing filter must not include a value")
			}
		default:
			return nil, fmt.Errorf("semantic filter property operator is unsupported")
		}
		var value *structpb.Value
		if predicate.GetValue() != nil {
			value = proto.Clone(predicate.GetValue()).(*structpb.Value)
		}
		out.Properties = append(out.Properties, &clientv1.SemanticPropertyPredicate{Key: key, Operator: predicate.GetOperator(), Value: value})
	}
	sort.SliceStable(out.Properties, func(i, j int) bool {
		if out.Properties[i].GetKey() != out.Properties[j].GetKey() {
			return out.Properties[i].GetKey() < out.Properties[j].GetKey()
		}
		if out.Properties[i].GetOperator() != out.Properties[j].GetOperator() {
			return out.Properties[i].GetOperator() < out.Properties[j].GetOperator()
		}
		return out.Properties[i].GetValue().String() < out.Properties[j].GetValue().String()
	})
	if len(out.NodeLabels) == 0 && len(out.NodeIds) == 0 && len(out.Properties) == 0 {
		return nil, nil
	}
	return out, nil
}

func semanticNodeEligible(node *clientv1.Node, filter *clientv1.SemanticEligibilityFilter) bool {
	if filter == nil {
		return true
	}
	if len(filter.GetNodeIds()) > 0 {
		found := false
		for _, id := range filter.GetNodeIds() {
			if node.GetNodeId() == id {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	labels := map[string]bool{}
	for _, label := range node.GetLabels() {
		labels[label] = true
	}
	for _, label := range filter.GetNodeLabels() {
		if !labels[label] {
			return false
		}
	}
	fields := map[string]*structpb.Value{}
	if node.GetPayload() != nil {
		fields = node.GetPayload().GetFields()
	}
	for _, predicate := range filter.GetProperties() {
		actual, exists := fields[predicate.GetKey()]
		switch predicate.GetOperator() {
		case clientv1.SemanticPropertyOperator_SEMANTIC_PROPERTY_OPERATOR_EQUAL:
			if !exists || !proto.Equal(actual, predicate.GetValue()) {
				return false
			}
		case clientv1.SemanticPropertyOperator_SEMANTIC_PROPERTY_OPERATOR_NOT_EQUAL:
			if !exists || proto.Equal(actual, predicate.GetValue()) {
				return false
			}
		case clientv1.SemanticPropertyOperator_SEMANTIC_PROPERTY_OPERATOR_EXISTS:
			if !exists {
				return false
			}
		case clientv1.SemanticPropertyOperator_SEMANTIC_PROPERTY_OPERATOR_MISSING:
			if exists {
				return false
			}
		default:
			return false
		}
	}
	return true
}
