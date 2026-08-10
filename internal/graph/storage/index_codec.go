package graphstorage

import (
	"encoding/base64"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	graph "github.com/myceldb/mycel/internal/graph/model"
)

const indexKeyEncodingVersion = 1

func EncodeIndexCursor(key string) string { return encodeIndexCursor(key) }

func DecodeIndexCursor(cursor string) (string, error) { return decodeIndexCursor(cursor) }

func EncodeOrderedNodeKey(value any, nodeID graph.NodeID) (string, error) {
	return encodeOrderedNodeKey(value, nodeID)
}

func EncodeSortableValue(value any) (string, error) { return encodeSortableValue(value) }

func encodeIndexCursor(key string) string {
	if key == "" {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString([]byte(key))
}

func decodeIndexCursor(cursor string) (string, error) {
	if cursor == "" {
		return "", nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", ErrUnsupported
	}
	return string(raw), nil
}

func encodeOrderedNodeKey(value any, nodeID graph.NodeID) (string, error) {
	encoded, err := encodeSortableValue(value)
	if err != nil {
		return "", err
	}
	return encoded + "\x00" + nodeID.String(), nil
}

func encodeOrderedEdgeKey(value any, edgeID graph.EdgeID) (string, error) {
	encoded, err := encodeSortableValue(value)
	if err != nil {
		return "", err
	}
	return encoded + "\x00" + edgeID.String(), nil
}

func encodeAdjacencyKey(edge graph.Edge) string {
	order := 0
	if parsed, ok := numberPropInt(edge.Properties["order"]); ok {
		order = parsed
	}
	return encodeSortableInt(int64(order)) + "\x00" + edge.ID.String()
}

func encodeSortableValue(value any) (string, error) {
	switch v := value.(type) {
	case string:
		return "s:" + v, nil
	case bool:
		if v {
			return "b:1", nil
		}
		return "b:0", nil
	case int:
		return encodeSortableInt(int64(v)), nil
	case int8:
		return encodeSortableInt(int64(v)), nil
	case int16:
		return encodeSortableInt(int64(v)), nil
	case int32:
		return encodeSortableInt(int64(v)), nil
	case int64:
		return encodeSortableInt(v), nil
	case uint:
		return encodeSortableUint(uint64(v)), nil
	case uint8:
		return encodeSortableUint(uint64(v)), nil
	case uint16:
		return encodeSortableUint(uint64(v)), nil
	case uint32:
		return encodeSortableUint(uint64(v)), nil
	case uint64:
		return encodeSortableUint(v), nil
	case float32:
		return encodeSortableFloat(float64(v)), nil
	case float64:
		return encodeSortableFloat(v), nil
	case time.Time:
		return "t:" + v.UTC().Format(time.RFC3339Nano), nil
	default:
		return "", ErrUnsupported
	}
}

func encodeSortableInt(value int64) string {
	flipped := uint64(value) ^ (uint64(1) << 63)
	return encodeSortableUint(flipped)
}

func encodeSortableUint(value uint64) string {
	return fmt.Sprintf("u:%020d", value)
}

func encodeSortableFloat(value float64) string {
	bits := math.Float64bits(value)
	if bits&(uint64(1)<<63) != 0 {
		bits = ^bits
	} else {
		bits ^= uint64(1) << 63
	}
	return fmt.Sprintf("f:%016x", bits)
}

func parseNodeIDFromOrderedKey(key string) (graph.NodeID, error) {
	idx := strings.LastIndex(key, "\x00")
	if idx < 0 || idx == len(key)-1 {
		return graph.NodeID{}, ErrUnsupported
	}
	id, err := uuid.Parse(key[idx+1:])
	if err != nil {
		return graph.NodeID{}, ErrUnsupported
	}
	return graph.NodeID(id), nil
}

func parseEdgeIDFromOrderedKey(key string) (graph.EdgeID, error) {
	idx := strings.LastIndex(key, "\x00")
	if idx < 0 || idx == len(key)-1 {
		return graph.EdgeID{}, ErrUnsupported
	}
	id, err := uuid.Parse(key[idx+1:])
	if err != nil {
		return graph.EdgeID{}, ErrUnsupported
	}
	return graph.EdgeID(id), nil
}

func labelCursorKey(id graph.NodeID) string { return id.String() }

func parseLabelCursor(cursor string) (graph.NodeID, error) {
	key, err := decodeIndexCursor(cursor)
	if err != nil || key == "" {
		return graph.NodeID{}, err
	}
	id, err := uuid.Parse(key)
	if err != nil {
		return graph.NodeID{}, ErrUnsupported
	}
	return graph.NodeID(id), nil
}
