package graphstorage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/myceldb/mycel/internal/graph/model"
)

const (
	valueNil uint8 = iota
	valueString
	valueBool
	valueInt
	valueFloat
	valueArray
	valueMap

	maxEncodedMapEntries = 1_000_000
)

func encodeNode(node graph.Node) ([]byte, error) {
	var b bytes.Buffer
	writeUUID(&b, node.ID)
	b.WriteByte(0) // legacy template-id presence flag; always absent after schema migration.
	writeString(&b, node.Content)
	writeTime(&b, node.CreatedAt)
	writeTime(&b, node.UpdatedAt)
	if err := writeMap(&b, node.Props); err != nil {
		return nil, err
	}
	// BlobRef is appended after props so records written before blob support
	// (which simply end at the props map) keep decoding.
	if err := writeBlobRef(&b, node.BlobRef); err != nil {
		return nil, err
	}
	writeUUID(&b, node.DomainID)
	writeStringSlice(&b, node.Labels)
	if err := writeMap(&b, node.Properties); err != nil {
		return nil, err
	}
	if err := writeMap(&b, node.Payload); err != nil {
		return nil, err
	}
	if err := writeMap(&b, node.Meta); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func writeStringSlice(b *bytes.Buffer, values []string) {
	_ = binary.Write(b, binary.BigEndian, uint32(len(values)))
	for _, value := range values {
		writeString(b, value)
	}
}

func readStringSlice(r *bytes.Reader) ([]string, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	out := make([]string, 0, n)
	for i := uint32(0); i < n; i++ {
		value, err := readString(r)
		if err != nil {
			return nil, err
		}
		out = append(out, value)
	}
	return out, nil
}

func writeBlobRef(b *bytes.Buffer, ref *graph.BlobID) error {
	if ref == nil {
		b.WriteByte(0)
		return nil
	}
	raw, err := (*ref).Bytes()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrUnsupported, err)
	}
	b.WriteByte(1)
	b.Write(raw)
	return nil
}

// readBlobRef decodes a trailing optional blob reference. Records written
// before blob support end right after the props map; those decode as nil.
func readBlobRef(r *bytes.Reader) (*graph.BlobID, error) {
	if r.Len() == 0 {
		return nil, nil
	}
	flag, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch flag {
	case 0:
		return nil, nil
	case 1:
		raw := make([]byte, 32)
		if _, err := io.ReadFull(r, raw); err != nil {
			return nil, err
		}
		id, err := graph.BlobIDFromBytes(raw)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrInvalidRecord, err)
		}
		return &id, nil
	default:
		return nil, fmt.Errorf("%w: invalid blob ref flag %d", ErrInvalidRecord, flag)
	}
}

func readDomainID(r *bytes.Reader) (graph.DomainID, error) {
	if r.Len() == 0 {
		return uuid.Nil, nil
	}
	if r.Len() < 16 {
		return uuid.Nil, fmt.Errorf("%w: invalid domain id tail", ErrInvalidRecord)
	}
	id, err := readUUID(r)
	if err != nil {
		return uuid.Nil, err
	}
	return graph.DomainID(id), nil
}

func decodeNode(payload []byte) (graph.Node, error) {
	id, content, r, err := decodeNodePrefix(payload)
	if err != nil {
		return graph.Node{}, err
	}

	propsOffset := len(payload) - r.Len()
	if r.Len() >= 20 && plausibleUnixNanos(peekInt64(payload[propsOffset:])) {
		createdAt, err := readTime(r)
		if err != nil {
			return graph.Node{}, err
		}
		updatedAt, err := readTime(r)
		if err != nil {
			return graph.Node{}, err
		}
		props, err := readMap(r)
		if err == nil {
			blobRef, blobErr := readBlobRef(r)
			domainID, domainErr := readDomainID(r)
			if blobErr == nil && domainErr == nil {
				labels := []string(nil)
				properties := map[string]any(nil)
				payload := map[string]any(nil)
				meta := map[string]any(nil)
				if r.Len() > 0 {
					labels, err = readStringSlice(r)
					if err != nil {
						return graph.Node{}, err
					}
					properties, err = readMap(r)
					if err != nil {
						return graph.Node{}, err
					}
					payload, err = readMap(r)
					if err != nil {
						return graph.Node{}, err
					}
					meta, err = readMap(r)
					if err != nil {
						return graph.Node{}, err
					}
				}
				if r.Len() == 0 {
					return graph.Node{ID: graph.NodeID(id), DomainID: domainID, Labels: labels, Properties: properties, Payload: payload, Meta: meta, BlobRef: blobRef, Content: content, Props: props, CreatedAt: createdAt, UpdatedAt: updatedAt}, nil
				}
			}
		}
	}

	// Legacy node records did not carry timestamps. Keep decoding them so stores
	// written before timestamp support can still be opened and re-imported.
	r = bytes.NewReader(payload[propsOffset:])
	props, err := readMap(r)
	if err != nil {
		return graph.Node{}, err
	}
	if r.Len() != 0 {
		return graph.Node{}, fmt.Errorf("%w: trailing node payload bytes", ErrUnsupported)
	}
	return graph.Node{ID: graph.NodeID(id), Content: content, Props: props}, nil
}

func decodeNodePrefix(payload []byte) (uuid.UUID, string, *bytes.Reader, error) {
	r := bytes.NewReader(payload)
	id, err := readUUID(r)
	if err != nil {
		return uuid.Nil, "", nil, err
	}
	hasLegacyTemplate, err := r.ReadByte()
	if err != nil {
		return uuid.Nil, "", nil, err
	}
	if hasLegacyTemplate == 1 {
		if _, err := readUUID(r); err != nil {
			return uuid.Nil, "", nil, err
		}
	}
	content, err := readString(r)
	if err != nil {
		return uuid.Nil, "", nil, err
	}
	return id, content, r, nil
}

func peekInt64(payload []byte) int64 {
	if len(payload) < 8 {
		return 0
	}
	return int64(binary.BigEndian.Uint64(payload[:8]))
}

func plausibleUnixNanos(nanos int64) bool {
	if nanos == 0 {
		return true
	}
	min := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	max := time.Date(2200, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano()
	return nanos >= min && nanos <= max
}

func encodeEdge(edge graph.Edge) ([]byte, error) {
	var b bytes.Buffer
	writeUUID(&b, edge.ID)
	writeUUID(&b, edge.DomainID)
	writeUUID(&b, edge.FromID)
	writeUUID(&b, edge.ToID)
	writeStringSlice(&b, edge.Labels)
	if err := writeMap(&b, edge.Properties); err != nil {
		return nil, err
	}
	if err := writeMap(&b, edge.Payload); err != nil {
		return nil, err
	}
	if err := writeMap(&b, edge.Meta); err != nil {
		return nil, err
	}
	writeTime(&b, edge.CreatedAt)
	writeTime(&b, edge.UpdatedAt)
	return b.Bytes(), nil
}

func decodeEdge(payload []byte) (graph.Edge, error) {
	r := bytes.NewReader(payload)
	id, err := readUUID(r)
	if err != nil {
		return graph.Edge{}, err
	}
	domainID, err := readUUID(r)
	if err != nil {
		return graph.Edge{}, err
	}
	from, err := readUUID(r)
	if err != nil {
		return graph.Edge{}, err
	}
	to, err := readUUID(r)
	if err != nil {
		return graph.Edge{}, err
	}
	labels, err := readStringSlice(r)
	if err != nil {
		return graph.Edge{}, err
	}
	properties, err := readMap(r)
	if err != nil {
		return graph.Edge{}, err
	}
	edgePayload, err := readMap(r)
	if err != nil {
		return graph.Edge{}, err
	}
	meta, err := readMap(r)
	if err != nil {
		return graph.Edge{}, err
	}
	createdAt, err := readTime(r)
	if err != nil {
		return graph.Edge{}, err
	}
	updatedAt, err := readTime(r)
	if err != nil {
		return graph.Edge{}, err
	}
	return graph.Edge{ID: graph.EdgeID(id), DomainID: graph.DomainID(domainID), FromID: graph.NodeID(from), ToID: graph.NodeID(to), Labels: labels, Properties: properties, Payload: edgePayload, Meta: meta, CreatedAt: createdAt, UpdatedAt: updatedAt}, nil
}

func writeUUID(b *bytes.Buffer, id uuid.UUID) { b.Write(id[:]) }
func readUUID(r *bytes.Reader) (uuid.UUID, error) {
	var id uuid.UUID
	_, err := r.Read(id[:])
	return id, err
}
func writeTime(b *bytes.Buffer, t time.Time) {
	if t.IsZero() {
		binary.Write(b, binary.BigEndian, int64(0))
		return
	}
	binary.Write(b, binary.BigEndian, t.UnixNano())
}

func readTime(r *bytes.Reader) (time.Time, error) {
	var nanos int64
	if err := binary.Read(r, binary.BigEndian, &nanos); err != nil {
		return time.Time{}, err
	}
	if nanos == 0 {
		return time.Time{}, nil
	}
	return time.Unix(0, nanos).UTC(), nil
}

func writeString(b *bytes.Buffer, s string) {
	binary.Write(b, binary.BigEndian, uint32(len(s)))
	b.WriteString(s)
}
func readString(r *bytes.Reader) (string, error) {
	var l uint32
	if err := binary.Read(r, binary.BigEndian, &l); err != nil {
		return "", err
	}
	buf := make([]byte, l)
	_, err := r.Read(buf)
	return string(buf), err
}

func writeMap(b *bytes.Buffer, m map[string]any) error {
	if m == nil {
		m = map[string]any{}
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	binary.Write(b, binary.BigEndian, uint32(len(keys)))
	for _, k := range keys {
		writeString(b, k)
		if err := writeValue(b, m[k]); err != nil {
			return err
		}
	}
	return nil
}
func readMap(r *bytes.Reader) (map[string]any, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}
	if n > maxEncodedMapEntries {
		return nil, fmt.Errorf("%w: encoded map has too many entries: %d", ErrUnsupported, n)
	}
	out := make(map[string]any, n)
	for i := uint32(0); i < n; i++ {
		k, err := readString(r)
		if err != nil {
			return nil, err
		}
		v, err := readValue(r)
		if err != nil {
			return nil, err
		}
		out[k] = v
	}
	return out, nil
}

func writeValue(b *bytes.Buffer, v any) error {
	switch x := v.(type) {
	case nil:
		b.WriteByte(valueNil)
	case string:
		b.WriteByte(valueString)
		writeString(b, x)
	case bool:
		b.WriteByte(valueBool)
		if x {
			b.WriteByte(1)
		} else {
			b.WriteByte(0)
		}
	case int:
		b.WriteByte(valueInt)
		binary.Write(b, binary.BigEndian, int64(x))
	case int8:
		b.WriteByte(valueInt)
		binary.Write(b, binary.BigEndian, int64(x))
	case int16:
		b.WriteByte(valueInt)
		binary.Write(b, binary.BigEndian, int64(x))
	case int32:
		b.WriteByte(valueInt)
		binary.Write(b, binary.BigEndian, int64(x))
	case int64:
		b.WriteByte(valueInt)
		binary.Write(b, binary.BigEndian, x)
	case uint:
		b.WriteByte(valueInt)
		binary.Write(b, binary.BigEndian, int64(x))
	case uint8:
		b.WriteByte(valueInt)
		binary.Write(b, binary.BigEndian, int64(x))
	case uint16:
		b.WriteByte(valueInt)
		binary.Write(b, binary.BigEndian, int64(x))
	case uint32:
		b.WriteByte(valueInt)
		binary.Write(b, binary.BigEndian, int64(x))
	case uint64:
		if x > math.MaxInt64 {
			return fmt.Errorf("%w: uint64 too large", ErrUnsupported)
		}
		b.WriteByte(valueInt)
		binary.Write(b, binary.BigEndian, int64(x))
	case float32:
		b.WriteByte(valueFloat)
		binary.Write(b, binary.BigEndian, float64(x))
	case float64:
		b.WriteByte(valueFloat)
		binary.Write(b, binary.BigEndian, x)
	case []any:
		b.WriteByte(valueArray)
		binary.Write(b, binary.BigEndian, uint32(len(x)))
		for _, item := range x {
			if err := writeValue(b, item); err != nil {
				return err
			}
		}
	case []string:
		b.WriteByte(valueArray)
		binary.Write(b, binary.BigEndian, uint32(len(x)))
		for _, item := range x {
			if err := writeValue(b, item); err != nil {
				return err
			}
		}
	case map[string]any:
		b.WriteByte(valueMap)
		return writeMap(b, x)
	default:
		return fmt.Errorf("%w: %T", ErrUnsupported, v)
	}
	return nil
}

func readValue(r *bytes.Reader) (any, error) {
	kind, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	switch kind {
	case valueNil:
		return nil, nil
	case valueString:
		return readString(r)
	case valueBool:
		b, err := r.ReadByte()
		return b == 1, err
	case valueInt:
		var v int64
		err := binary.Read(r, binary.BigEndian, &v)
		return v, err
	case valueFloat:
		var v float64
		err := binary.Read(r, binary.BigEndian, &v)
		return v, err
	case valueArray:
		var n uint32
		if err := binary.Read(r, binary.BigEndian, &n); err != nil {
			return nil, err
		}
		out := make([]any, 0, n)
		for i := uint32(0); i < n; i++ {
			v, err := readValue(r)
			if err != nil {
				return nil, err
			}
			out = append(out, v)
		}
		return out, nil
	case valueMap:
		return readMap(r)
	default:
		return nil, ErrInvalidRecord
	}
}
