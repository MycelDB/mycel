package graphstorage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/google/uuid"
	"martinbeauvais.com/mbgit/knotbase/knotdb/domain/graph"
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
	if node.TemplateID == nil {
		b.WriteByte(0)
	} else {
		b.WriteByte(1)
		writeUUID(&b, *node.TemplateID)
	}
	writeString(&b, node.Content)
	writeTime(&b, node.CreatedAt)
	writeTime(&b, node.UpdatedAt)
	if err := writeMap(&b, node.Props); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func decodeNode(payload []byte) (graph.Node, error) {
	id, templateID, content, r, err := decodeNodePrefix(payload)
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
		if err == nil && r.Len() == 0 {
			return graph.Node{ID: graph.NodeID(id), TemplateID: templateID, Content: content, Props: props, CreatedAt: createdAt, UpdatedAt: updatedAt}, nil
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
	return graph.Node{ID: graph.NodeID(id), TemplateID: templateID, Content: content, Props: props}, nil
}

func decodeNodePrefix(payload []byte) (uuid.UUID, *graph.TemplateID, string, *bytes.Reader, error) {
	r := bytes.NewReader(payload)
	id, err := readUUID(r)
	if err != nil {
		return uuid.Nil, nil, "", nil, err
	}
	hasTemplate, err := r.ReadByte()
	if err != nil {
		return uuid.Nil, nil, "", nil, err
	}
	var templateID *graph.TemplateID
	if hasTemplate == 1 {
		tid, err := readUUID(r)
		if err != nil {
			return uuid.Nil, nil, "", nil, err
		}
		gtid := graph.TemplateID(tid)
		templateID = &gtid
	}
	content, err := readString(r)
	if err != nil {
		return uuid.Nil, nil, "", nil, err
	}
	return id, templateID, content, r, nil
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
	writeUUID(&b, edge.FromID)
	writeUUID(&b, edge.ToID)
	writeString(&b, string(edge.Kind))
	if err := writeMap(&b, edge.Props); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func decodeEdge(payload []byte) (graph.Edge, error) {
	r := bytes.NewReader(payload)
	id, err := readUUID(r)
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
	kind, err := readString(r)
	if err != nil {
		return graph.Edge{}, err
	}
	props, err := readMap(r)
	if err != nil {
		return graph.Edge{}, err
	}
	return graph.Edge{ID: graph.EdgeID(id), FromID: graph.NodeID(from), ToID: graph.NodeID(to), Kind: graph.EdgeKind(kind), Props: props}, nil
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
