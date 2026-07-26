package compile

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"

	graph "github.com/myceldb/mycel/internal/graph/model"
	schema "github.com/myceldb/mycel/internal/schema/model"
)

type CompiledSchema struct {
	Schema                schema.DomainSchema
	Hash                  string
	NodeTypesByName       map[string]*schema.NodeType
	NodeTypesByLabel      map[string][]*schema.NodeType
	NodeTypesByRecordType map[string]*schema.NodeType
	EdgeTypesByLabel      map[string][]*schema.EdgeType
}

func Compile(value schema.DomainSchema) (*CompiledSchema, error) {
	value = value.Normalize()
	if err := schema.Validate(value); err != nil {
		return nil, err
	}
	out := &CompiledSchema{Schema: value, Hash: Hash(value), NodeTypesByName: map[string]*schema.NodeType{}, NodeTypesByLabel: map[string][]*schema.NodeType{}, NodeTypesByRecordType: map[string]*schema.NodeType{}, EdgeTypesByLabel: map[string][]*schema.EdgeType{}}
	for i := range out.Schema.NodeTypes {
		nt := &out.Schema.NodeTypes[i]
		out.NodeTypesByName[nt.Name] = nt
		for _, label := range nt.Labels {
			label = strings.TrimSpace(label)
			if label != "" {
				out.NodeTypesByLabel[label] = append(out.NodeTypesByLabel[label], nt)
			}
		}
		if rt, ok := recordTypeValue(nt.Properties); ok {
			out.NodeTypesByRecordType[rt] = nt
		}
	}
	for i := range out.Schema.EdgeTypes {
		et := &out.Schema.EdgeTypes[i]
		for _, label := range et.Labels {
			label = strings.TrimSpace(label)
			if label != "" {
				out.EdgeTypesByLabel[label] = append(out.EdgeTypesByLabel[label], et)
			}
		}
	}
	return out, nil
}

func Hash(value schema.DomainSchema) string {
	value = value.Normalize()
	data, _ := json.Marshal(value)
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}
func SourceHash(source string) string {
	sum := sha256.Sum256([]byte(source))
	return hex.EncodeToString(sum[:])
}

func recordTypeValue(fields []schema.FieldSpec) (string, bool) {
	for _, f := range fields {
		if f.Name == "record_type" && f.Type == schema.FieldTypeEnum && len(f.EnumValues) == 1 {
			return f.EnumValues[0], true
		}
	}
	return "", false
}

func NodeTypesFor(c *CompiledSchema, node graph.Node) []*schema.NodeType {
	if c == nil {
		return nil
	}
	seen := map[string]bool{}
	var out []*schema.NodeType
	if rt, ok := graph.Property(node, "record_type"); ok {
		if s, ok := rt.(string); ok {
			if nt := c.NodeTypesByRecordType[s]; nt != nil {
				out = append(out, nt)
				seen[nt.Name] = true
			}
		}
	}
	for _, label := range node.Labels {
		for _, nt := range c.NodeTypesByLabel[label] {
			if !seen[nt.Name] {
				out = append(out, nt)
				seen[nt.Name] = true
			}
		}
	}
	return out
}
