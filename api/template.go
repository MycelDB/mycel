package api

// Template defines a reusable node shape.
//
// Templates are optional and provide a schema-like contract for node props.
type Template struct {
	ID          TemplateID
	DisplayName string
	NodeType    string
	Fields      []TemplateField
	System      bool
	Version     int
}

// TemplateField defines a single template property.
type TemplateField struct {
	Name     string
	Type     string
	Required bool
	Default  any
}
