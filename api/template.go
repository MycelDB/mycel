package api

type Template struct {
	Key         string
	DisplayName string
	NodeType    string
	Fields      []TemplateField
	System      bool
	Version     int
}

type TemplateField struct {
	Name     string
	Type     string
	Required bool
	Default  any
}
