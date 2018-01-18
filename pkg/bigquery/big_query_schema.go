package bigquery

import (
	"encoding/json"
	"io/ioutil"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/viant/toolbox"
	"github.com/viant/toolbox/url"
)

// Field is a custom bigquery schema that is compatible with current Adserver implementation
type Field struct {
	Name   string   `json:"name"`
	Type   string   `json:"type"`
	Fields []*Field `json:"fields,omitempty"`
	Mode   string   `json:"mode,omitempty"`
}

// SchemaFromFile reads schema from a file
func SchemaFromFile(URL string) (bigquery.Schema, error) {
	resource := url.NewResource(URL)
	content, err := resource.Download()
	if err != nil {
		return nil, err
	}
	return SchemaFromRaw(content)
}

// SchemaFromRaw reads schema from raw slice of bytes
func SchemaFromRaw(content []byte) (bigquery.Schema, error) {
	fields := make([]*Field, 0)
	err := json.Unmarshal(content, &fields)
	if err != nil {
		return nil, err
	}
	schema := bigquery.Schema{}

	for _, v := range fields {
		schema = append(schema, convertSchema(v))
	}
	return schema, nil
}

// Convert custom json schema to bigquery compatible schema.
func convertSchema(fieldSchema *Field) *bigquery.FieldSchema {
	newFieldSchema := &bigquery.FieldSchema{}
	newFieldSchema.Name = fieldSchema.Name

	// Set Repeated value if mode is "REPEATED"
	newFieldSchema.Repeated = strings.ToLower(fieldSchema.Mode) == "repeated"

	// FieldType is a native Uppercase string type
	newFieldSchema.Type = (bigquery.FieldType)(strings.ToUpper(fieldSchema.Type))

	if len(fieldSchema.Fields) == 0 {
		return newFieldSchema
	}

	schema := bigquery.Schema{}

	for _, v := range fieldSchema.Fields {
		schema = append(schema, convertSchema(v))
	}
	newFieldSchema.Schema = schema
	return newFieldSchema
}
