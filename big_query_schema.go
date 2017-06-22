package etly

import (
	"encoding/json"
	"io/ioutil"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/viant/toolbox"
)

// BigqueryField is a custom bigquery schema that is compatible with current Adserver implementation
type BigqueryField struct {
	Name   string           `json:"name"`
	Type   string           `json:"type"`
	Fields []*BigqueryField `json:"fields,omitempty"`
	Mode   string           `json:"mode,omitempty"`
}

// SchemaFromFile reads schema from a file
func SchemaFromFile(URL string) (bigquery.Schema, error) {
	reader, _, err := toolbox.OpenReaderFromURL(URL)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	return SchemaFromRaw(content)
}

// SchemaFromRaw reads schema from raw slice of bytes
func SchemaFromRaw(content []byte) (bigquery.Schema, error) {
	fields := make([]*BigqueryField, 0)
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
func convertSchema(fieldSchema *BigqueryField) *bigquery.FieldSchema {
	newFieldSchema := &bigquery.FieldSchema{}
	newFieldSchema.Name = fieldSchema.Name

	// Set Repeated value if type is "REPEATED"
	newFieldSchema.Repeated = strings.ToUpper(fieldSchema.Type) == string(bigquery.RecordFieldType)

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
