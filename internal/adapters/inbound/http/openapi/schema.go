// Package openapi generates the API's OpenAPI document from the registered
// routes and the handler payload types. The spec is derived, never
// hand-written: a hand-written one drifts from the routes the moment anybody
// adds a field.
package openapi

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Schema is the subset of JSON Schema the document uses.
type Schema struct {
	Type                 string             `json:"type,omitempty"`
	Format               string             `json:"format,omitempty"`
	Description          string             `json:"description,omitempty"`
	Properties           map[string]*Schema `json:"properties,omitempty"`
	Required             []string           `json:"required,omitempty"`
	Items                *Schema            `json:"items,omitempty"`
	AdditionalProperties *Schema            `json:"additionalProperties,omitempty"`
	Nullable             bool               `json:"nullable,omitempty"`
	Enum                 []string           `json:"enum,omitempty"`
	Minimum              *float64           `json:"minimum,omitempty"`
	Maximum              *float64           `json:"maximum,omitempty"`
	MinLength            *int               `json:"minLength,omitempty"`
	MaxLength            *int               `json:"maxLength,omitempty"`
	Ref                  string             `json:"$ref,omitempty"`
}

var (
	timeType    = reflect.TypeOf(time.Time{})
	rawJSONType = reflect.TypeOf(json.RawMessage{})
)

// SchemaFor reflects a Go type into a schema. Validator tags become
// constraints, so the published contract matches what the binder actually
// enforces rather than what somebody remembered to document.
func SchemaFor(t reflect.Type) *Schema {
	if t == nil {
		return &Schema{}
	}

	switch t {
	case timeType:
		return &Schema{Type: "string", Format: "date-time"}
	case rawJSONType:
		return &Schema{Description: "Arbitrary JSON"}
	}

	switch t.Kind() {
	case reflect.Pointer:
		schema := SchemaFor(t.Elem())
		schema.Nullable = true
		return schema

	case reflect.Bool:
		return &Schema{Type: "boolean"}

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return &Schema{Type: "integer"}

	case reflect.Float32, reflect.Float64:
		return &Schema{Type: "number"}

	case reflect.String:
		return &Schema{Type: "string"}

	case reflect.Slice, reflect.Array:
		if t.Elem().Kind() == reflect.Uint8 {
			// A byte slice on the wire is base64 in JSON.
			return &Schema{Type: "string", Format: "byte"}
		}
		return &Schema{Type: "array", Items: SchemaFor(t.Elem())}

	case reflect.Map:
		return &Schema{Type: "object", AdditionalProperties: SchemaFor(t.Elem())}

	case reflect.Struct:
		return structSchema(t)

	default:
		return &Schema{}
	}
}

func structSchema(t reflect.Type) *Schema {
	schema := &Schema{Type: "object", Properties: map[string]*Schema{}}

	for i := range t.NumField() {
		field := t.Field(i)
		if !field.IsExported() {
			continue
		}

		// An embedded struct's fields are inlined, which is how the created
		// response reuses the plain one.
		if field.Anonymous && field.Type.Kind() == reflect.Struct {
			embedded := structSchema(field.Type)
			for name, property := range embedded.Properties {
				schema.Properties[name] = property
			}
			schema.Required = append(schema.Required, embedded.Required...)
			continue
		}

		name, omit := jsonFieldName(field)
		if omit {
			continue
		}

		property := SchemaFor(field.Type)
		applyValidation(property, field.Tag.Get("validate"))
		schema.Properties[name] = property

		if isRequired(field.Tag.Get("validate")) {
			schema.Required = append(schema.Required, name)
		}
	}

	if len(schema.Properties) == 0 {
		schema.Properties = nil
	}
	return schema
}

// jsonFieldName resolves a field's wire name, reporting fields the encoder
// skips.
func jsonFieldName(field reflect.StructField) (name string, omit bool) {
	tag := field.Tag.Get("json")
	if tag == "-" {
		return "", true
	}
	name, _, _ = strings.Cut(tag, ",")
	if name == "" {
		name = field.Name
	}
	return name, false
}

func isRequired(validate string) bool {
	for _, rule := range strings.Split(validate, ",") {
		if strings.TrimSpace(rule) == "required" {
			return true
		}
	}
	return false
}

// applyValidation turns the validator tags the binder enforces into schema
// constraints.
func applyValidation(schema *Schema, validate string) {
	for _, rule := range strings.Split(validate, ",") {
		name, value, _ := strings.Cut(strings.TrimSpace(rule), "=")
		switch name {
		case "min":
			if schema.Type == "string" {
				if parsed, err := strconv.Atoi(value); err == nil {
					schema.MinLength = &parsed
				}
				continue
			}
			if parsed, err := strconv.ParseFloat(value, 64); err == nil {
				schema.Minimum = &parsed
			}
		case "max":
			if schema.Type == "string" {
				if parsed, err := strconv.Atoi(value); err == nil {
					schema.MaxLength = &parsed
				}
				continue
			}
			if parsed, err := strconv.ParseFloat(value, 64); err == nil {
				schema.Maximum = &parsed
			}
		case "oneof":
			schema.Enum = strings.Fields(value)
		case "url":
			schema.Format = "uri"
		case "email":
			schema.Format = "email"
		}
	}
}
