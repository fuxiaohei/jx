package gojx

import (
	"reflect"
)

type Schema struct {
	PK          string   `json:"pk"`
	StringIndex []string `json:"strings"`
	IntIndex    []string `json:"ints"`
	MaxId       int      `json:"max"`
	ChunkSize   int      `json:"chuck_size"`
	name        string
	file        string
}

// Write schema to file.
// When schema creating, the file path is assigned in Schema.file.
func (s *Schema) Write() error {
	if s.file == "" {
		return fmtError(ErrSchemaWriteNoFile, s.name)
	}
	return toJsonFile(s.file, s)
}

// Create new schema from reflect.Type.
// It parsed struct tag of `gojx:"**"`.
// No Schema support int pk, int and string index types.
func NewSchema(rt reflect.Type) (*Schema, error) {
	numField := rt.NumField()
	if numField < 1 {
		return nil, fmtError(ErrSchemaNeedField, rt)
	}
	schema := &Schema{PK: "",
		StringIndex: make([]string, 0),
		IntIndex:    make([]string, 0),
		MaxId:       0,
		ChunkSize:   100,
	}
	for i := 0; i < numField; i++ {
		field := rt.Field(i)
		tag := field.Tag.Get("gojx")
		if tag == "" || tag == "-" {
			continue
		}
		if tag == "pk" {
			if field.Type.Kind() != reflect.Int {
				return nil, fmtError(ErrSchemaPKNeedInt, rt, field.Name)
			}
			schema.PK = field.Name
			continue
		}
		if tag == "index" {
			if field.Type.Kind() == reflect.String {
				schema.StringIndex = append(schema.StringIndex, field.Name)
				continue
			}
			if field.Type.Kind() == reflect.Int {
				schema.IntIndex = append(schema.IntIndex, field.Name)
				continue
			}
			return nil, fmtError(ErrSchemaIndexTypeError, rt, field.Name)
		}
	}
	return schema, nil
}
