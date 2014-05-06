package gojx

import "reflect"

type Schema struct {
	Name        string   `json:"name"`
	PK          string   `json:"pk"`
	StringIndex []string `json:"string"`
	IntIndex    []string `json:"int"`
	Max         int      `json:"max"`
	ChunkSize   int      `json:"chunk_size"`
	file        string
}

func NewSchema(rt reflect.Type) (s *Schema, err error) {
	numField := rt.NumField()
	if numField < 1 {
		return nil, fmtError(ErrSchemaNeedField, rt)
	}
	schema := &Schema{
		Name:        rt.Name(),
		PK:          "",
		StringIndex: make([]string, 0),
		IntIndex:    make([]string, 0),
		Max:         0,
		ChunkSize:   CHUNK_SIZE,
	}
	for i := 0; i < numField; i++ {
		field := rt.Field(i)
		tag := field.Tag.Get("jx")
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
