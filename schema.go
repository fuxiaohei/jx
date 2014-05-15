package gojx

import "reflect"

type Schema struct {
	Name      string
	PK        string
	Index     []string
	Max       int
	ChunkSize int
	file      string
}

// create new schema with type
func NewSchema(rt reflect.Type, size int) (sc *Schema, e error) {
	numField := rt.NumField()
	if numField < 1 {
		e = fmtError(ErrStrStructNeedField, rt)
		return
	}
	sc = new(Schema)
	sc.Name = rt.Name()
	sc.Max = 0
	sc.Index = []string{}
	sc.ChunkSize = size

	// parse fields
	for i := 0; i < numField; i++ {
		field := rt.Field(i)
		tag := field.Tag.Get("jx")
		if tag == "" || tag == "-" {
			continue
		}
		if tag == "pk" {
			if field.Type.Kind() != reflect.Int {
				e = fmtError(ErrStrStructPkNeedInt, rt, field.Name)
				return
			}
			sc.PK = field.Name
			continue
		}
		if tag == "index" {
			sc.Index = append(sc.Index, field.Name)
		}
	}
	return
}
