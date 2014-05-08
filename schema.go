package gojx

import "reflect"

type Schema struct {
	Name  string   `json:"name"`
	PK    string   `json:"pk"`
	Index []string `json:"index"`
	Max   int      `json:"max"`
	file  string
}

// create new schema with type
func NewSchema(rt reflect.Type) (sc *Schema, e error) {
	numField := rt.NumField()
	if numField < 1 {
		e = ErrorNeedField
		return
	}
	sc = new(Schema)
	sc.Name = rt.Name()
	sc.Max = 0
	sc.Index = []string{}

	for i := 0; i < numField; i++ {
		field := rt.Field(i)
		tag := field.Tag.Get("jx")
		if tag == "" || tag == "-" {
			continue
		}
		if tag == "pk" {
			if field.Type.Kind() != reflect.Int {
				e = ErrorNeedPKInt
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
