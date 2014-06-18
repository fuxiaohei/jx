package jx

import (
	"fmt"
	"reflect"
)

type Object struct {
	DataType reflect.Type

	Pk     string
	PkType reflect.Type
	PkAuto bool

	Index map[string]reflect.Type
}

// create new object from value.
// pk field need int64,float64 or string.
// auto pk field need int64.
// pk field must be set.
func NewObject(v interface{}) (obj *Object, e error) {
	// parse value reflect type.
	// need struct pointer.
	rt := reflect.TypeOf(v)
	if rt.Kind() != reflect.Ptr || rt.Elem().Kind() != reflect.Struct {
		e = fmt.Errorf("object need struct pointer : %s", rt.String())
		return

	}
	rt = rt.Elem()

	obj = &Object{
		DataType: rt,
		Index:    make(map[string]reflect.Type),
	}

	num := rt.NumField()
	// parse field
	for i := 0; i < num; i++ {
		field := rt.Field(i)
		tag := field.Tag.Get("jx")

		// pk
		if tag == "pk" {
			if !isBaseType(field.Type.Kind()) {
				e = fmt.Errorf("pk field need string, int64 or float64 : %s,%s", rt.String(), field.Name)
				return
			}
			obj.Pk = field.Name
			obj.PkType = field.Type
			obj.PkAuto = false
			continue
		}

		// auto pk
		if tag == "pk-auto" {
			if field.Type.Kind() != reflect.Int64 {
				e = fmt.Errorf("auto pk field need int64 : %s,%s", rt.String(), field.Name)
				return
			}
			obj.Pk = field.Name
			obj.PkType = field.Type
			obj.PkAuto = true
			continue
		}

		// index
		if tag == "index" {
			obj.Index[field.Name] = field.Type
			continue
		}

	}
	if len(obj.Pk) < 1 {
		e = fmt.Errorf("need pk field : %s", rt.String())
	}
	return
}
