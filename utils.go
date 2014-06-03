package gojx

import "reflect"

func getPk(rv reflect.Value, pk string) int64 {
	if rv.Kind() != reflect.Struct {
		return -1
	}
	rf := rv.FieldByName(pk)
	if !rf.IsValid() || rf.Kind() != reflect.Int64 {
		return -1
	}
	return rf.Int()
}
