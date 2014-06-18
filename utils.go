package jx

import "reflect"

// is string,int64 or float64 type.
func isBaseType(k reflect.Kind) bool {
	return k == reflect.String || k == reflect.Int64 || k == reflect.Float64
}

// get reflect type of struct value.
// indirect to pointer inner.
func getReflectType(v interface{}) reflect.Type {
	rt := reflect.TypeOf(v)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	return rt
}
