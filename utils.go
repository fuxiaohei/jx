package gojx

import (
	"fmt"
	"reflect"
	"sort"
	"strings"
)

func getReflectTypeName(rt reflect.Type) string {
	return strings.ToLower(fmt.Sprint(rt))
}

// get reflect type of value.
// if pointer, return inner type.
// only support struct type.
func getReflectType(a interface{}) (rt reflect.Type, name string, e error) {
	rt = reflect.TypeOf(a)
	if rt.Kind() != reflect.Ptr {
		e = fmtError(ErrStrStructNeedPointer, rt)
		return
	}
	rt = rt.Elem()
	name = getReflectTypeName(rt)
	if rt.Kind() != reflect.Struct {
		e = fmtError(ErrStrStructNeedPointer, rt)
		return
	}
	return
}

// get reflect value.
// if pointer, return inner value.
func getReflect(a interface{}) (rv reflect.Value, rt reflect.Type, name string, e error) {
	rv = reflect.ValueOf(a)
	if rv.Kind() != reflect.Ptr {
		e = fmtError(ErrStrStructNeedPointer, rv.Type())
		return
	}
	rv = rv.Elem()
	rt = rv.Type()
	name = strings.ToLower(fmt.Sprint(rt))
	if rt.Kind() != reflect.Struct {
		e = fmtError(ErrStrStructNeedPointer, rt)
		return
	}
	return
}

// get field interface value in reflect value by field name.
func getReflectFieldValue(rv reflect.Value, fieldName string) interface{} {
	rf := rv.FieldByName(fieldName)
	if !rf.IsValid() {
		return nil
	}
	return rf.Interface()
}

// set field value to reflect value.
// make sure the setting value's type is matched to field type in reflect value.
func setReflectField(rv reflect.Value, fieldName string, v interface{}) {
	rf := rv.FieldByName(fieldName)
	if !rf.IsValid() {
		return
	}
	rf.Set(reflect.ValueOf(v))
}

func isInIntSlice(src []int, value int) (i int, b bool) {
	for k, v := range src {
		if v == value {
			i = k
			b = true
			return
		}
	}
	return
}

func isInInterfaceSlice(src []interface{}, value interface{}) (i int, b bool) {
	for k, v := range src {
		if v == value {
			i = k
			b = true
			return
		}
	}
	return
}

func sortIntSliceASC(src []int) {
	sort.Sort(sort.IntSlice(src))
}

func sortIntSliceDESC(src []int) {
	sort.Sort(sort.Reverse(sort.IntSlice(src)))
}
