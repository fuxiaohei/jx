package gojx

import (
	"errors"
	"fmt"
	"github.com/Unknwon/com"
	"path"
	"reflect"
	"strconv"
	"strings"
)

// Object is struct definition for storage.
// it saves object's pk, index and increment data.
type Object struct {
	DataType  reflect.Type
	Pk        string
	Indexes   map[string]reflect.Type
	Increment int64
	objFile   string
}

// SetPk to value.
// it pk is over Object.Increment use pk.
// or use Object.Increment.
func (o *Object) SetPk(v reflect.Value) (pk int64, e error) {
	pk = getPk(v, o.Pk)
	if pk <= o.Increment {
		o.Increment++
		pk = o.Increment
		v.FieldByName(o.Pk).SetInt(pk)
	} else {
		o.Increment = pk
	}
	return pk, o.writeIncrement()
}

// parse value to object fields
func (o *Object) parseData(v interface{}) (e error) {
	rt := reflect.TypeOf(v)
	if rt.Kind() == reflect.Ptr {
		rt = rt.Elem()
	}
	if rt.Kind() != reflect.Struct {
		e = errors.New(rt.String() + " need be struct")
		return
	}
	numField := rt.NumField()
	for i := 0; i < numField; i++ {
		rf := rt.Field(i)
		tag := rf.Tag.Get("gojx")
		if len(tag) < 1 || tag == "-" {
			continue
		}
		if tag == "pk" && rf.Type.Kind() == reflect.Int64 {
			o.Pk = rf.Name
			continue
		}
		if tag == "index" {
			o.Indexes[rf.Name] = rf.Type
			continue
		}
	}
	if len(o.Pk) < 1 {
		e = errors.New(rt.String() + " need pk int64 field")
		return
	}
	o.DataType = rt
	return
}

// write increment to file
func (o *Object) writeIncrement() (e error) {
	_, e = com.SaveFileS(o.objFile, fmt.Sprint(o.Increment))
	return
}

// read increment from file
func (o *Object) readIncrement() (e error) {
	str, e := com.ReadFileS(o.objFile)
	if e != nil {
		return
	}
	o.Increment, _ = strconv.ParseInt(str, 10, 64)
	return
}

// create new object from value and directory.
func NewObject(v interface{}, directory string) (o *Object, e error) {
	o = &Object{
		Indexes: make(map[string]reflect.Type),
	}
	if e = o.parseData(v); e != nil {
		return
	}
	o.objFile = path.Join(directory, strings.ToLower(o.DataType.String()+".obj"))
	if com.IsFile(o.objFile) {
		e = o.readIncrement()
		return
	}
	o.Increment = 0
	e = o.writeIncrement()
	return
}
