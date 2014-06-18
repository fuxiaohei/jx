package jx

import (
	"errors"
	"github.com/Unknwon/com"
	"github.com/fuxiaohei/jx/col"
	"os"
	"path"
	"reflect"
)

var (
	Nil      = errors.New("nil")
	Conflict = errors.New("conflict")
	Wrong    = errors.New("wrong")
)

type Table struct {
	directory string
	Object    *Object

	Chunk *col.Chunk
	Pk    *col.PK
}

// insert value to table.
// save value to chunk and pk.
func (t *Table) Insert(v interface{}) (e error) {
	// set pk value, auto-increment or unique.
	var pk interface{}
	pk, e = t.Pk.SetPk(v, t.Object.Pk)
	if e != nil {
		// use table error, not pk error
		if e == col.PKConflict {
			e = Conflict
		}
		if e == col.PkEmpty {
			e = Wrong
		}
		return
	}

	// write to chunk
	uid, cursor, e := t.Chunk.Write(v)
	if e != nil {
		return
	}

	// write to pk
	e = t.Pk.Write(pk, cursor, uid, 0)
	return
}

// delete value in table.
// delete pk and data in chunk together.
func (t *Table) Delete(v interface{}) (e error) {
	// get pkValue for chunk deleting
	pk := reflect.ValueOf(v).Elem().FieldByName(t.Object.Pk).Interface()
	pkValue, e := t.Pk.Get(pk)
	if e != nil || pkValue == nil {
		return
	}
	// delete in pk first.
	if e = t.Pk.Delete(pk); e != nil {
		return
	}

	// get value, if not found, no need to delete.
	value, e := t.Chunk.Get(pkValue)
	if e != nil || value == nil {
		return
	}
	e = t.Chunk.Delete(pkValue)
	return
}

// update value in table.
// update data and pk together.
func (t *Table) Update(v interface{}) (e error) {
	// get pkValue for chunk updating
	pk := reflect.ValueOf(v).Elem().FieldByName(t.Object.Pk).Interface()
	pkValue, e := t.Pk.Get(pk)
	if e != nil || pkValue == nil {
		return
	}

	// write to data chunk
	e = t.Chunk.Update(v, pkValue)
	if e != nil {
		return
	}

	// write to pk
	e = t.Pk.Update(pk, pkValue)
	return
}

// get value by value pk field.
// it not found, return error Nil.
func (t *Table) Get(v interface{}) (e error) {
	pk := reflect.ValueOf(v).Elem().FieldByName(t.Object.Pk).Interface()
	pkValue, e := t.Pk.Get(pk)
	if e != nil {
		return
	}
	if pkValue == nil {
		e = Nil
		return
	}
	value, e := t.Chunk.Get(pkValue)
	if e != nil {
		return
	}
	if value == nil {
		e = Nil
		return
	}
	// assign to passed value
	reflect.ValueOf(v).Elem().Set(reflect.ValueOf(value).Elem())
	return
}

// init table.
// if first run, create chunk and pk.
// otherwise, read chunk data and pk data.
func (t *Table) init() (e error) {
	if !com.IsDir(t.directory) {
		e = t.firstInit()
		return
	}

	// read pk file
	dir := path.Join(t.directory, "_pk")
	if t.Pk, e = col.NewPk(dir, t.Object.PkAuto); e != nil {
		return
	}

	// read chunk file
	dir = path.Join(t.directory, "_data")
	if t.Chunk, e = col.NewChunk(dir, "data", ".dat", 1000, t.Object.DataType); e != nil {
		return
	}

	// read last chunk as default
	e = t.Chunk.ReadCursorFile(t.Pk.GetLastCursor(), true)

	return
}

// first init for table,
// create data and pk directories and default files.
func (t *Table) firstInit() (e error) {
	// create directory
	e = os.MkdirAll(t.directory, os.ModePerm)
	if e != nil {
		return
	}

	// init data chunk
	dir := path.Join(t.directory, "_data")
	if t.Chunk, e = col.NewChunk(dir, "data", ".dat", 1000, t.Object.DataType); e != nil {
		return
	}

	// init pk
	dir = path.Join(t.directory, "_pk")
	if t.Pk, e = col.NewPk(dir, t.Object.PkAuto); e != nil {
		return
	}

	return
}

// optimize table data.
// chunk and pk are all optimized.
func (t *Table) Optimize() (e error) {
	if e = t.Pk.Optimize(); e != nil {
		return
	}
	e = t.Chunk.Optimize()
	return
}

// create new table in directory with object definition.
func NewTable(directory string, obj *Object) (t *Table, e error) {
	t = &Table{
		directory: directory,
		Object:    obj,
	}
	e = t.init()
	return
}
