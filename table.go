package gojx

import (
	"fmt"
	"github.com/Unknwon/com"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
)

// table provides indexes and chunks methods for each schema type value.
type Table struct {
	name      string
	directory string
	s         Mapper
	sc        *Schema

	pkIndex    *Pk
	valueIndex map[string]*Index

	chunk *Chunk
}

// put reflect.Value to table.
func (t *Table) Put(rv reflect.Value, pk int) (e error) {

	// add to chunk
	c, e := t.chunk.Put(strconv.Itoa(pk), rv.Interface())
	if e != nil {
		return
	}
	if e = t.chunk.flushChunk(c); e != nil {
		return
	}

	// add to pk index
	t.pkIndex.Put(pk)
	if e = t.pkIndex.Flush(); e != nil {
		return
	}

	// add values to index
	for name, idx := range t.valueIndex {
		idx.Put(fmt.Sprintf("%v", getReflectFieldValue(rv, name)), pk)
		e = idx.Flush()
		if e != nil {
			return
		}
	}
	return
}

// get interface value by pk int.
func (t *Table) Get(pk int) (i int, v interface{}, e error) {
	i, v, e = t.chunk.Get(strconv.Itoa(pk))
	return
}

// update value by pk.
// it update indexess by reflect.Value and new value's reflect.Value.
// and update chunk value by cursor and pk int.
func (t *Table) Update(pk int, cursor int, rv reflect.Value, nrv reflect.Value) (e error) {
	// write to chunk
	t.chunk.Update(cursor, strconv.Itoa(pk), nrv.Interface())
	if e = t.chunk.flushChunk(cursor); e != nil {
		return
	}

	// update index
	for name, idx := range t.valueIndex {
		oldValue, newValue := getReflectFieldValue(rv, name), getReflectFieldValue(nrv, name)
		if oldValue != newValue {
			oldKey := fmt.Sprintf("%v", oldValue)
			idx.Del(oldKey, pk)
			newKey := fmt.Sprintf("%v", newValue)
			idx.Put(newKey, pk)
			e = idx.Flush()
			if e != nil {
				return
			}
		}
	}

	return
}

// delete value by pk.
// it deletes indexes by reflect.Value.
func (t *Table) Delete(pk int, cursor int, rv reflect.Value) (e error) {
	// delete index
	for name, idx := range t.valueIndex {
		key := fmt.Sprintf("%v", getReflectFieldValue(rv, name))
		idx.Del(key, pk)
		e = idx.Flush()
		if e != nil {
			return
		}
	}

	// delete pk
	t.pkIndex.Del(pk)
	e = t.pkIndex.Flush()
	if e != nil {
		return
	}

	// delete in chunk
	t.chunk.Del(cursor, strconv.Itoa(pk))
	e = t.chunk.flushChunk(cursor)
	return
}

// create or read table in directory with schema.
func NewTable(name string, directory string, sc *Schema, s Mapper) (t *Table, e error) {
	if !com.IsDir(directory) {
		if e = os.MkdirAll(directory, os.ModePerm); e != nil {
			return
		}
	}
	t = new(Table)
	t.name = name
	t.directory = directory
	t.s = s
	t.sc = sc

	// add or read pk index
	t.pkIndex, e = NewPkIndex(path.Join(directory, strings.ToLower(sc.PK+".pk")), s)
	if e != nil {
		return
	}

	// add or read value index
	t.valueIndex = make(map[string]*Index)
	for _, v := range sc.Index {
		t.valueIndex[v], e = NewIndex(strings.ToLower(name+"_"+v), path.Join(directory, strings.ToLower(v+".idx")), s)
		if e != nil {
			return
		}
	}

	// add or read chunk
	t.chunk, e = NewChunk(directory, s, sc.ChunkSize)
	return
}
