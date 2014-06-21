package jx

import (
	"fmt"
	"github.com/Unknwon/com"
	"math/rand"
	"os"
	"path"
	"reflect"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

type Storage struct {
	directory string

	tables map[reflect.Type]*Table
}

// get struct table.
// the struct must be synced.
func (s *Storage) Table(v interface{}) *Table {
	return s.tables[getReflectType(v)]
}

// get all tables in storage.
// the tables are followed by synced struct objects.
func (s *Storage) Tables() map[string]*Table {
	data := make(map[string]*Table)
	for rt, tbl := range s.tables {
		data[rt.String()] = tbl
	}
	return data
}

// insert new struct value.
// it must be synced struct.
func (s *Storage) Insert(v interface{}) (e error) {
	rt := getReflectType(v)
	tbl := s.tables[rt]
	if tbl == nil {
		e = fmt.Errorf("no sync struct : %s", rt.String())
		return
	}
	e = tbl.Insert(v)
	return
}

// get struct value by its pk field value.
func (s *Storage) Get(v interface{}) (e error) {
	rt := getReflectType(v)
	tbl := s.tables[rt]
	if tbl == nil {
		e = fmt.Errorf("no sync struct : %s", rt.String())
		return
	}
	e = tbl.Get(v)
	return
}

// delete struct value by its pk field.
func (s *Storage) Delete(v interface{}) (e error) {
	rt := getReflectType(v)
	tbl := s.tables[rt]
	if tbl == nil {
		e = fmt.Errorf("no sync struct : %s", rt.String())
		return
	}
	e = tbl.Delete(v)
	return
}

// update struct value by its pk value
func (s *Storage) Update(v interface{}) (e error) {
	rt := getReflectType(v)
	tbl := s.tables[rt]
	if tbl == nil {
		e = fmt.Errorf("no sync struct : %s", rt.String())
		return
	}
	e = tbl.Update(v)
	return
}

// sync struct pointer to create table.
// it parses struct field to create or read table data.
func (s *Storage) Sync(value ...interface{}) (e error) {
	for _, v := range value {
		var obj *Object
		obj, e = NewObject(v)
		if e != nil {
			return
		}
		s.tables[obj.DataType], e = NewTable(path.Join(s.directory, obj.DataType.String()), obj)
		if e != nil {
			return
		}
	}
	return
}

// optimize storage data.
// clean deleted data and pk.
func (s *Storage) Optimize() (e error) {
	for _, tbl := range s.tables {
		if e = tbl.Optimize(); e != nil {
			return
		}
	}
	return
}

// create storage in directory.
// it doesn't load data,
// util call Sync(...) to load data.
func NewStorage(directory string) (s *Storage, e error) {
	if !com.IsDir(directory) {
		if e = os.MkdirAll(directory, os.ModePerm); e != nil {
			return
		}
	}
	s = &Storage{
		directory: directory,
		tables:    make(map[reflect.Type]*Table),
	}
	return
}
