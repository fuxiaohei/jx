package gojx

import (
	"github.com/Unknwon/com"
	"os"
	"path"
	"reflect"
)

type Storage struct {
	directory string

	schema     map[string]*Schema
	schemaFile string

	table map[string]*Table

	saver Mapper
}

func (s *Storage) bootstrap() (e error) {
	if com.IsFile(s.schemaFile) {
		// load schema first
		e = s.saver.FromFile(s.schemaFile, &s.schema)
		if e != nil {
			return
		}

		// load table
		s.table = make(map[string]*Table)
		for k, sc := range s.schema {
			s.table[k], e = NewTable(k, path.Join(s.directory, k), sc, s.saver)
			if e != nil {
				return
			}
		}
	} else {
		s.schema = make(map[string]*Schema)
		s.table = make(map[string]*Table)
	}
	return
}

func (s *Storage) flushSchema() error {
	return s.saver.ToFile(s.schemaFile, s.schema)
}

func (s *Storage) Register(v interface{}, size int) (e error) {
	rt, name, e := getReflectType(v)
	// check schema existing
	if _, ok := s.schema[name]; ok {
		return
	}
	if e != nil {
		return e
	}
	// create schema
	s.schema[name], e = NewSchema(rt, size)
	if e != nil {
		return e
	}
	// create table
	s.table[name], e = NewTable(name, path.Join(s.directory, name), s.schema[name], s.saver)
	if e != nil {
		return e
	}
	return s.flushSchema()
}

func (s *Storage) Put(v interface{}) (e error) {
	rv, _, name, e := getReflect(v)
	if e != nil {
		return
	}
	sc := s.schema[name]
	if sc == nil {
		return fmtError(ErrStrSchemaUnknown, name)
	}
	pk := s.setPk(rv, sc)
	e = s.table[name].Put(rv, pk)
	if e != nil {
		return
	}
	return s.flushSchema()
}

func (s *Storage) Get(v interface{}) (e error) {
	rv, _, name, e := getReflect(v)
	if e != nil {
		return
	}
	sc := s.schema[name]
	if sc == nil {
		return fmtError(ErrStrSchemaUnknown, name)
	}
	pk := getReflectFieldValue(rv, sc.PK).(int)
	if pk < 1 {
		return fmtError(ErrStrStructPkZero, name)
	}
	_, res, e := s.table[name].Get(pk)
	if e != nil {
		return
	}
	if res == nil {
		return ErrorNoData
	}
	e = s.saver.ToStruct(res, v)
	return
}

func (s *Storage) getValue(pk int, name string, rt reflect.Type) (i int, rv reflect.Value, e error) {
	rv = reflect.New(rt)
	i, res, e := s.table[name].Get(pk)
	if e != nil {
		return
	}
	if res == nil {
		e = ErrorNoData
		return
	}
	e = s.saver.ToStruct(res, rv.Interface())
	return
}

func (s *Storage) Update(v interface{}) (e error) {
	// check value type
	rv, _, name, e := getReflect(v)
	if e != nil {
		return
	}
	sc := s.schema[name]
	if sc == nil {
		return fmtError(ErrStrSchemaUnknown, name)
	}
	pk := getReflectFieldValue(rv, sc.PK).(int)
	if pk < 1 {
		return fmtError(ErrStrStructPkZero, name)
	}

	// get old value
	i, oldV, e := s.getValue(pk, name, rv.Type())
	if e != nil {
		return
	}
	e = s.table[name].Update(pk, i, oldV.Elem(), rv)
	return
}

func (s *Storage) Delete(v interface{}) (e error) {
	// check value type
	rv, _, name, e := getReflect(v)
	if e != nil {
		return
	}
	sc := s.schema[name]
	if sc == nil {
		return fmtError(ErrStrSchemaUnknown, name)
	}
	pk := getReflectFieldValue(rv, sc.PK).(int)
	if pk < 1 {
		return fmtError(ErrStrStructPkZero, name)
	}

	i, nrv, e := s.getValue(pk, name, rv.Type())
	if e != nil {
		return
	}
	if nrv.IsNil() {
		return ErrorNoData
	}
	e = s.table[name].Delete(pk, i, nrv.Elem())
	return
}

func (s *Storage) setPk(rv reflect.Value, sc *Schema) int {
	pk := getReflectFieldValue(rv, sc.PK).(int)
	if pk > sc.Max {
		sc.Max = pk
		return pk
	}
	sc.Max++
	pk = sc.Max
	setReflectField(rv, sc.PK, pk)
	return pk
}

func NewStorage(directory string, saver string) (s *Storage, e error) {
	if !com.IsDir(directory) {
		e = os.MkdirAll(directory, os.ModePerm)
		if e != nil {
			return
		}
	}
	if _, ok := mapperManager[saver]; !ok {
		e = fmtError(ErrStrSaverUnknown, saver)
		return
	}
	s = new(Storage)
	s.directory = directory
	s.schemaFile = path.Join(s.directory, "schema.scm")
	s.saver = mapperManager[saver]
	if e = s.bootstrap(); e != nil {
		return
	}
	return
}
