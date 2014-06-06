package jx

import (
	"errors"
	"github.com/Unknwon/com"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strings"
)

var (
	NULL     = errors.New("NULL")
	CONFLICT = errors.New("CONFLICT")
)

// StorageConfig is options for storage init
type StorageConfig struct {
	Dir      string
	Encoder  Encoder
	Size     int
	Optimize bool
}

// fill config as default value if empty
func buildStorageConfig(conf StorageConfig) StorageConfig {
	if conf.Dir == "" {
		conf.Dir = "data"
	}
	if conf.Encoder == nil {
		conf.Encoder = new(JsonEncoder)
	}
	if conf.Size < 1 {
		conf.Size = 5000
	}
	return conf
}

// Storage is main saving engine and manages objects, tables and indexes.
type Storage struct {
	directory string
	encoder   Encoder
	size      int

	Objects map[string]*Object
	Tables  map[string]*Table
	Indexes map[string]*Index
}

// get value's reflect data, object and index
func (s *Storage) getValueMeta(v interface{}) (rv reflect.Value, rt reflect.Type, obj *Object, tbl *Table, idx *Index) {
	rv = reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	rt = rv.Type()
	obj = s.Objects[rt.String()]
	tbl = s.Tables[rt.String()]
	idx = s.Indexes[rt.String()]
	return
}

// put value to storage
func (s *Storage) Put(v interface{}) (e error) {
	rv, rt, obj, tbl, idx := s.getValueMeta(v)
	if obj == nil || tbl == nil {
		return errors.New("unknown struct " + rt.String())
	}

	// set pk
	var pk int64
	if pk, e = obj.SetPk(rv); e != nil {
		return
	}

	// put to table
	if e = tbl.Put(pk, v); e != nil {
		return
	}

	// put to index
	e = idx.Put(pk, rv)
	return
}

// set value to storage.
// set existed pk value to new value, means update.
func (s *Storage) Set(v interface{}) (e error) {
	rv, rt, obj, tbl, idx := s.getValueMeta(v)
	if obj == nil || tbl == nil {
		return errors.New("unknown struct " + rt.String())
	}
	pk := getPk(rv, obj.Pk)
	if pk < 1 {
		return NULL
	}

	// get old data
	old, e := tbl.Get(pk)
	if e != nil {
		return
	}
	if old == nil {
		return NULL
	}

	// set to tale
	if e = tbl.Set(pk, v); e != nil {
		return
	}

	// set to index
	e = idx.Set(pk, rv, reflect.ValueOf(old).Elem())
	return
}

// get from storage by object pk
func (s *Storage) Get(v interface{}) (e error) {
	rv, rt, obj, tbl, _ := s.getValueMeta(v)
	if obj == nil || tbl == nil {
		return errors.New("unknown struct " + rt.String())
	}
	pk := getPk(rv, obj.Pk)
	if pk < 1 {
		return NULL
	}

	// get from table
	result, e := tbl.Get(pk)
	if e != nil {
		return
	}
	if result == nil {
		return NULL
	}

	// set to value
	rv.Set(reflect.ValueOf(result).Elem())
	return
}

// delete value by object pk
func (s *Storage) Del(v interface{}) (e error) {
	rv, rt, obj, tbl, idx := s.getValueMeta(v)
	if obj == nil || tbl == nil {
		return errors.New("unknown struct " + rt.String())
	}
	pk := getPk(rv, obj.Pk)
	if pk < 1 {
		return NULL
	}

	// get old value
	old, e := tbl.Get(pk)
	if e != nil {
		return
	}
	if old == nil {
		return NULL
	}

	// delete in table
	if e = tbl.Del(pk); e != nil {
		return
	}

	// delete in index
	e = idx.Del(pk, reflect.ValueOf(old).Elem())
	return
}

func (s *Storage) Sync(value ...interface{}) (e error) {
	for _, v := range value {

		// create object
		obj, e := NewObject(v, s.directory)
		if e != nil {
			return e
		}
		s.Objects[obj.DataType.String()] = obj

		// create table
		dir := strings.ToLower(path.Join(s.directory, obj.DataType.String()))
		s.Tables[obj.DataType.String()], e = NewTable(obj.DataType, dir, s.encoder, obj.Pk, s.size)
		if e != nil {
			return e
		}

		// create index
		s.Indexes[obj.DataType.String()], e = NewIndex(obj.Indexes, dir, s.encoder)
		if e != nil {
			return e
		}
	}
	return
}

// flush all tables.
// write data from system buffer to disk file.
func (s *Storage) Flush() (e error) {
	for _, tbl := range s.Tables {
		if e = tbl.Flush(); e != nil {
			return
		}
	}
	return
}

// optimize all tables and indexes.
// create rebuild files for optimized result.
func (s *Storage) Optimize() (e error) {
	for _, tbl := range s.Tables {
		e = tbl.Rebuild()
		if e != nil {
			return
		}
	}
	for _, idx := range s.Indexes {
		e = idx.Rebuild()
		if e != nil {
			return
		}
	}
	return
}

// apply optimized rebuild files to old data files.
// if rebuild file is newer than original file, replace with newer one.
func (s *Storage) applyOptimized() (e error) {
	e = filepath.Walk(s.directory, func(p string, info os.FileInfo, _ error) error {
		if path.Ext(p) == ".rebuild" {

			// find original file from rebuild file
			dataFile := strings.TrimSuffix(p, ".rebuild")
			if !com.IsFile(dataFile) {
				return nil
			}
			t, _ := com.FileMTime(dataFile)

			// if rebuild file is newer, replace original file
			if t > 0 && info.ModTime().Unix() > t {
				err := os.Remove(dataFile)
				if err != nil {
					return err
				}
				err = os.Rename(p, dataFile)
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
	return
}

// new storage with config.
// if exist, read storage.
func New(conf StorageConfig) (s *Storage, e error) {
	conf = buildStorageConfig(conf)
	// create storage directory
	if !com.IsDir(conf.Dir) {
		if e = os.MkdirAll(conf.Dir, os.ModePerm); e != nil {
			return
		}
	}
	s = &Storage{
		directory: conf.Dir,
		encoder:   conf.Encoder,
		size:      conf.Size,
		Objects:   make(map[string]*Object),
		Tables:    make(map[string]*Table),
		Indexes:   make(map[string]*Index),
	}
	if conf.Optimize {
		e = s.applyOptimized()
	}
	return
}
