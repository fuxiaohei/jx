package gojx

import (
	"github.com/Unknwon/com"
	"os"
	"path"
	"reflect"
	"strconv"
)

// storage struct manages all types, schema objects, index objects and chunks.
type Storage struct {
	dir      string
	typeData map[string]reflect.Type

	schemaData map[string]*Schema
	schemaFile string

	indexData map[string]*Index

	chunk *Chunk
}

// put values to storage.
// if the value's type is not registered, stop and return error.
// this method will write all changes to file directly.
func (s *Storage) Put(value ...interface{}) error {
	names := []string{}
	for _, a := range value {
		// get type
		rt, err := getStructPointer(a)
		if err != nil {
			return err
		}

		name := rt.Name()
		if !inStringSlice(names, name) {
			names = append(names, name)
		}

		// get index
		idx, ok := s.indexData[name]
		if !ok {
			return fmtError(ErrPutMissingSchema, rt)
		}

		// struct to map
		data, err := struct2map(a)
		if err != nil {
			return err
		}

		// write to chunk
		s.schemaData[name].Max++
		data[s.schemaData[name].PK] = s.schemaData[name].Max
		err = s.chunk.Put(name+strconv.Itoa(s.schemaData[name].Max), data)
		if err != nil {
			return err
		}

		// put into index
		idx.Put(data, s.schemaData[name].Max)

		// update raw data
		err = map2struct(data, a)
		if err != nil {
			return err
		}
	}

	// flush chunk
	err := s.chunk.FlushCurrent()
	if err != nil {
		return err
	}

	// flush idx
	for _, n := range names {
		err = s.indexData[n].Flush(s)
		if err != nil {
			return err
		}
	}

	// update schema
	err = toJsonFile(s.schemaFile, s.schemaData)
	if err != nil {
		return err
	}

	return nil
}

// get data by pk value.
// if found, assign data to passed interface value.
func (s *Storage) Get(a interface{}) error {
	rt, err := getStructPointer(a)
	if err != nil {
		return err
	}
	name := rt.Name()

	if _, ok := s.schemaData[name]; !ok {
		return fmtError(ErrGetMissingSchema, rt)
	}

	data, err := struct2map(a)
	if err != nil {
		return err
	}

	pk := int(data[s.schemaData[name].PK].(float64))
	if pk < 1 {
		return fmtError(ErrGetPKInvalid, rt, pk)
	}

	_, result, err := s.chunk.Get(name + strconv.Itoa(pk))
	if err != nil {
		return err
	}
	if result == nil {
		return ErrorNoData
	}
	tmp, err := struct2map(result)
	if err != nil {
		return err
	}

	return map2struct(tmp, a)
}

// register type to storage.
// struct type will be used for schema.
// if registered, it will be overwritten.
func (s *Storage) Register(data ...interface{}) error {
	for _, a := range data {
		rt, err := getStructPointer(a)
		if err != nil {
			return err
		}
		s.typeData[rt.Name()] = rt
		s.schemaData[rt.Name()], err = NewSchema(rt)
		if err != nil {
			return err
		}
	}
	err := toJsonFile(s.schemaFile, s.schemaData)
	if err != nil {
		return err
	}
	return s.bootstrap()
}

// check schema objects loaded.
func (s *Storage) IsLoadSchema() bool {
	return len(s.schemaData) > 0
}

func (s *Storage) bootstrap() error {
	// read or create index
	for name, schema := range s.schemaData {
		if _, ok := s.indexData[name]; ok {
			continue
		}
		idx, err := NewIndex(schema, s)
		if err != nil {
			return err
		}
		s.indexData[name] = idx
	}

	// read chunk
	if s.chunk == nil {
		var err error
		if com.IsFile(path.Join(s.dir, "data1.dat")) {
			s.chunk, err = ReadChunk(s.dir, CHUNK_SIZE)
			if err != nil {
				return err
			}
		} else {
			s.chunk, err = NewChunk(s.dir, CHUNK_SIZE)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// create new storage in directory.
// it loads schemas, indexes and chunks.
func NewStorage(dir string) (s *Storage, err error) {
	if !com.IsDir(dir) {
		err = os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return
		}
	}
	s = &Storage{dir,
		make(map[string]reflect.Type),
		make(map[string]*Schema),
		path.Join(dir, "schema.scm"),
		make(map[string]*Index),
		nil,
	}
	fromJsonFile(s.schemaFile, &s.schemaData)
	if s.IsLoadSchema() {
		err = s.bootstrap()
	}
	return
}
