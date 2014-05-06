package gojx

import (
	"github.com/Unknwon/com"
	"os"
	"path"
	"reflect"
	"strconv"
)

type Storage struct {
	dir      string
	typeData map[string]reflect.Type

	schemaData map[string]*Schema
	schemaFile string

	indexData map[string]*Index

	chunk *Chunk
}

func (s *Storage) Put(value ...interface{}) error {
	names := []string{}
	for _, a := range value {
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
