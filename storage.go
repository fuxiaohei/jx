package gojx

import (
	"github.com/Unknwon/com"
	"os"
	"path"
	"sort"
	"strconv"
)

type Storage struct {
	dir string

	schemaData map[string]*Schema
	schemaFile string

	chunk *Chunk
	index *Index
}

// get storage directory.
func (s *Storage) Dir() string {
	return s.dir
}

func (s *Storage) parseValue(value interface{}) (sc *Schema, data map[string]interface{}, e error) {
	rt, e := getReflectType(value)
	if e != nil {
		return
	}
	name := rt.Name()
	sc, ok := s.schemaData[name]
	if !ok {
		e = fmtError(ErrPutNoSchema, rt)
	}

	data, e = struct2map(value)
	return
}

// put data into storage.
// if set pk value and over current max pk, use pk in data then auto increase.
func (s *Storage) Put(value interface{}) error {
	sc, data, e := s.parseValue(value)
	if e != nil {
		return e
	}

	// set or get pk value
	pk := getMapPk(data, sc.PK)
	if pk > sc.Max {
		sc.Max = pk
	} else {
		sc.Max++
		data[sc.PK] = sc.Max
		pk = sc.Max
		e2 := map2struct(data, value)
		if e2 != nil {
			return e2
		}
	}

	// write chunk
	e = s.chunk.Put(data, sc.Name+strconv.Itoa(pk))
	if e != nil {
		return e
	}
	e = s.chunk.FlushCurrent()
	if e != nil {
		return e
	}

	// write index
	e = s.index.Put(sc, data, pk)
	if e != nil {
		return e
	}
	e = s.index.FlushCurrent()
	if e != nil {
		return e
	}

	return toJsonFile(s.schemaFile, s.schemaData)
}

// get data by pk int value.
func (s *Storage) getByPk(sc *Schema, name string, value interface{}, pk int) error {
	if pk < 1 {
		return ErrorGetNoPk
	}
	_, result, e := s.chunk.Get(name + strconv.Itoa(pk))
	if e != nil {
		return e
	}
	if result != nil {
		e = map2struct(result.(map[string]interface{}), value)
		if e != nil {
			return e
		}
	} else {
		map2struct(map[string]interface{}{sc.PK: 0}, value)
		return ErrorNoData
	}
	return nil
}

// get data by pk value.
// if no data, value is assigned to empty data.
func (s *Storage) Get(value interface{}) error {
	sc, data, e := s.parseValue(value)
	if e != nil {
		return e
	}

	// set or get pk value
	pk := getMapPk(data, sc.PK)
	return s.getByPk(sc, sc.Name, value, pk)
}

// get data by index value.
func (s *Storage) getByIndex(sc *Schema, value interface{}, name string, index string, indexValue interface{}, isMax bool) error {

	// find index result
	_, rawResult := s.index.Get(name, index, indexValue)
	if len(rawResult) < 1 {
		return nil
	}
	result, ok := s.index.toIntSlice(rawResult)
	if !ok {
		return nil
	}
	// sort index
	if isMax {
		sort.Sort(sort.Reverse(sort.IntSlice(result)))
	} else {
		sort.Sort(sort.IntSlice(result))
	}
	// get first one in result index
	return s.getByPk(sc, name, value, result[0])
}

func (s *Storage) Update(value interface{}) error {
	sc, newData, e := s.parseValue(value)
	if e != nil {
		return e
	}

	// get old data
	pk := getMapPk(newData, sc.PK)
	key := sc.Name + strconv.Itoa(pk)
	_, oldData, e := s.chunk.Get(key)

	// update index
	e = s.index.Update(sc, oldData.(map[string]interface{}), newData, pk)
	if e != nil {
		return e
	}

	// update chunk
	c, e := s.chunk.Update(newData, key)
	if e != nil {
		return e
	}

	// flush files
	e = s.index.FlushChange()
	if e != nil {
		return e
	}
	e = s.chunk.FlushChunk(c)
	if e != nil {
		return e
	}
	return nil
}

// get data by index value.
// set the index field name to read the field in value interface.
// if is max, use the one of max id in result to assign into value interface.
// Otherwise, use min id one.
func (s *Storage) GetBy(value interface{}, index string, isMax bool) error {
	sc, data, e := s.parseValue(value)
	if e != nil {
		return e
	}

	return s.getByIndex(sc, value, sc.Name, index, data[index], isMax)

}

// register struct if not exist in storage.
func (s *Storage) Register(value ...interface{}) error {
	for _, v := range value {
		rt, e := getReflectType(v)

		// check schema existing
		if _, ok := s.schemaData[rt.Name()]; ok {
			continue
		}
		if e != nil {
			return e
		}
		// create schema
		s.schemaData[rt.Name()], e = NewSchema(rt)
		if e != nil {
			return e
		}
	}
	return toJsonFile(s.schemaFile, s.schemaData)
}

// create new storage in dir.
func NewStorage(dir string) (s *Storage, e error) {
	if !com.IsDir(dir) {
		e = os.MkdirAll(dir, os.ModePerm)
		if e != nil {
			return
		}
	}
	s = new(Storage)
	s.dir = dir
	s.schemaData = map[string]*Schema{}
	s.schemaFile = path.Join(s.dir, "schema.scd")

	// try to load schema
	if com.IsFile(s.schemaFile) {
		e = fromJsonFile(s.schemaFile, &s.schemaData)
		if e != nil {
			return
		}
	}

	// create or load chunk
	if !com.IsFile(path.Join(s.dir, "data1.dat")) {
		s.chunk, e = NewChunk(s.dir)
		if e != nil {
			return
		}
	} else {
		s.chunk, e = ReadChunk(s.dir)
		if e != nil {
			return
		}
	}

	// create or load index
	if !com.IsFile(path.Join(s.dir, "index1.idx")) {
		s.index, e = NewIndex(s.dir)
		if e != nil {
			return
		}
	} else {
		s.index, e = ReadIndex(s.dir)
		if e != nil {
			return
		}
	}

	return
}
