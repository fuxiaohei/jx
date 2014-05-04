package gojx

import (
	"github.com/Unknwon/com"
	"path"
	"reflect"
	"strconv"
)

type Table struct {
	Name string
	s    *Storage

	Schema *Schema

	Index     map[string]*Index
	indexFile string

	Chunk *Chunk
}

// Check table whether exist.
// It checks schema file and index file.
// If they are exist, return true.
func (t *Table) IsExist() bool {
	schemaFile := path.Join(t.s.dir, t.Name+".scd")
	if !com.IsFile(schemaFile) {
		return false
	}
	indexFile := path.Join(t.s.dir, t.Name+".idx")
	if !com.IsFile(indexFile) {
		return false
	}
	return true
}

// Insert data to storage.
// The data type need be registered.
// If success, return max id. Otherwise, return error.
// It updates schema changes and indexes changes.
func (t *Table) Insert(a interface{}) (int, error) {
	// struct to map
	dataMap, err := struct2map(a)
	if err != nil {
		return 0, nil
	}

	// get max id
	t.Schema.MaxId++

	// insert index
	for name, idx := range t.Index {
		if idx.Type == INDEX_PK {
			idx.Insert("", t.Schema.MaxId)
			continue
		}
		if idx.Type == INDEX_INT {
			value := strconv.Itoa(int(dataMap[name].(float64)))
			idx.Insert(value, t.Schema.MaxId)
			continue
		}
		if idx.Type == INDEX_STRING {
			value := dataMap[name].(string)
			idx.Insert(value, t.Schema.MaxId)
		}
	}

	// insert chunk
	dataMap[t.Schema.PK] = t.Schema.MaxId
	err = t.Chunk.Insert(dataMap, t.Schema.MaxId)
	if err != nil {
		return 0, err
	}

	// save all to file
	err = t.Schema.Write()
	if err != nil {
		return 0, err
	}
	err = toJsonFile(t.indexFile, t.Index)
	if err != nil {
		return 0, err
	}
	setStructPKInt(a, t.Schema.PK, t.Schema.MaxId)

	return t.Schema.MaxId, nil
}

// Get data by pk int.
// If not in pk slice, return no-data.
// If not in chunk, return no-data.
func (t *Table) GetByPK(a interface{}) error {
	// struct to map
	dataMap, err := struct2map(a)
	if err != nil {
		return err
	}

	// get pk int slice
	ids := t.Index[t.Schema.PK].PK
	if len(ids) < 1 {
		return ErrorNoData
	}

	// check pk exist
	pk := int(dataMap[t.Schema.PK].(float64))
	if !inIntSlice(ids, pk) {
		return ErrorNoData
	}

	// get from chunk
	return t.getInChunk(pk, a)

}

func (t *Table) getInChunk(pk int, a interface{}) error {
	// get from chunk
	_, value := t.Chunk.Get(pk)
	valueMap, ok := value.(map[string]interface{})
	if ok {
		// assign to struct
		err := map2struct(valueMap, a)
		if err != nil {
			return err
		}
		return nil
	}
	return ErrorNoData
}

// Get data by index value.
// The index parameter means index names.
// And they are affected together. So if some indexes, the result is in merged index result.
// If found pk slice, it assigns max pk one to return.
func (t *Table) GetByIndex(a interface{}, index []string) error {
	// struct to map
	dataMap, err := struct2map(a)
	if err != nil {
		return err
	}

	ids := []int{}

	for _, indexName := range index {
		idSlice := []int{}
		value := dataMap[indexName]
		if idx, ok := t.Index[indexName]; ok {
			if idx.Type == INDEX_STRING {
				idSlice = idx.GetIds(value.(string))
			}
			if idx.Type == INDEX_INT {
				idSlice = idx.GetIds(int(value.(float64)))
			}
		}

		ids = mergeIntSliceUnique(ids, idSlice)
	}
	if len(ids) < 1 {
		return ErrorNoData
	}

	if len(ids) > 1 {
		sortIntSliceDesc(ids)
	}

	// get max id one
	return t.getInChunk(ids[0], a)
}

func createSchema(rt reflect.Type, t *Table, s *Storage) error {
	var err error
	t.Schema, err = NewSchema(rt)
	if err != nil {
		return err
	}
	t.Schema.name = t.Name
	t.Schema.file = path.Join(s.dir, t.Name+".scd")
	return t.Schema.Write()
}

func createIndex(t *Table, s *Storage) error {
	// create indexes
	t.Index = make(map[string]*Index)
	for _, idx := range t.Schema.StringIndex {
		t.Index[idx] = NewIndex(idx, INDEX_STRING)
	}
	for _, idx := range t.Schema.IntIndex {
		t.Index[idx] = NewIndex(idx, INDEX_INT)
	}
	t.Index[t.Schema.PK] = NewIndex(t.Schema.PK, INDEX_PK)
	t.indexFile = path.Join(s.dir, t.Name+".idx")
	return toJsonFile(t.indexFile, t.Index)
}

// Create new table from reflect.Type.
// Create schema file, write empty index and data chunk file.
func (s *Storage) CreateTable(rt reflect.Type) (*Table, error) {
	t := new(Table)
	t.Name = rt.Name()
	t.s = s

	// create schema
	err := createSchema(rt, t, s)
	if err != nil {
		return nil, err
	}

	// create indexes
	err = createIndex(t, s)
	if err != nil {
		return nil, err
	}

	// create chunk
	c, err := NewChunk(t, s)
	if err != nil {
		return nil, err

	}
	t.Chunk = c

	return t, nil
}

func readSchema(t *Table, s *Storage) error {
	file := path.Join(s.dir, t.Name+".scd")
	err := fromJsonFile(file, &t.Schema)
	if err != nil {
		return err
	}
	t.Schema.file = file
	return nil
}

func readIndex(t *Table, s *Storage) error {
	file := path.Join(s.dir, t.Name+".idx")
	err := fromJsonFile(file, &t.Index)
	if err != nil {
		return err
	}
	t.indexFile = file
	return nil
}

// Read table by name.
// Read schema and index from files.
// Read recent chunk data.
func (s *Storage) ReadTable(name string) (*Table, error) {
	t := new(Table)
	t.Name = name
	t.s = s

	// read schema
	err := readSchema(t, s)
	if err != nil {
		return nil, err
	}

	// read index
	err = readIndex(t, s)
	if err != nil {
		return nil, err
	}

	// read chunk
	t.Chunk, err = NewReadChunk(t, s)
	if err != nil {
		return nil, err
	}

	return t, nil
}

// Create or read table.
// If not exist, create.
func (s *Storage) CreateOrReadTable(rt reflect.Type) (*Table, error) {
	table := &Table{Name: rt.Name(), s: s}
	if table.IsExist() {
		println("read table "+rt.Name())
		return s.ReadTable(rt.Name())
	}
	return s.CreateTable(rt)
}
