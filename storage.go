package gojx

import (
	"github.com/Unknwon/com"
	"os"
	"reflect"
)

type Storage struct {
	dir    string
	types  map[string]reflect.Type
	tables map[string]*Table
}

// Register struct to storage.
// If struct type exist, read table data.
// If not exist, create table with empty data.
// The table name is struct name.
func (s *Storage) Register(a ...interface{}) error {
	for _, v := range a {
		rt, err := getStructPointer(v)
		if err != nil {
			return err
		}
		s.types[rt.Name()] = rt
	}
	for name, rt := range s.types {
		table, err := s.CreateOrReadTable(rt)
		if err != nil {
			return err
		}
		s.tables[name] = table
	}
	return nil
}

// Insert struct type.
func (s *Storage) Insert(a interface{}) (int, error) {
	// check struct type
	err := checkStructType(a, s)
	if err != nil {
		return 0, err
	}
	table, err := getTable(a, s)
	if err != nil {
		return 0, err
	}
	return table.Insert(a)
}

// Create storage struct by directory name.
// It will make dir if not existed.
// It doesn't load data now until register struct to storage.
func NewStorage(dir string) (*Storage, error) {
	if !com.IsDir(dir) {
		err := os.MkdirAll(dir, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}
	s := &Storage{dir, make(map[string]reflect.Type), make(map[string]*Table)}
	return s, nil
}
