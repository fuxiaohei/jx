package gojx

import (
	"fmt"
	"path"
)

type Index struct {
	Name  string                      `json:"name"`
	Type  map[string]string           `json:"type"`
	Data  map[string]map[string][]int `json:"data"`
	Queue map[string][]interface{}    `json:"queue"`
	PK    []int                       `json:"pk"`
}

// put data to fill index slice.
// it will fill all indexes and pk slice.
// remember that this method only operates index's memory data.
// please call Index.Flush() to write into file.
func (idx *Index) Put(data map[string]interface{}, pk int) {
	for name, t := range idx.Type {
		// add pk
		if t == INDEX_PK {
			if idx.PK == nil {
				idx.PK = []int{pk}
			} else {
				idx.PK = append(idx.PK, pk)
			}
			continue
		}

		// add pk to index
		key := fmt.Sprintf("%v", data[name])
		if idx.Data[name] == nil {
			idx.Data[name] = map[string][]int{key: []int{pk}}
		} else {
			if idx.Data[name][key] == nil {
				idx.Data[name][key] = []int{pk}
			} else {
				idx.Data[name][key] = append(idx.Data[name][key], pk)
			}
		}

		// add value to queue
		if idx.Queue[name] == nil {
			idx.Queue[name] = []interface{}{data[name]}
		} else {
			if !inItfSlice(idx.Queue[name], data[name]) {
				idx.Queue[name] = append(idx.Queue[name], data[name])
			}
		}
	}
}

// write index object to file.
func (idx *Index) Flush(s *Storage) error {
	return toJsonFile(path.Join(s.dir, idx.Name+".idx"), idx)
}

// create new index with schema.
// try to load from file first.
// if fails, create empty index object.
func NewIndex(sc *Schema, s *Storage) (idx *Index, err error) {
	idxFile := path.Join(s.dir, sc.Name+".idx")
	idx = new(Index)
	err = fromJsonFile(idxFile, idx)
	if err == nil {
		return
	}
	idx = &Index{Name: sc.Name,
		PK:    []int{},
		Data:  make(map[string]map[string][]int),
		Queue: make(map[string][]interface{}),
		Type:  map[string]string{},
	}
	idx.Type[sc.PK] = INDEX_PK
	for _, i := range sc.StringIndex {
		idx.Data[i] = make(map[string][]int)
		idx.Type[i] = INDEX_STRING
	}
	for _, i := range sc.IntIndex {
		idx.Data[i] = make(map[string][]int)
		idx.Type[i] = INDEX_INT
	}
	err = toJsonFile(idxFile, idx)
	return
}
