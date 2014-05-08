package gojx

import (
	"fmt"
	"github.com/Unknwon/com"
	"path"
	"strconv"
)

type Index struct {
	raw    map[int]map[string]interface{}
	writeC int
	dir    string
	size   int
}

// get index cursor string.
func (idx *Index) getCursor(i int) string {
	return "index" + strconv.Itoa(i) + ".idx"
}

// get index cursor file.
func (idx *Index) getCursorFile(i int) string {
	return path.Join(idx.dir, idx.getCursor(i))
}

// is current index file is full
func (idx *Index) isCurrentFull() bool {
	return len(idx.raw[idx.writeC]) > idx.size
}

// move index writer cursor to next.
func (idx *Index) moveWriteNext() error {
	e := idx.FlushCurrent()
	if e != nil {
		return e
	}
	idx.writeC++
	idx.raw[idx.writeC] = make(map[string]interface{})
	return nil
}

// find index data by key.
func (idx *Index) findKey(key string) (int, interface{}) {
	for j, v := range idx.raw {
		if _, ok := v[key]; ok {
			return j, v[key]
		}
	}
	return 0, nil
}

// build index key with type name, field name and value string.
func (idx *Index) buildKey(name string, field string, value interface{}) string {
	return fmt.Sprintf("%s%s%v", name, field, value)
}

// put value and key into index raw data.
// it updates memory data, not files.
func (idx *Index) putValue(key string, v interface{}) {
	i, pkIndex := idx.findKey(key)
	if pkIndex == nil {
		tmp := []interface{}{v}
		idx.raw[idx.writeC][key] = tmp
	} else {
		tmp := pkIndex.([]interface{})
		tmp = append(tmp, v)
		idx.raw[i][key] = tmp
	}
}

// put data into indexes with pk and schema.
// it writes indexes in memory.
// use Index.FlushCurrent() to flush files.
func (idx *Index) Put(sc *Schema, data map[string]interface{}, pk int) error {
	if idx.isCurrentFull() {
		e := idx.moveWriteNext()
		if e != nil {
			return e
		}
	}

	// write pk
	pkKey := idx.buildKey(sc.Name, "pk", "")
	idx.putValue(pkKey, pk)

	// write indexes
	for _, idxName := range sc.Index {
		key := idx.buildKey(sc.Name, idxName, data[idxName])
		idx.putValue(key, pk)
	}
	return nil
}

// flush current index data map.
func (idx *Index) FlushCurrent() error {
	return toJsonFile(idx.getCursorFile(idx.writeC), idx.raw[idx.writeC])
}

// create new index with dir.
func NewIndex(dir string) (idx *Index, e error) {
	idx = new(Index)
	idx.dir = dir
	idx.writeC = 1
	idx.size = INDEX_SIZE
	idx.raw = make(map[int]map[string]interface{})
	idx.raw[idx.writeC] = make(map[string]interface{})
	e = toJsonFile(path.Join(idx.dir, idx.getCursor(idx.writeC)), idx.raw[idx.writeC])
	return
}

// read index in dir.
// move to last index file for cursor.
// then load all indexes data to map.
func ReadIndex(dir string) (idx *Index, e error) {
	i := 2
	idx = new(Index)
	idx.dir = dir
	idx.size = INDEX_SIZE
	for {
		if !com.IsFile(idx.getCursorFile(i)) {
			break
		}
		i++
	}
	i--
	idx.writeC = i
	idx.raw = make(map[int]map[string]interface{})
	for {
		if i < 1 {
			break
		}
		var tmp map[string]interface{}
		e = fromJsonFile(idx.getCursorFile(i), &tmp)
		if e != nil {
			return
		}
		idx.raw[i] = tmp
		i--
	}
	return
}
