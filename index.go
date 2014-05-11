package gojx

import (
	"fmt"
	"github.com/Unknwon/com"
	"path"
	"strconv"
)

type Index struct {
	raw     map[int]map[string]interface{}
	writeC  int
	dir     string
	size    int
	changeC []int
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

// assign cursor to changeC.
// use for Index.FlushChange().
func (idx *Index) assignChange(i int) {
	_, ok := isInIntSlice(idx.changeC, i)
	if !ok {
		idx.changeC = append(idx.changeC, i)
	}
}

// put data into indexes with pk and schema.
// it writes indexes in memory.
// use Index.FlushCurrent() to flush files.
func (idx *Index) Insert(sc *Schema, data map[string]interface{}, pk int) error {
	if idx.isCurrentFull() {
		e := idx.moveWriteNext()
		if e != nil {
			return e
		}
	}

	// write pk
	pkKey := idx.buildKey(sc.Name, "pk", "")
	idx.putValue(pkKey, pk)

	// write indexes pk
	for _, idxName := range sc.Index {
		key := idx.buildKey(sc.Name, idxName, data[idxName])
		idx.putValue(key, pk)
		key2 := idx.buildKey(sc.Name, idxName, "")
		idx.putValue(key2, data[idxName])
	}
	return nil
}

// update index with old and new data map.
// if same value in old and new data, do not change index.
// or remove index by old data, add index by new data.
// it changes in memory, please use index.FlushChange() to write changing index file.
func (idx *Index) Update(sc *Schema, oldData, newData map[string]interface{}, pkInt int) error {
	pk := float64(pkInt)
	for _, idxName := range sc.Index {
		// if value is same, do not change index
		if oldData[idxName] == newData[idxName] {
			continue
		}
		// remove pk in old data index
		cursor, _ := idx.removeInIndex(sc.Name, idxName, oldData[idxName], pk)
		idx.assignChange(cursor)
		// add pk to new data index
		cursor, _ = idx.addInIndex(sc.Name, idxName, newData[idxName], pk)
		idx.assignChange(cursor)
	}
	return nil
}

// get index result slice by type name, field name and value.
func (idx *Index) Select(name string, field string, value interface{}) (cursor int, result []interface{}) {
	key := idx.buildKey(name, field, value)
	for i, m := range idx.raw {
		if _, ok := m[key]; ok {
			result = m[key].([]interface{})
			cursor = i
			return
		}
	}
	return
}

// flush current index data map.
func (idx *Index) FlushCurrent() error {
	return toJsonFile(idx.getCursorFile(idx.writeC), idx.raw[idx.writeC])
}

// flush changed index to file.
func (idx *Index) FlushChange() error {
	for _, i := range idx.changeC {
		e := toJsonFile(idx.getCursorFile(i), idx.raw[i])
		if e != nil {
			return e
		}
	}
	return nil
}

// make interface slice to int slice.
// it matches float64 type in json unmarshal types.
func (idx *Index) toIntSlice(src []interface{}) (des []int, ok bool) {
	if len(src) < 1 {
		ok = false
		return
	}
	_, ok = src[0].(float64)
	if !ok {
		return
	}
	des = make([]int, len(src))
	for i, v := range src {
		des[i] = int(v.(float64))
	}
	ok = true
	return
}

// remove pk in data index.
// if index is empty or null, return nil.
// otherwise, return changed index slice.
func (idx *Index) removeInIndex(name string, field string, value interface{}, pk interface{}) (int, []interface{}) {
	cursor, index := idx.Select(name, field, value)
	key := idx.buildKey(name, field, value)
	if cursor < 1 || len(index) < 1 {
		return 0, nil
	}
	for i, v := range index {
		if v == pk {
			index = append(index[:i], index[i+1:]...)
			idx.raw[cursor][key] = index
			return cursor, index
		}
	}
	return cursor, index
}

// add pk into data index.
// if index is null, create index for new data value.
func (idx *Index) addInIndex(name string, field string, value interface{}, pk interface{}) (int, []interface{}) {
	cursor, index := idx.Select(name, field, value)
	key := idx.buildKey(name, field, value)
	if cursor < 1 || len(index) < 1 {
		// add new index
		cursor = idx.writeC
		index = []interface{}{pk}
		idx.raw[cursor][key] = index
		// add index value
		key2 := idx.buildKey(name, field, "")
		idx.putValue(key2, value)
	} else {
		index = idx.raw[cursor][key].([]interface{})
		index = append(index, pk)
		idx.raw[cursor][key] = index
	}
	return cursor, index
}

// create new index with dir.
func NewIndex(dir string) (idx *Index, e error) {
	idx = new(Index)
	idx.dir = dir
	idx.writeC = 1
	idx.size = INDEX_SIZE
	idx.changeC = []int{}
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
	idx.changeC = []int{}
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
