package gojx

import (
	"bufio"
	"github.com/Unknwon/com"
	"io"
	"os"
	"path"
	"reflect"
	"strings"
)

var (
	indexSep = []byte{10, 0, 0, 0, 10}
)

// IndexValue is single index item struct. It maintains index's name, value and pk.
// If Del is true, it marks this item as deleted,
// so the previous index value with same name, pk and value is ignored.
type IndexValue struct {
	Name string
	Pk   int64
	Val  interface{}
	Del  bool
}

// Index is index value's manager.
// Get, put and delete working for indexes.
type Index struct {
	files       map[string]string
	fileTypes   map[string]reflect.Type
	fileWriters map[string]*os.File

	data    map[string]map[interface{}][]int64
	encoder Encoder
}

// put index value to memory
func (idx *Index) putToMem(v IndexValue) {
	if _, ok := idx.data[v.Name]; !ok {
		idx.data[v.Name] = make(map[interface{}][]int64)
	}
	if _, ok := idx.data[v.Name][v.Val]; !ok {
		idx.data[v.Name][v.Val] = []int64{v.Pk}
		return
	}
	idx.data[v.Name][v.Val] = append(idx.data[v.Name][v.Val], v.Pk)
}

// delete index value in memory
func (idx *Index) delToMem(v IndexValue) {
	if _, ok := idx.data[v.Name]; !ok {
		return
	}
	if _, ok := idx.data[v.Name][v.Val]; !ok {
		return
	}
	// delete just assign pk to -1
	// if need filtered pk slice, use cleanIndex()
	for i, pk := range idx.data[v.Name][v.Val] {
		if pk == v.Pk {
			idx.data[v.Name][v.Val][i] = -1
		}
	}
}

// write index value to file
func (idx *Index) putToFile(v IndexValue) (e error) {
	bytes, e := idx.encoder.DataToBytes(v)
	if e != nil {
		return
	}
	bytes = append(bytes, indexSep...)
	_, e = idx.fileWriters[v.Name].Write(bytes)
	return
}

// put value and pk to indexes.
// it parses each index field and value in reflect.Value to each index value,
// then put to memory and file.
func (idx *Index) Put(pk int64, rv reflect.Value) (e error) {
	for name, _ := range idx.files {
		v := rv.FieldByName(name).Interface()
		idxValue := IndexValue{name, pk, v, false}
		e = idx.PutValue(idxValue)
		if e != nil {
			return
		}
	}
	return
}

// set pk and value to indexes.
// it compared field value from rv reflect.Value and oldRv reflect.Value.
// if same value, do not update.
// if not same, delete old index with value in oldRv, put new index with value in rv.
func (idx *Index) Set(pk int64, rv reflect.Value, oldRv reflect.Value) (e error) {
	for name, _ := range idx.files {
		v := rv.FieldByName(name).Interface()
		oldV := oldRv.FieldByName(name).Interface()
		if v == oldV {
			continue
		}
		idxV := IndexValue{name, pk, v, false}
		if e = idx.PutValue(idxV); e != nil {
			return
		}
		idxV = IndexValue{name, pk, oldV, true}
		if e = idx.PutValue(idxV); e != nil {
			return
		}
	}
	return
}

// delete index by pk.
// it parsed fields and values in rv and makes them as deleted.
func (idx *Index) Del(pk int64, rv reflect.Value) (e error) {
	for name, _ := range idx.files {
		v := rv.FieldByName(name).Interface()
		idxValue := IndexValue{name, pk, v, true}
		e = idx.PutValue(idxValue)
		if e != nil {
			return
		}
	}
	return
}

// put indexValue to memory and file.
// it indexValue.Del is true, delete in memory.
func (idx *Index) PutValue(v IndexValue) (e error) {
	if v.Del {
		idx.delToMem(v)
	} else {
		idx.putToMem(v)
	}
	e = idx.putToFile(v)
	return
}

// rebuild index means clean deleted indexValue in index file.
// it writes new indexValues to a rebuild file.
func (idx *Index) Rebuild() (e error) {
	for name, indexes := range idx.data {
		file := idx.files[name] + ".build"
		os.Remove(file)

		var writer *os.File
		writer, e = os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
		if e != nil {
			return
		}
		for v, pkSlice := range indexes {
			for _, pk := range pkSlice {
				idxV := IndexValue{name, pk, v, false}
				bytes, e := idx.encoder.DataToBytes(idxV)
				if e != nil {
					return e
				}
				bytes = append(bytes, indexSep...)
				_, e = writer.Write(bytes)
				if e != nil {
					return e
				}
			}
		}

		if e = writer.Sync(); e != nil {
			return
		}
		if e = writer.Close(); e != nil {
			return
		}

	}
	return
}

// read index file.
// it read index file bytes and decodes bytes to many indexValue.
// then make them living in memory in proper map.
func (idx *Index) readIdxFile(name string, file string) (e error) {
	idx.fileWriters[name], e = os.OpenFile(file, os.O_APPEND|os.O_RDWR, os.ModePerm)
	if e != nil {
		return
	}
	bufferReader := bufio.NewReader(idx.fileWriters[name])
	tmp := []byte{}
	for {
		bytes, e := bufferReader.ReadSlice('\n')
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		if len(bytes) == 4 && bytes[0] == 0 && bytes[1] == 0 && bytes[2] == 0 {
			v := IndexValue{}
			e = idx.encoder.DataFromBytes(tmp[:len(tmp)-1], &v)
			if e != nil {
				return e
			}
			tmp = []byte{}
			if !v.Del {
				idx.putToMem(v)
			} else {
				idx.delToMem(v)
			}
			continue
		}
		tmp = append(tmp, bytes...)
	}
	return
}

// clean deleted items in memory index.
// deleted items is -1 value in pk slice.
func (idx *Index) cleanIndex() {
	for k, v := range idx.data {
		for k2, pkSlice := range v {
			tmp := []int64{}
			for _, pk := range pkSlice {
				if pk > 0 {
					tmp = append(tmp, pk)
				}
			}
			idx.data[k][k2] = tmp
		}
	}
}

// create new index with Object's indexes map, directory and encoder.
// it not exist, it creates default empty files.
// or read old index files.
func NewIndex(indexes map[string]reflect.Type, directory string, encoder Encoder) (idx *Index, e error) {
	idx = &Index{
		data:        make(map[string]map[interface{}][]int64),
		files:       make(map[string]string),
		fileTypes:   indexes,
		fileWriters: make(map[string]*os.File),
		encoder:     encoder,
	}
	for name, _ := range indexes {
		idx.files[name] = strings.ToLower(path.Join(directory, name+".idx"))
	}
	for name, file := range idx.files {
		if com.IsFile(file) {
			if e = idx.readIdxFile(name, file); e != nil {
				return
			}
			continue
		}
		idx.fileWriters[name], e = os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
		if e != nil {
			return
		}
		idx.data[name] = make(map[interface{}][]int64)
	}
	idx.cleanIndex()
	return
}
