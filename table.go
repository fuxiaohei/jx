package gojx

import (
	"bufio"
	"fmt"
	"github.com/Unknwon/com"
	"io"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
)

var (
	dataSep []byte
)

func init() {
	dataSep = []byte("\n")
	dataSep = append(dataSep, make([]byte, 30)...)
	dataSep = append(dataSep, []byte("\n")...)
}

// Table struct is each value management.
type Table struct {
	dataType  reflect.Type
	directory string
	encoder   Encoder
	pk        string
	size      int

	cursor  int
	writers map[int]*os.File

	data map[int]map[int64]interface{}

	// data file pager leaf
	pagerWriter *os.File
	pagerFile   string
	pageData    map[int64]int
}

// move to write next data file
func (t *Table) tryMoveNext() (e error) {
	if len(t.data[t.cursor]) < t.size {
		return
	}
	t.writers[t.cursor].Sync()
	//t.writer.Close()
	t.cursor++
	t.data[t.cursor] = make(map[int64]interface{})
	t.writers[t.cursor], e = os.OpenFile(t.getFile(t.cursor), os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	return
}

// put value with pk
func (t *Table) Put(pk int64, v interface{}) (e error) {
	// need put new one
	if t.pageData[pk] > 0 {
		return CONFLICT
	}
	// put to memory
	t.data[t.cursor][pk] = v
	t.pageData[pk] = t.cursor

	// write to file
	bytes, e := t.encoder.DataToBytes(v)
	if e != nil {
		return
	}
	if _, e = t.writers[t.cursor].Write(bytes); e != nil {
		return
	}
	if _, e = t.writers[t.cursor].Write(dataSep); e != nil {
		return
	}

	// write to page
	str := fmt.Sprintf("%d-%d\n", pk, t.cursor)
	if _, e = t.pagerWriter.Write([]byte(str)); e != nil {
		return
	}

	// try to move next
	e = t.tryMoveNext()
	return
}

// set value with pk
func (t *Table) Set(pk int64, v interface{}) (e error) {
	cursor := t.pageData[pk]
	if cursor < 1 {
		return NULL
	}

	// read data file if not load
	if _, ok := t.data[cursor]; !ok {
		e = t.readDataFile(cursor)
		if e != nil {
			return
		}
	}

	// set to memory
	t.data[cursor][pk] = v

	// write to file
	bytes, e := t.encoder.DataToBytes(v)
	if e != nil {
		return
	}
	bytes = append(bytes, dataSep...)
	_, e = t.writers[t.cursor].Write(bytes)

	return
}

// delete value by pk
func (t *Table) Del(pk int64) (e error) {
	cursor := t.pageData[pk]
	if cursor < 1 {
		return NULL
	}
	// delete in memory
	if _, ok := t.data[cursor]; ok {
		delete(t.data[cursor], pk)
	}
	delete(t.pageData, pk)

	// delete in page file
	str := fmt.Sprintf("%d-%d-delete\n", pk, cursor)
	_, e = t.pagerWriter.Write([]byte(str))
	return
}

// get value by pk
func (t *Table) Get(pk int64) (v interface{}, e error) {
	cursor := t.pageData[pk]
	if cursor < 1 {
		return
	}
	if _, ok := t.data[cursor]; !ok {
		e = t.readDataFile(cursor)
		if e != nil {
			return
		}
	}
	v = t.data[cursor][pk]
	return
}

// sync all data file writers
func (t *Table) Flush() (e error) {
	if e = t.pagerWriter.Sync(); e != nil {
		return
	}
	for _, writer := range t.writers {
		if e = writer.Sync(); e != nil {
			return
		}
	}
	return
}

// rebuild data file for cleaning deleted values.
// it saves in .rebuild file.
func (t *Table) Rebuild() (e error) {
	for i, _ := range t.data {
		e = t.rebuildFile(i)
		if e != nil {
			return
		}
	}
	return
}

// rebuild each data file
func (t *Table) rebuildFile(i int) (e error) {
	// check data is loaded, if not, stop
	if _, ok := t.data[i]; !ok {
		return
	}

	// create new build file
	data := t.data[i]
	file := t.getFile(i) + ".rebuild"
	os.Remove(file)

	writer, e := os.OpenFile(file, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if e != nil {
		return
	}

	// write data
	for _, v := range data {
		bytes, e := t.encoder.DataToBytes(v)
		if e != nil {
			return e
		}
		bytes = append(bytes, dataSep...)
		if _, e = writer.Write(bytes); e != nil {
			return e
		}
	}

	if e = writer.Sync(); e != nil {
		return
	}

	return writer.Close()
}

// get file with cursor int
func (t *Table) getFile(i int) string {
	return path.Join(t.directory, "data"+strconv.Itoa(i)+".dat")
}

// read page leaf file
func (t *Table) readPageFile() (e error) {
	t.pagerWriter, e = os.OpenFile(t.pagerFile, os.O_APPEND|os.O_RDWR, os.ModePerm)
	if e != nil {
		return
	}
	bufferReader := bufio.NewReader(t.pagerWriter)
	for {
		bytes, e := bufferReader.ReadSlice('\n')
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		strSlice := strings.Split(string(bytes[:len(bytes)-1]), "-")
		if len(strSlice) == 2 {
			pk, _ := strconv.ParseInt(strSlice[0], 10, 64)
			cursor, _ := strconv.Atoi(strSlice[1])
			if pk < 1 || cursor < 1 {
				continue
			}
			t.pageData[pk] = cursor
			continue
		}
		if len(strSlice) == 3 && strSlice[2] == "delete" {
			pk, _ := strconv.ParseInt(strSlice[0], 10, 64)
			if pk < 1 {
				continue
			}
			delete(t.pageData, pk)
		}
	}
	return
}

// read single data file
func (t *Table) readDataFile(i int) (e error) {
	if _, ok := t.data[i]; !ok {
		t.data[i] = make(map[int64]interface{})
	}
	t.writers[i], e = os.OpenFile(t.getFile(i), os.O_APPEND|os.O_RDWR, os.ModePerm)
	if e != nil {
		return
	}
	bufferReader := bufio.NewReader(t.writers[i])
	tmp := []byte{}
	for {
		bytes, e := bufferReader.ReadSlice('\n')
		if e == io.EOF {
			break
		}
		if e != nil {
			return e
		}
		if len(bytes) == 31 && bytes[0] == 0 && bytes[1] == 0 && bytes[30] == 10 {
			rv := reflect.New(t.dataType)
			e = t.encoder.DataFromBytes(tmp[:len(tmp)-1], rv.Interface())
			if e != nil {
				return e
			}
			tmp = []byte{}
			pk := getPk(rv.Elem(), t.pk)
			if pk < 1 || t.pageData[pk] < 1 {
				continue
			}
			t.data[i][pk] = rv.Interface()
			continue
		}
		tmp = append(tmp, bytes...)
	}
	return
}

// create new table
func NewTable(dataType reflect.Type, directory string, encoder Encoder, pk string, size int) (t *Table, e error) {
	if !com.IsDir(directory) {
		if e = os.MkdirAll(directory, os.ModePerm); e != nil {
			return
		}
	}
	t = &Table{
		size:      size,
		pk:        pk,
		dataType:  dataType,
		directory: directory,
		encoder:   encoder,
		cursor:    1,
		pagerFile: path.Join(directory, ".page"),
		data:      make(map[int]map[int64]interface{}),
		pageData:  make(map[int64]int),
		writers:   make(map[int]*os.File),
	}

	if !com.IsFile(t.getFile(t.cursor)) {
		if t.writers[t.cursor], e = os.OpenFile(t.getFile(t.cursor), os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm); e != nil {
			return
		}
		if t.pagerWriter, e = os.OpenFile(t.pagerFile, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm); e != nil {
			return
		}
		t.data[t.cursor] = make(map[int64]interface{})
		return
	}

	// read page file
	if e = t.readPageFile(); e != nil {
		return
	}

	// read data file
	for {
		if com.IsFile(t.getFile(t.cursor + 1)) {
			t.cursor++
			continue
		}
		e = t.readDataFile(t.cursor)
		if e != nil {
			return
		}
		break
	}
	return
}
