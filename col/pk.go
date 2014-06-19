package col

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/Unknwon/com"
	"io"
	"io/ioutil"
	"os"
	"path"
	"reflect"
)

var (
	PKConflict = errors.New("pk conflict")
	PkEmpty    = errors.New("pk empty")
)

type PK struct {
	directory string
	file      *os.File

	autoFile string
	autoId   int64
	auto     bool

	data           map[string]*PkValue
	lastLoadCursor int
}

// set pk value if auto increment,
// check pk unique.
func (p *PK) SetPk(v interface{}, field string) (pk interface{}, e error) {
	rv := reflect.ValueOf(v).Elem()
	pk = rv.FieldByName(field).Interface()
	if _, ok := p.data[fmt.Sprint(pk)]; ok {
		e = PKConflict
		return
	}
	// if pk auto increment, set pk
	if p.auto {
		p.autoId++
		rv.FieldByName(field).SetInt(p.autoId)
		e = p.WriteIncrement()
		if e == nil {
			pk = p.autoId
		}
	} else {
		// make sure pk is not empty. but 0 is valid.
		if fmt.Sprint(pk) == "" {
			e = PkEmpty
			return
		}
	}
	return
}

// write current max id to auto increment file.
func (p *PK) WriteIncrement() (e error) {
	e = ioutil.WriteFile(p.autoFile, int64ToBytes(p.autoId), os.ModePerm)
	return
}

// get pk meta by value.
func (p *PK) Get(pk interface{}) (v *PkValue, e error) {
	v = p.data[fmt.Sprint(pk)]
	return
}

// delete pk meta by value.
// it writes a deleted pkValue, not deletes old data.
func (p *PK) Delete(pk interface{}) (e error) {
	// write delete mark item
	pkValue := &PkValue{
		Value: fmt.Sprint(pk),
		Del:   1,
	}
	bytes, e := json.Marshal(pkValue)
	if e != nil {
		return
	}
	e = p.writeBytes(bytes, p.file)
	if e == nil {
		// delete in memory
		delete(p.data, pkValue.Value)
	}
	return
}

// read all pk items from file.
// assign to memory map.
func (p *PK) Read() (e error) {
	for {
		// read head
		head := make([]byte, 8)
		if _, e = p.file.Read(head); e != nil {
			return
		}

		// read data by length
		data := make([]byte, bytesToInt64(head))
		if _, e = p.file.Read(data); e != nil {
			return
		}

		// json unmarshal
		v := &PkValue{}
		if e = json.Unmarshal(data, v); e != nil {
			return
		}
		if v.Del > 0 {
			delete(p.data, v.Value)
			continue
		}
		p.data[v.Value] = v
		p.lastLoadCursor = v.Cursor
	}
	return
}

// read increment max id from file.
// if current id is larger, keep current.
func (p *PK) ReadIncrement() (e error) {
	bytes, e := ioutil.ReadFile(p.autoFile)
	if e != nil {
		return
	}
	id := bytesToInt64(bytes)
	if id > p.autoId {
		p.autoId = id
	}
	return
}

// write pk values to file.
// cursor means where the data in.
// uid means the data unique id in chunk file.
// del means deleted status.
func (p *PK) Write(pk interface{}, cursor int, uid int64, del int) (e error) {
	pkValue := &PkValue{
		Cursor: cursor,
		Del:    del,
		Uid:    uid,
		Value:  fmt.Sprint(pk),
	}
	bytes, e := json.Marshal(pkValue)
	if e != nil {
		return
	}
	e = p.writeBytes(bytes, p.file)
	if e == nil {
		p.data[pkValue.Value] = pkValue
	}
	return
}

// update pk with new pkValue.
// write to file with pk interface value.
// assign new pkValue in memory.
func (p *PK) Update(pk interface{}, pkV *PkValue) (e error) {
	// write new value to file
	if e = p.Write(pk, pkV.Cursor, pkV.Uid, 0); e != nil {
		return
	}
	// update memory
	p.data[pkV.Value] = pkV // todo : maybe no need
	return
}

// write bytes to file.
// build bytes with header byte.
func (p *PK) writeBytes(b []byte, writer *os.File) (e error) {
	var buf bytes.Buffer
	buf.Write(int64ToBytes(int64(len(b))))
	buf.Write(b)
	_, e = writer.Write(buf.Bytes())
	return
}

// get the last pk value's chunk cursor.
func (p *PK) GetLastCursor() int {
	return p.lastLoadCursor
}

// get current max auto increment int64.
func (p *PK) GetAutoIncrement() int64 {
	return p.autoId
}

// init pk data as first running.
func (p *PK) firstInit() (e error) {
	// first init
	if e = os.MkdirAll(p.directory, os.ModePerm); e != nil {
		return
	}

	// create pk file
	p.file, e = os.OpenFile(path.Join(p.directory, "pk.pk"), os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if e != nil {
		return
	}

	// create auto file
	if p.auto {
		p.autoFile = path.Join(p.directory, "auto.pk")
		p.autoId = 0
		// write current id to file, make sure to create file.
		e = p.WriteIncrement()
	}
	return
}

// init pk.
// create files in first init.
// read files after first init.
func (p *PK) init() (e error) {
	if !com.IsDir(p.directory) {
		return p.firstInit()
	}

	// read auto increment
	if p.auto {
		p.autoFile = path.Join(p.directory, "auto.pk")
		e = p.ReadIncrement()
		if e != nil {
			return
		}
	}

	// read file in
	p.file, e = os.OpenFile(path.Join(p.directory, "pk.pk"), os.O_APPEND|os.O_RDWR, os.ModePerm)
	if e != nil {
		return
	}
	e = p.Read()
	//println("read pk items :", len(p.data))
	if e != nil && e != io.EOF {
		return
	}
	e = nil

	return
}

// optimize pk value to opm file.
// clean delete items.
func (p *PK) Optimize() (e error) {
	optFile := path.Join(p.directory, "pk.pk.opm")

	fileWriter, e := os.OpenFile(optFile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.ModePerm)
	if e != nil {
		return
	}

	// pull all memory pk data to opm file.
	for _, pkValue := range p.data {
		bytes, e := json.Marshal(pkValue)
		if e != nil {
			return e
		}
		if e = p.writeBytes(bytes, fileWriter); e != nil {
			return e
		}
	}

	return
}

// create new pk in directory with pk auto-increment setting.
func NewPk(directory string, auto bool) (p *PK, e error) {
	p = &PK{
		directory: directory,
		auto:      auto,
		data:      make(map[string]*PkValue),
	}
	e = p.init()
	return
}

// PkValue defines the each pk item data struct.
type PkValue struct {
	Value  string `json:"v"`
	Uid    int64  `json:"u,omitempty"`
	Cursor int    `json:"c,omitempty"`
	Del    int    `json:"d"`
}
