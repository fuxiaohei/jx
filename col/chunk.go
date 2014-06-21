package col

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/Unknwon/com"
	"io"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
)

type Chunk struct {
	directory string
	prefix    string
	ext       string

	files   map[int]*os.File
	current int

	limit int

	dataType reflect.Type
	data     map[int]map[int64]interface{}
}

// get chunk directory.
func (c *Chunk) GetDirectory()string{
	return c.directory
}

// get current cursor.
func (c *Chunk) GetCurrent() int {
	return c.current
}

// get data by pkValue.
func (c *Chunk) Get(pk *PkValue) (v interface{}, e error) {
	if _, ok := c.data[pk.Cursor]; !ok {
		// read cursor file if not loaded
		e = c.ReadCursorFile(pk.Cursor, false)
		if e != nil {
			return
		}
	}

	v = c.data[pk.Cursor][pk.Uid]
	return
}

// delete data by pkValue
func (c *Chunk) Delete(pk *PkValue) (e error) {
	if _, ok := c.data[pk.Cursor]; !ok {
		// read cursor file if not loaded
		e = c.ReadCursorFile(pk.Cursor, false)
		if e != nil {
			return
		}
	}
	// delete in memory item
	delete(c.data[pk.Cursor], pk.Uid)
	return
}

// read file by cursor int.
// if asCurrent is true, set the file handler to current.
// so new data are appended to this file until over limit.
func (c *Chunk) ReadCursorFile(i int, asCurrent bool) (e error) {
	// create file handler
	file := c.GetFile(i)
	if !com.IsFile(file) {
		e = fmt.Errorf("file is missing : %s", file)
		return
	}
	c.files[i], e = os.OpenFile(file, os.O_APPEND|os.O_RDWR, os.ModePerm)
	if e != nil {
		return
	}
	// read file data to memory
	mapData, e := c.readFileHandler(c.files[i])
	if e == nil || e == io.EOF {
		c.data[i] = mapData
		e = nil
		if asCurrent {
			c.current = i
		}
	}
	//println("read chunk : @", i, "of", len(mapData), "items")
	return
}

// use file handler to read all data in this file.
// return a map result or error.
func (c *Chunk) readFileHandler(f *os.File) (result map[int64]interface{}, e error) {
	result = make(map[int64]interface{})
	for {
		// read head and uid head
		head := make([]byte, 8)
		if _, e = f.Read(head); e != nil {
			return
		}
		uidHead := make([]byte, 8)
		if _, e = f.Read(uidHead); e != nil {
			return
		}

		// read data
		data := make([]byte, bytesToInt64(head))
		if _, e = f.Read(data); e != nil {
			return
		}

		v := reflect.New(c.dataType).Interface()
		if e = json.Unmarshal(data, v); e != nil {
			return
		}

		result[bytesToInt64(uidHead)] = v
	}
	return
}

// write data into chunk file.
// it returns an unique id for this value bytes.
// it encodes value by json.
func (c *Chunk) Write(v interface{}) (uid int64, cursor int, e error) {
	bytes, e := json.Marshal(v)
	if e != nil {
		return
	}
	uid, e = c.writeBytes(c.current, bytes)
	if e != nil {
		return
	}
	cursor = c.current
	c.data[cursor][uid] = v
	// try move to next if over limit
	if c.limit < len(c.data[c.current]) {
		// sync current file
		c.files[c.current].Sync()

		// rand a new cursor as current
		c.randCursor()
		c.files[c.current], e = os.OpenFile(c.GetFile(c.current), os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
		c.data[c.current] = make(map[int64]interface{})
		//println("move to ", c.current)
	}
	return
}

// update data by pkValue.
// it saves new value to chunk with new unique id.
// update pkValue with new uid.
func (c *Chunk) Update(v interface{}, pk *PkValue) (e error) {
	if _, ok := c.data[pk.Cursor]; !ok {
		// read cursor file if not loaded
		e = c.ReadCursorFile(pk.Cursor, false)
		if e != nil {
			return
		}
	}
	// encode
	bytes, e := json.Marshal(v)
	if e != nil {
		return
	}
	uid, e := c.writeBytes(pk.Cursor, bytes)
	if e != nil {
		return
	}

	// update pkValue and change in memory
	delete(c.data[pk.Cursor], pk.Uid)
	pk.Uid = uid
	c.data[pk.Cursor][uid] = v

	return
}

// write bytes to file.
// build bytes header and a random unique id int64.
func (c *Chunk) writeBytes(cursor int, b []byte) (uid int64, e error) {
	uid = rand.Int63()
	e = c.writeBytesWithUid(c.files[cursor], uid, b)
	return
}

// write bytes to file with unique id.
func (c *Chunk) writeBytesWithUid(writer *os.File, uid int64, b []byte) (e error) {
	var buf bytes.Buffer
	buf.Write(int64ToBytes(int64(len(b))))
	buf.Write(int64ToBytes(uid))
	buf.Write(b)
	_, e = writer.Write(buf.Bytes())
	return
}

// get cursor file path.
func (c *Chunk) GetFile(i int) string {
	return path.Join(c.directory, c.prefix+strconv.Itoa(i)+c.ext)
}

// rand a new cursor to current.
// it the cursor file is, rand new one.
func (c *Chunk) randCursor() {
	cursor := rand.Intn(999)
	if com.IsFile(c.GetFile(cursor)) {
		c.randCursor()
		return
	}
	c.current = cursor
}

// init chunk
func (c *Chunk) init() (e error) {
	if !com.IsDir(c.directory) {
		// first init
		if e = os.MkdirAll(c.directory, os.ModePerm); e != nil {
			return
		}

		// rand cursor and create first data file
		c.randCursor()
		c.files[c.current], e = os.OpenFile(c.GetFile(c.current), os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
		if e != nil {
			return
		}
		c.data[c.current] = make(map[int64]interface{})
	}
	e = c.tryOptimized()
	return
}

// try optimized means replace data file with .opm file,
// if the .opm file is newer than data file, comparing modification time.
func (c *Chunk) tryOptimized() (e error) {
	files, e := filepath.Glob(filepath.Join(c.directory, "*.opm"))
	if e != nil {
		return
	}
	if len(files) < 1 {
		return
	}
	for _, f := range files {
		// compare opm file and original file
		originFile := strings.TrimSuffix(f, ".opm")
		fi, _ := os.Stat(f)
		ofi, _ := os.Stat(originFile)
		if fi == nil || ofi == nil {
			continue
		}

		// if opm file is newer, replace to original file
		if fi.ModTime().Sub(ofi.ModTime()) > 1 {
			if e = os.RemoveAll(originFile); e != nil {
				return
			}
			if e = os.Rename(f, originFile); e != nil {
				return
			}
			if e = os.RemoveAll(f); e != nil {
				return
			}
		}
	}
	return
}

// optimize chunk data.
// it pulls all memory data to opm file.
// notice just loaded chunk file will be optimized.
func (c *Chunk) Optimize() (e error) {
	for cursor, data := range c.data {
		// if < 10% items, no need to optimize
		if len(data) < c.limit/10 {
			continue
		}
		opmFile := c.GetFile(cursor) + ".opm"
		fileWriter, e := os.OpenFile(opmFile, os.O_CREATE|os.O_TRUNC|os.O_RDWR, os.ModePerm)
		if e != nil {
			return e
		}
		for uid, v := range data {
			// encode
			bytes, e := json.Marshal(v)
			if e != nil {
				return e
			}
			// write to file
			e = c.writeBytesWithUid(fileWriter, uid, bytes)
			if e != nil {
				return e
			}
		}
		// do not keep file handler
		fileWriter.Sync()
		fileWriter.Close()
	}
	return
}

// create chunk with directory, prefix and ext string, limit size and data reflect type.
// if chunk data are not existed, create first data as default.
func NewChunk(directory, prefix, ext string, limit int, dataType reflect.Type) (c *Chunk, e error) {
	c = &Chunk{
		directory: directory,
		prefix:    prefix,
		ext:       ext,
		files:     make(map[int]*os.File),
		limit:     limit,
		dataType:  dataType,
		data:      make(map[int]map[int64]interface{}),
	}
	e = c.init()
	return
}
