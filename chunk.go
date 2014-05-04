package gojx

import (
	"github.com/Unknwon/com"
	"path"
	"strconv"
)

type Chunk struct {
	writeCursorInt int
	writeCursor    string
	readCursorInt  int
	readCursor     string
	rawData        map[string]map[string]interface{}
	size           int
	name           string
	s              *Storage
}

// Insert data to chunk.
// The id is pk int value.
// If the current write cursor map is length of chunk size, it moves to next chunk.
func (c *Chunk) Insert(data interface{}, id int) error {
	if len(c.rawData[c.writeCursor]) >= c.size {
		c.moveWriteNext()
	}
	key := c.name + "-" + strconv.Itoa(id)
	c.rawData[c.writeCursor][key] = data
	file := path.Join(c.s.dir, c.name+"."+c.writeCursor)
	return toJsonFile(file, c.rawData[c.writeCursor])
}

func (c *Chunk) moveWriteNext() {
	c.writeCursorInt++
	c.writeCursor = "dat" + strconv.Itoa(c.writeCursorInt)
	if len(c.rawData[c.writeCursor]) < 1 {
		c.rawData[c.writeCursor] = make(map[string]interface{})
	}
}

func (c *Chunk) getInAll(id int) interface{} {
	for name, _ := range c.rawData {
		value := c.getInChunk(name, id)
		if value != nil {
			return value
		}
	}
	return nil
}

func (c *Chunk) getInChunk(name string, id int) interface{} {
	key := c.name + "-" + strconv.Itoa(id)
	return c.rawData[name][key]
}

// Get chunk by pk id.
// It reads all memory data first. If found, return interface value.
// If not found, move reader cursor to prev one, load proper chunk to memory.
// Then find it in the last-loaded chunk data.
func (c *Chunk) Get(id int) interface{} {
	value := c.getInAll(id)
	if value == nil {
		if c.moveReadPrev() {
			return c.Get(id)
		} else {
			return nil
		}
	}
	return value
}

func (c *Chunk) moveReadPrev() bool {
	c.readCursorInt--
	if c.readCursorInt < 1 {
		return false
	}
	c.readCursor = "dat" + strconv.Itoa(c.readCursorInt)
	if len(c.rawData[c.readCursor]) < 1 {
		c.rawData[c.readCursor] = make(map[string]interface{})
		file := path.Join(c.s.dir, c.name+"."+c.readCursor)
		tmp := make(map[string]interface{})
		fromJsonFile(file, &tmp)
		c.rawData[c.readCursor] = tmp
		println("move chunk read cursor to " + c.name + "." + c.readCursor)
	}
	return true
}

// Create new chunk for table in storage.
// It creates an empty chunk file as dat1.
func NewChunk(t *Table, s *Storage) (*Chunk, error) {
	c := new(Chunk)
	c.s = s
	c.name = t.Name
	c.size = t.Schema.ChunkSize

	// init first chunk
	c.writeCursorInt = 1
	c.writeCursor = "dat1"
	c.readCursorInt = 1
	c.readCursor = "dat1"

	// write empty data to first chunk
	c.rawData = make(map[string]map[string]interface{})
	c.rawData[c.writeCursor] = make(map[string]interface{})
	file := path.Join(c.s.dir, c.name+"."+c.writeCursor)
	err := toJsonFile(file, c.rawData[c.writeCursor])
	return c, err
}

// Read chunks in files for table and storage.
// It walks to recent data file as datX.
// And set write-read cursor to recent index.
func NewReadChunk(t *Table, s *Storage) (*Chunk, error) {
	c := new(Chunk)
	c.s = s
	c.name = t.Name
	c.size = t.Schema.ChunkSize

	// walk to last chunk file
	i := 1
	for {
		file := path.Join(s.dir, c.name+".dat"+strconv.Itoa(i))
		if com.IsFile(file) {
			i++
			continue
		}
		break
	}
	i--
	c.writeCursorInt = i
	c.writeCursor = "dat" + strconv.Itoa(i)
	c.readCursorInt = i
	c.readCursor = c.writeCursor

	// load last chunk to memory
	c.rawData = make(map[string]map[string]interface{})
	c.rawData[c.writeCursor] = make(map[string]interface{})
	file := path.Join(c.s.dir, c.name+"."+c.writeCursor)
	tmp := make(map[string]interface{})
	err := fromJsonFile(file, &tmp)
	if err != nil {
		return nil, err
	}
	c.rawData[c.writeCursor] = tmp
	println("read chunk cursor to " + c.name + "." + c.readCursor)
	return c, nil
}
