package gojx

import (
	"github.com/Unknwon/com"
	"path"
	"strconv"
)

// Chunk is data file reader and writer.
// It saves data to each chunk files.
type Chunk struct {
	writeC, readC int // read and writer cursor
	raw           map[int]map[string]interface{}
	directory     string
	s             Mapper
	size          int // size of data items in a chunk file
}

// get file name by cursor.
func (c *Chunk) fileName(cursor int) string {
	return path.Join(c.directory, "data"+strconv.Itoa(cursor)+".dat")
}

// get value by key in memory map.
func (c *Chunk) getInRaw(key string) (cursor int, v interface{}) {
	for k, m := range c.raw {
		if _, ok := m[key]; ok {
			cursor = k
			v = m[key]
			return
		}
	}
	cursor = -1
	return
}

// get value by key in prev chunks files.
// The chunk files are not loaded in memory.
func (c *Chunk) getInPrev(key string) (i int, v interface{}, e error) {
	// move read cursor to prev one
	c.readC--
	i = c.readC
	if c.readC < 1 {
		return
	}

	// load chunk file
	var tmp map[string]interface{}
	e = c.s.FromFile(c.fileName(c.readC), &tmp)
	if e != nil {
		return
	}
	c.raw[c.readC] = tmp

	// find in this chunk data
	if _, ok := tmp[key]; ok {
		v = tmp[key]
		return
	}
	return c.getInPrev(key)
}

// get value by key.
// return the chunk cursor where value in and value data.
func (c *Chunk) Get(key string) (i int, v interface{}, e error) {
	i, v = c.getInRaw(key)
	if i < 1 || v == nil {
		i, v, e = c.getInPrev(key)
	}
	return
}

// put value to next chunk.
// it increases the writer cursor to next chunk.
func (c *Chunk) putInNext(key string, v interface{}) {
	c.writeC++
	c.raw[c.writeC] = make(map[string]interface{})
	c.raw[c.writeC][key] = v
}

// put value to chunk.
// if current chunk is full, put to next chunk.
func (c *Chunk) Put(key string, v interface{}) (cursor int, e error) {
	// if current is full, flush current chunk then put value to next chunk.
	if len(c.raw[c.writeC]) > c.size {
		if e = c.flushChunk(c.writeC); e != nil {
			cursor = -1
			return
		}
		c.putInNext(key, v)
		cursor = c.writeC
		return
	}
	c.raw[c.writeC][key] = v
	cursor = c.writeC
	return
}

// update value.
func (c *Chunk) Update(cursor int, key string, v interface{}) {
	if _, ok := c.raw[cursor]; ok {
		c.raw[cursor][key] = v
	}
}

// delete value
func (c *Chunk) Del(cursor int, key string) {
	if _, ok := c.raw[cursor]; ok {
		delete(c.raw[cursor], key)
	}
}

// flush chunk by cursor int
func (c *Chunk) flushChunk(i int) (e error) {
	if _, ok := c.raw[i]; !ok {
		return
	}
	e = c.s.ToFile(c.fileName(i), c.raw[i])
	return
}

// creat or read chunk in directory.
// if not exist, write empty chunk file.
// or read old chunks to prepare.
func NewChunk(directory string, s Mapper, size int) (c *Chunk, e error) {
	c = new(Chunk)
	c.directory = directory
	c.s = s
	c.size = size
	c.raw = make(map[int]map[string]interface{})

	// try to read chunk files
	i := 2
	for {
		file := c.fileName(i)
		if com.IsFile(file) {
			i++
		} else {
			i--
			break
		}
	}
	// if no chunk files, write empty chunk
	if !com.IsFile(c.fileName(i)) {
		c.readC = 1
		c.writeC = 1
		c.raw[c.writeC] = make(map[string]interface{})
		e = c.flushChunk(c.writeC)
		return
	}

	// read last chunk file to memory and save cursor
	c.readC = i
	c.writeC = i
	var tmp map[string]interface{}
	e = s.FromFile(c.fileName(c.readC), &tmp)
	if e != nil {
		return
	}
	c.raw[c.readC] = tmp
	return
}
