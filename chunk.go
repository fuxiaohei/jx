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
	xFile         string
	xMap          map[string]int
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

// get value by key in chunk.
func (c *Chunk) getInChunk(key string, i int) (j int, v interface{}, e error) {
	if len(c.raw[i]) < 1 {
		e = c.readChunk(i)
		if e != nil {
			return
		}
	}
	if _, ok := c.raw[i][key]; ok {
		v = c.raw[i][key]
		j = i
		return
	}
	j = -1
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
	e = c.readChunk(c.readC)
	if e != nil {
		return
	}

	// find in this chunk data
	i, v, e = c.getInChunk(key, c.readC)
	if e != nil {
		return
	}
	if v != nil {
		return
	}
	return c.getInPrev(key)
}

// get value by key.
// return the chunk cursor where value in and value data.
func (c *Chunk) Get(key string) (i int, v interface{}, e error) {
	i = c.xMap[key]
	if i > 0 {
		i, v, e = c.getInChunk(key, i)
		if v != nil || e != nil {
			return
		}
	}
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
	} else {
		c.raw[c.writeC][key] = v
		cursor = c.writeC
	}

	c.xMap[key] = cursor
	e = c.flushXMap()
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
	delete(c.xMap, key)
}

// read chunk by cursor int
func (c *Chunk) readChunk(i int) (e error) {
	// load chunk file
	var tmp map[string]interface{}
	e = c.s.FromFile(c.fileName(i), &tmp)
	if e != nil {
		return
	}
	c.raw[c.readC] = tmp
	return
}

// flush chunk by cursor int
func (c *Chunk) flushChunk(i int) (e error) {
	if _, ok := c.raw[i]; !ok {
		return
	}
	e = c.s.ToFile(c.fileName(i), c.raw[i])
	return
}

// flush x map to file
func (c *Chunk) flushXMap() (e error) {
	e = c.s.ToFile(c.xFile, c.xMap)
	return
}

// read x map from file
func (c *Chunk) readXMap() (e error) {
	e = c.s.FromFile(c.xFile, &c.xMap)
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
	c.xFile = path.Join(directory, "datax.dat")
	c.xMap = make(map[string]int)

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
		if e = c.flushXMap(); e != nil {
			return
		}
		e = c.flushChunk(c.writeC)
		return
	}

	// read last chunk file to memory and save cursor
	c.readC = i
	c.writeC = i
	if e = c.readXMap(); e != nil {
		return
	}
	e = c.readChunk(c.readC)
	return
}
