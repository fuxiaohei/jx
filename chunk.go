package gojx

import (
	"github.com/Unknwon/com"
	"path"
	"strconv"
)

type Chunk struct {
	dir string

	readC, writeC int
	changeC []int

	raw   map[int]map[string]interface{} // raw data in chunks
	cache map[string]int                 // key / chunk cursor map cache

	size int
}

// get cursor string by int.
func (c *Chunk) getCursor(i int) string {
	return "data" + strconv.Itoa(i) + ".dat"
}

// get cursor file name by int.
func (c *Chunk) getCursorFile(i int) string {
	return path.Join(c.dir, c.getCursor(i))
}

// is current chunk full.
func (c *Chunk) isCurrentFull() bool {
	return len(c.raw[c.writeC]) > c.size
}

// move writer chunk to next.
// it flushes current chunk first.
func (c *Chunk) moveWriteNext() error {
	e := c.FlushCurrent()
	if e != nil {
		return e
	}
	c.writeC++
	c.raw[c.writeC] = make(map[string]interface{})
	return nil
}

// flush chunk by cursor int.
// if the chunk is nil or not-loaded, return error.
func (c *Chunk) FlushChunk(i int) error {
	if _, ok := c.raw[i]; ok {
		return toJsonFile(path.Join(c.dir, c.getCursor(i)), c.raw[i])
	}
	return ErrorFlushNull
}

// flush current chunk.
func (c *Chunk) FlushCurrent() error {
	return c.FlushChunk(c.writeC)
}

// put data into chunk with key string.
// if current chunk is full, move to next then insert.
// it can't write file immediately.
// use Chunk.FlushCurrent() to sync current writing chunk.
func (c *Chunk) Put(data interface{}, key string) error {
	if c.isCurrentFull() {
		e := c.moveWriteNext()
		if e != nil {
			return e
		}
	}
	c.raw[c.writeC][key] = data
	return nil
}

// update data in chunk with key string.
// try to find the data. If not exist, return error.
// then update data in chunk, return cursor int.
// it doesn't write file.
// use Chunk.FlushChunk(cursor) to write into file.
func (c *Chunk) Update(data interface{}, key string) (int, error) {
	// try to find it, not no it, do not update
	i, _, e := c.Get(key)
	if e != nil {
		return 0, e
	}
	c.raw[i][key] = data
	return i, nil
}

// find data in memory data by key.
// it depends the loaded chunks data.
func (c *Chunk) findInMem(key string) (cursor int, data interface{}) {
	for i, m := range c.raw {
		if _, ok := m[key]; ok {
			data = m[key]
			cursor = i
			c.cache[key] = i
			return
		}
	}
	return 0, nil
}

// find data in non-loaded data by key.
// it moves read cursor to prev chunk then find data in this chunk.
// if exist, return. Otherwise, move prev again util no prev chunk.
func (c *Chunk) findInPrev(key string) (cursor int, data interface{}, e error) {
	if c.readC < 2 {
		return
	}
	c.readC--
	var tmp map[string]interface{}
	e = fromJsonFile(c.getCursorFile(c.readC), &tmp)
	if e != nil {
		return
	}
	c.raw[c.readC] = tmp
	if _, ok := c.raw[c.readC][key]; ok {
		cursor = c.readC
		data = c.raw[c.readC][key]
		c.cache[key] = cursor
		return
	}
	return c.findInPrev(key)
}

// find data in cache map by key.
// cache map saves the cursor of chunk where the key in.
// then find data in proper chunk.
func (c *Chunk) findInCache(key string) (cursor int, data interface{}) {
	cursor = c.cache[key]
	// if chunk is not load, return nil. then call c.findInPrev(key) to find in no-loaded prev chunks.
	if _, ok := c.raw[cursor]; !ok {
		return
	}
	if cursor > 0 {
		data = c.raw[cursor][key]
	}
	return
}

// get data by key.
// find in cache map first.
// then find in memory map.
// last find in prev chunks.
func (c *Chunk) Get(key string) (cursor int, data interface{}, e error) {
	cursor, data = c.findInCache(key)
	if data != nil && cursor > 0 {
		return
	}
	cursor, data = c.findInMem(key)
	if data == nil {
		return c.findInPrev(key)
	}
	return
}

// create new chunk in dir.
func NewChunk(dir string) (c *Chunk, e error) {
	c = new(Chunk)
	c.dir = dir
	c.readC, c.writeC = 1, 1
	c.raw = make(map[int]map[string]interface{})
	c.raw[c.writeC] = map[string]interface{}{}
	c.size = CHUNK_SIZE
	c.cache = map[string]int{}
	c.changeC = []int{}
	e = c.FlushChunk(c.writeC)
	return
}

// read chunks in dir.
// it reads the last chunk as pre-load chunk and move cursor to last.
func ReadChunk(dir string) (c *Chunk, e error) {
	i := 2
	c = new(Chunk)
	c.dir = dir
	c.size = CHUNK_SIZE
	c.cache = map[string]int{}
	c.changeC = []int{}
	for {
		if !com.IsFile(c.getCursorFile(i)) {
			break
		}
		i++
	}
	i--
	c.readC, c.writeC = i, i
	c.raw = make(map[int]map[string]interface{})
	c.raw[c.readC] = map[string]interface{}{}

	var tmp map[string]interface{}
	e = fromJsonFile(c.getCursorFile(c.readC), &tmp)
	if e != nil {
		return
	}
	c.raw[c.readC] = tmp
	return
}
