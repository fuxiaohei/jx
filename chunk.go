package gojx

import (
	"github.com/Unknwon/com"
	"path"
	"strconv"
)

// Chunk manages all chunk files.
type Chunk struct {
	writeCursorInt int
	writeCursor    string
	readCursorInt  int
	readCursor     string

	rawData map[int]map[string]interface{}

	size int
	dir  string
}

// check current chunk is full.
func (c *Chunk) isWritingChunkFull() bool {
	return len(c.rawData[c.writeCursorInt]) >= c.size
}

// move current chunk to next.
// create new chunk with empty data.
func (c *Chunk) moveWriteNext() (err error) {
	c.writeCursorInt++
	c.writeCursor = "data" + strconv.Itoa(c.writeCursorInt)
	c.rawData[c.writeCursorInt] = make(map[string]interface{})
	err = toJsonFile(path.Join(c.dir, c.writeCursor+".dat"), c.rawData[c.writeCursorInt])
	return
}

// put data with key.
// If the current chunk is full, move to next automatically.
// Remember this put action is inserting data to memory map.
// Call chunk.FlushCurrent() to write to file.
func (c *Chunk) Put(key string, data map[string]interface{}) (err error) {
	// try to move next if current chunk is full
	if c.isWritingChunkFull() {
		err = c.FlushCurrent()
		if err != nil {
			return err
		}
		err = c.moveWriteNext()
		if err != nil {
			return
		}
	}
	// write to file
	c.rawData[c.writeCursorInt][key] = data
	return
}

// get data by key in all memory data.
// it ranges all maps in memory.
// return chunk cursor and data or nil.
func (c *Chunk) getInMemory(key string) (i int, data interface{}) {
	for cursor, chunk := range c.rawData {
		if _, ok := chunk[key]; ok {
			data = chunk[key]
			i = cursor
			return
		}
	}
	return 0, nil
}

// move read cursor to prev chunk.
// it loads data in prev chunk to memory and keep.
func (c *Chunk) moveReadPrev() (bool, error) {
	// move to 1, stop
	if c.readCursorInt < 2 {
		return false, nil
	}
	c.readCursorInt--
	c.readCursor = "data" + strconv.Itoa(c.readCursorInt)
	var tmp map[string]interface{}
	err := fromJsonFile(path.Join(c.dir, c.readCursor+".dat"), &tmp)
	if err != nil {
		return false, err
	}
	c.rawData[c.readCursorInt] = tmp
	return true, nil
}

// get data in prev chunks.
// it will find by moving read cursor automatically until the cursor is at the beginning.
func (c *Chunk) getInPrev(key string) (i int, data interface{}, err error) {
	flag, err := c.moveReadPrev()
	if err != nil || !flag {
		return
	}
	if _, ok := c.rawData[c.readCursorInt][key]; ok {
		data = c.rawData[c.readCursorInt][key]
		i = c.readCursorInt
		return
	}
	return c.getInPrev(key)
}

// get data by key.
// return the chunk cursor of the data, data interface value or error.
func (c *Chunk) Get(key string) (i int, data interface{}, err error) {
	i, data = c.getInMemory(key)
	if data != nil {
		return
	}
	i, data, err = c.getInPrev(key)
	return
}

// flush current chunk to file.
func (c *Chunk) FlushCurrent() error {
	return toJsonFile(path.Join(c.dir, c.writeCursor+".dat"), c.rawData[c.writeCursorInt])
}

// create new chunk.
// It's default cursor is 1.
// And write first chunk file as empty data set.
func NewChunk(dir string, size int) (c *Chunk, err error) {
	c = &Chunk{size: size, dir: dir, rawData: make(map[int]map[string]interface{})}

	c.readCursorInt = 1
	c.readCursor = "data" + strconv.Itoa(c.readCursorInt)
	c.writeCursorInt = c.readCursorInt
	c.writeCursor = c.readCursor

	c.rawData[c.writeCursorInt] = make(map[string]interface{})

	err = toJsonFile(path.Join(dir, c.writeCursor+".dat"), c.rawData[c.writeCursorInt])
	return
}

// read existed chunks.
// Walk all chunks to the last.
// Move cursor to last int and read last chunk as pre-load data.
func ReadChunk(dir string, size int) (c *Chunk, err error) {
	i := 2
	for {
		key := "data" + strconv.Itoa(i)
		file := path.Join(dir, key+".dat")
		if com.IsFile(file) {
			i++
			continue
		}
		break
	}
	c = &Chunk{size: size, dir: dir, rawData: make(map[int]map[string]interface{})}

	c.readCursorInt = i - 1
	c.readCursor = "data" + strconv.Itoa(c.readCursorInt)
	c.writeCursorInt = c.readCursorInt
	c.writeCursor = c.readCursor

	var tmp map[string]interface{}
	err = fromJsonFile(path.Join(dir, c.readCursor+".dat"), &tmp)
	if err != nil {
		return
	}
	c.rawData[c.readCursorInt] = tmp
	return
}
