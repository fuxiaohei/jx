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

	rawData map[int]map[string]interface{}

	size int
	dir  string
}

func (c *Chunk) isWritingChunkFull() bool {
	return len(c.rawData[c.writeCursorInt]) >= c.size
}

func (c *Chunk) moveWriteNext() (err error) {
	c.writeCursorInt++
	c.writeCursor = "data"+strconv.Itoa(c.writeCursorInt)
	c.rawData[c.writeCursorInt] = make(map[string]interface{})
	err = toJsonFile(path.Join(c.dir, c.writeCursor+".dat"), c.rawData[c.writeCursorInt])
	return
}

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

func (c *Chunk) FlushCurrent() error {
	return toJsonFile(path.Join(c.dir, c.writeCursor+".dat"), c.rawData[c.writeCursorInt])
}

// create new chunk.
// It's default cursor is 1.
// And write first chunk file as empty data set.
func NewChunk(dir string, size int) (c *Chunk, err error) {
	c = &Chunk{size: size, dir: dir, rawData: make(map[int]map[string]interface{})}

	c.readCursorInt = 1
	c.readCursor = "data"+strconv.Itoa(c.readCursorInt)
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

	c.readCursorInt = i-1
	c.readCursor = "data"+strconv.Itoa(c.readCursorInt)
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
