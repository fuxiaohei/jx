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
	c.writeCursor = "dat"+strconv.Itoa(c.writeCursorInt)
	c.rawData[c.writeCursor] = make(map[string]interface{})
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
	c.writeCursor = "dat"+strconv.Itoa(i)
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
	return c, nil
}
