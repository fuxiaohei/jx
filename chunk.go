package gojx

import (
	"github.com/Unknwon/com"
	"path"
	"strconv"
)

type Chunk struct {
	writeC, readC int
	raw           map[int]map[string]interface{}
	directory     string
	s             Mapper
	size          int
}

func (c *Chunk) fileName(cursor int) string {
	return path.Join(c.directory, "data"+strconv.Itoa(cursor)+".dat")
}

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

func (c *Chunk) getInPrev(key string) (i int, v interface{}, e error) {
	c.readC--
	i = c.readC
	if c.readC < 1 {
		return
	}
	var tmp map[string]interface{}
	e = c.s.FromFile(c.fileName(c.readC), &tmp)
	if e != nil {
		return
	}
	c.raw[c.readC] = tmp
	if _, ok := tmp[key]; ok {
		v = tmp[key]
		return
	}
	return c.getInPrev(key)
}

func (c *Chunk) Get(key string) (i int, v interface{}, e error) {
	i, v = c.getInRaw(key)
	if i < 1 || v == nil {
		i, v, e = c.getInPrev(key)
	}
	return
}

func (c *Chunk) putInNext(key string, v interface{}) {
	c.writeC++
	c.raw[c.writeC] = make(map[string]interface{})
	c.raw[c.writeC][key] = v
}

func (c *Chunk) Put(key string, v interface{}) (cursor int, e error) {
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

func (c *Chunk) Update(cursor int, key string, v interface{}) {
	c.raw[cursor][key] = v
}

func (c *Chunk) Del(cursor int, key string) {
	if _, ok := c.raw[cursor]; ok {
		delete(c.raw[cursor], key)
	}
}

func (c *Chunk) flushChunk(i int) (e error) {
	if _, ok := c.raw[i]; !ok {
		return
	}
	e = c.s.ToFile(c.fileName(i), c.raw[i])
	return
}

func NewChunk(directory string, s Mapper, size int) (c *Chunk, e error) {
	c = new(Chunk)
	c.directory = directory
	c.s = s
	c.size = size
	c.raw = make(map[int]map[string]interface{})
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
	if !com.IsFile(c.fileName(i)) {
		c.readC = 1
		c.writeC = 1
		c.raw[c.writeC] = make(map[string]interface{})
		e = c.flushChunk(c.writeC)
		return
	}

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
