package gojx

import (
	"fmt"
	"github.com/Unknwon/com"
)

// Index is value index operator.
type Index struct {
	prefix string // the key for saving keys
	raw    map[string][]interface{}
	file   string
	s      Mapper
}

// refresh index keys
func (idx *Index) refresh() {
	tmp := []interface{}{}
	for k, v := range idx.raw {
		if k == idx.prefix {
			continue
		}
		if len(v) < 1 {
			// delete key if empty value
			delete(idx.raw, k)
			continue
		}
		tmp = append(tmp, k)
	}
	idx.raw[idx.prefix] = tmp
}

// put value to index by key.
func (idx *Index) Put(key string, value interface{}) {
	tmp := idx.raw[key]
	if len(tmp) < 1 {
		tmp = []interface{}{value}
	} else {
		tmp = append(tmp, value)
	}
	idx.raw[key] = tmp
	if key == idx.prefix {
		return
	}
	idx.refresh()
}

// get index by key.
func (idx *Index) Get(key string) []interface{} {
	return idx.raw[key]
}

// delete value in index by key
func (idx *Index) Del(key string, value interface{}) {
	tmp := idx.raw[key]
	for k, v := range tmp {
		// use string to check equal
		if fmt.Sprintf("%v", v) == fmt.Sprintf("%v", value) {
			tmp = append(tmp[:k], tmp[k+1:]...)
		}
	}
	idx.raw[key] = tmp
	idx.refresh()
}

// flush index data to file.
func (idx *Index) Flush() error {
	return idx.s.ToFile(idx.file, idx.raw)
}

// create or read index from index file.
func NewIndex(prefix string, file string, s Mapper) (idx *Index, e error) {
	idx = new(Index)
	idx.prefix = prefix
	idx.file = file
	idx.s = s
	if com.IsFile(file) {
		if e = s.FromFile(file, &idx.raw); e != nil {
			return
		}
	} else {
		idx.raw = make(map[string][]interface{})
		idx.raw[prefix] = []interface{}{}
		if e = s.ToFile(file, idx.raw); e != nil {
			return
		}
	}
	return
}

//--------------------------------

// Pk is pk index operator.
type Pk struct {
	raw  []int
	s    Mapper
	file string
}

// put pk to index.
func (p *Pk) Put(i int) {
	_, b := isInIntSlice(p.raw, i)
	if !b {
		p.raw = append(p.raw, i)
	}
}

// get pk position in index.
func (p *Pk) Get(i int) (int, bool) {
	return isInIntSlice(p.raw, i)
}

// get all pks in index.
func (p *Pk) All() []int {
	return p.raw
}

// delete pk in index.
func (p *Pk) Del(i int) {
	for k, v := range p.raw {
		if v == i {
			p.raw = append(p.raw[:k], p.raw[k+1:]...)
		}
	}
}

// flush pk index to file.
func (p *Pk) Flush() error {
	return p.s.ToFile(p.file, p.raw)
}

// create or read pk index from file.
func NewPkIndex(file string, s Mapper) (p *Pk, e error) {
	p = new(Pk)
	p.s = s
	p.file = file
	if com.IsFile(file) {
		if e = s.FromFile(file, &p.raw); e != nil {
			return
		}
	} else {
		p.raw = []int{}
		if e = s.ToFile(file, p.raw); e != nil {
			return
		}
	}
	return
}
