package gojx

import (
	"fmt"
	"github.com/Unknwon/com"
)

type Index struct {
	prefix string
	raw    map[string][]interface{}
	file   string
	s      Mapper
}

func (idx *Index) refresh() {
	tmp := []interface{}{}
	for k, v := range idx.raw {
		if k == idx.prefix {
			continue
		}
		if len(v) < 1 {
			delete(idx.raw, k)
			continue
		}
		tmp = append(tmp, k)
	}
	idx.raw[idx.prefix] = tmp
}

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

func (idx *Index) Get(key string) []interface{} {
	return idx.raw[key]
}

func (idx *Index) Del(key string, value interface{}) {
	tmp := idx.raw[key]
	for k, v := range tmp {
		if fmt.Sprintf("%v", v) == fmt.Sprintf("%v", value) {
			tmp = append(tmp[:k], tmp[k+1:]...)
		}
	}
	idx.raw[key] = tmp
	idx.refresh()
}

func (idx *Index) Flush() error {
	return idx.s.ToFile(idx.file, idx.raw)
}

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

type Pk struct {
	raw  []int
	s    Mapper
	file string
}

func (p *Pk) Put(i int) {
	_, b := isInIntSlice(p.raw, i)
	if !b {
		p.raw = append(p.raw, i)
	}
}

func (p *Pk) Get(i int) (int, bool) {
	return isInIntSlice(p.raw, i)
}

func (p *Pk) All() []int {
	return p.raw
}

func (p *Pk) Del(i int) {
	for k, v := range p.raw {
		if v == i {
			p.raw = append(p.raw[:k], p.raw[k+1:]...)
		}
	}
}

func (p *Pk) Flush() error {
	return p.s.ToFile(p.file, p.raw)
}

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
