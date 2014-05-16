package gojx

import (
	"fmt"
	"reflect"
	"strconv"
)

type Query struct {
	s     *Storage
	eq    map[string]interface{}
	order int
}

func (q *Query) Eq(k string, v interface{}) *Query {
	q.eq[k] = v
	return q
}

func (q *Query) ToSlice(v interface{}) (e error) {
	sliceRt, sliceRv := reflect.TypeOf(v), reflect.ValueOf(v)
	// check type, need *[]*Value
	if sliceRt.Kind() != reflect.Ptr || sliceRt.Elem().Kind() != reflect.Slice || sliceRt.Elem().Elem().Kind() != reflect.Ptr || sliceRt.Elem().Elem().Elem().Kind() != reflect.Struct {
		e = ErrorToSliceNeedSlice
		return
	}
	// get table
	sliceElemRt := sliceRt.Elem().Elem().Elem()
	table := q.s.table[getReflectTypeName(sliceElemRt)]

	// parse eq condition
	sliceResult := q.parseEq(table)
	q.buildSlice(sliceResult, table, sliceElemRt, sliceRv)
	return
}

func (q *Query) buildSlice(sliceResult []int, t *Table, rt reflect.Type, sliceRv reflect.Value) (e error) {
	if len(sliceResult) < 1 {
		return
	}
	for _, pk := range sliceResult {
		rv := reflect.New(rt)
		if pk < 1 {
			continue
		}
		_, iftValue, e := t.Get(pk)
		if e != nil {
			return e
		}
		if iftValue == nil {
			continue
		}
		e = q.s.saver.ToStruct(iftValue, rv.Interface())
		if e != nil {
			return e
		}
		sliceRv.Elem().Set(reflect.Append(sliceRv.Elem(), rv))
	}
	return
}

func (q *Query) parseEq(t *Table) []int {
	res := []interface{}{}
	for k, v := range q.eq {
		idx := t.valueIndex[k]
		if idx == nil {
			continue
		}
		tmp := idx.Get(fmt.Sprintf("%v", v))
		if len(tmp) > 0 {
			if len(res) < 1 {
				res = tmp
				continue
			}
			tmp2 := []interface{}{}
			for _, j := range tmp {
				if _, b := isInInterfaceSlice(res, j); b {
					tmp2 = append(tmp2, j)
				}
			}
			if len(tmp2) < 1 {
				return []int{}
			}
			res = tmp2
		} else {
			return []int{}
		}
	}
	result := make([]int, len(res))
	for i, v := range res {
		pk, _ := strconv.Atoi(fmt.Sprint(v))
		result[i] = pk
	}
	return result
}

func NewQuery(s *Storage) *Query {
	q := new(Query)
	q.s = s
	q.order = ORDER_ASC
	q.eq = make(map[string]interface{})
	return q
}
