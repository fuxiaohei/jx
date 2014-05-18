package gojx

import (
	"fmt"
	"reflect"
	"strconv"
)

// Query provides query engine for selection by condition.
type Query struct {
	s     *Storage
	eq    map[string]interface{}
	order int
	limit [2]int
}

// Eq add eq condition in index field.
func (q *Query) Eq(k string, v interface{}) *Query {
	q.eq[k] = v
	return q
}

// Limit set returning slice size.
func (q *Query) Limit(i int) *Query {
	q.limit = [2]int{0, i}
	return q
}

// Page set returning slice size as pagination.
func (q *Query) Pager(page, size int) *Query {
	if page < 1 {
		page = 1
	}
	if size < 1 {
		size = 1
	}
	q.limit = [2]int{(page - 1) * size, page * size}
	return q
}

// ToSlice query storage by conditions and assign results to slice.
// Slice need a *[]*Struct ( a reference of slice of struct pointer.
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

	// parse limit condition
	sliceResult = q.parseLimit(sliceResult)
	if len(sliceResult) < 1 {
		return
	}

	// build slice
	q.buildSlice(sliceResult, table, sliceElemRt, sliceRv)
	return
}

// assign result data to slice.
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

// parse eq condition.
// return pk slice []int.
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

// parse limit condition and pagination.
func (q *Query) parseLimit(result []int) []int {
	if q.limit[1] > 0 {
		length := len(result)
		if length <= q.limit[0] {
			return nil
		}
		if length <= q.limit[1] {
			q.limit[1] = length
		}
		return result[q.limit[0]:q.limit[1]]
	}
	return result
}

// create new Query with *Storage.
func NewQuery(s *Storage) *Query {
	q := new(Query)
	q.s = s
	q.order = ORDER_ASC
	q.eq = make(map[string]interface{})
	q.limit = [2]int{-1, -1}
	return q
}
