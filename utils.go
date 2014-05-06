package gojx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
	"sort"
)

func setStructPKInt(a interface{}, name string, id int) {
	rv := reflect.ValueOf(a)
	field := rv.Elem().FieldByName(name)
	if !field.CanSet() {
		return
	}
	field.SetInt(int64(id))
}

func getStructPointer(data interface{}) (reflect.Type, error) {
	rt := reflect.TypeOf(data)
	if rt.Kind() == reflect.Ptr && rt.Elem().Kind() == reflect.Struct {
		return rt.Elem(), nil
	}
	return nil, fmtError(ErrRegisterNeedStructPointer)
}

func fmtError(msg string, a ...interface{}) error {
	return errors.New(fmt.Sprintf(msg, a...))
}

func toJsonFile(file string, data interface{}) error {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return err
	}
	return ioutil.WriteFile(file, jsonBytes, os.ModePerm)
}

func fromJsonFile(file string, data interface{}) error {
	fileBytes, err := ioutil.ReadFile(file)
	if err != nil {
		return err
	}
	return json.Unmarshal(fileBytes, data)
}

func struct2map(data interface{}) (map[string]interface{}, error) {
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	var tmp map[string]interface{}
	err = json.Unmarshal(jsonBytes, &tmp)
	return tmp, err
}

func map2struct(tmp map[string]interface{}, data interface{}) error {
	jsonBytes, err := json.Marshal(tmp)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonBytes, data)
}

func inIntSlice(s []int, i int) bool {
	for _, j := range s {
		if i == j {
			return true
		}
	}
	return false
}

func inStringSlice(s []string, i string) bool {
	for _, j := range s {
		if i == j {
			return true
		}
	}
	return false
}

func inItfSlice(s []interface{}, v interface{}) bool {
	for _, vv := range s {
		if vv == v {
			return true
		}
	}
	return false
}

func mergeIntSliceUnique(src []int, mg []int) []int {
	t := src
	for _, i := range mg {
		if !inIntSlice(src, i) {
			t = append(t, i)
		}
	}
	return t
}

func sortIntSliceDesc(src []int) {
	sort.Sort(sort.Reverse(sort.IntSlice(src)))
}
