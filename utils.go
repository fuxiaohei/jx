package gojx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
)

func getReflectType(a interface{}) (rt reflect.Type, e error) {
	rt = reflect.TypeOf(a)
	if rt.Kind() != reflect.Ptr {
		e = ErrorNeedPointer
		return
	}
	rt = rt.Elem()
	if rt.Kind() != reflect.Struct {
		e = ErrorNeedPointer
		return
	}
	return
}

func getMapPk(data map[string]interface{}, pk string) int {
	return int(data[pk].(float64))
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

func isInIntSlice(src []int, value int) (i int, b bool) {
	for k, v := range src {
		if v == value {
			i = k
			b = true
			return
		}
	}
	return
}

func isInInterfaceSlice(src []interface{}, value interface{}) (i int, b bool) {
	for k, v := range src {
		if v == value {
			i = k
			b = true
			return
		}
	}
	return
}
