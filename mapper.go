package gojx

import (
	"encoding/json"
	"errors"
	"io/ioutil"
	"os"
	"reflect"
)

var (
	mapperManager map[string]Mapper
)

func init() {
	mapperManager = make(map[string]Mapper)
	mapperManager[MAPPER_JSON] = new(JsonMapper)
}

type Mapper interface {
	FromStruct(interface{}) ([]byte, error)
	ToStruct(interface{}, interface{}) error

	ToFile(string, interface{}) error
	FromFile(string, interface{}) error
}

type JsonMapper struct{}

func (jm *JsonMapper) FromStruct(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

func (jm *JsonMapper) ToStruct(v interface{}, v2 interface{}) error {
	if reflect.TypeOf(v) == reflect.TypeOf(v2).Elem() {
		reflect.ValueOf(v2).Elem().Set(reflect.ValueOf(v))
		return nil
	}
	tmp, ok := v.(map[string]interface{})
	if !ok {
		return errors.New("json mapper only can convert map[string]interface to struct")
	}
	jsonBytes, err := json.Marshal(tmp)
	if err != nil {
		return err
	}
	return json.Unmarshal(jsonBytes, v2)
}

func (jm *JsonMapper) ToFile(file string, v interface{}) (e error) {
	bytes, e := json.Marshal(v)
	if e != nil {
		return
	}
	e = ioutil.WriteFile(file, bytes, os.ModePerm)
	return
}

func (jm *JsonMapper) FromFile(file string, v interface{}) (e error) {
	bytes, e := ioutil.ReadFile(file)
	if e != nil {
		return
	}
	e = json.Unmarshal(bytes, v)
	return
}
