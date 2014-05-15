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

// mapper read and write struct to file, convert data to struct or struct to bytes.
type Mapper interface {
	FromStruct(interface{}) ([]byte, error)
	ToStruct(interface{}, interface{}) error

	ToFile(string, interface{}) error
	FromFile(string, interface{}) error
}

// json data mapper.
type JsonMapper struct{}

// json marshal struct to bytes.
func (jm *JsonMapper) FromStruct(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// convert data to struct by json unmarshal.
// if data's reflect.Type is same to destination struct, assign use reflect.
// if json unmarshal, data need be type map[string]interface.
func (jm *JsonMapper) ToStruct(v interface{}, v2 interface{}) error {
	// if same reflect.Type, use reflect.Set
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

// write struct to json in  file.
func (jm *JsonMapper) ToFile(file string, v interface{}) (e error) {
	bytes, e := json.Marshal(v)
	if e != nil {
		return
	}
	e = ioutil.WriteFile(file, bytes, os.ModePerm)
	return
}

// read struct from json in file.
func (jm *JsonMapper) FromFile(file string, v interface{}) (e error) {
	bytes, e := ioutil.ReadFile(file)
	if e != nil {
		return
	}
	e = json.Unmarshal(bytes, v)
	return
}
