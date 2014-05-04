package gojx

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"reflect"
)

func setStructPKInt(a interface{}, name string, id int) {
	fmt.Println("set pk",a)
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

func checkStructType(data interface{}, s *Storage) error {
	rt, err := getStructPointer(data)
	if err != nil {
		return err
	}
	if _, ok := s.types[rt.Name()]; ok {
		return nil
	}
	return fmtError(ErrInsertNoType, rt)
}

func getTable(data interface{}, s *Storage) (*Table, error) {
	rt, err := getStructPointer(data)
	if err != nil {
		return nil, err
	}
	t, ok := s.tables[rt.Name()]
	if !ok {
		return nil, fmtError(ErrInsertNoTable, rt)
	}
	return t, nil
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

func inIntSlice(s []int,i int)bool{
	for _,j := range s{
		if i == j{
			return true
		}
	}
	return false
}
