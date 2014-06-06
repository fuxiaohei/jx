package jx

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"io/ioutil"
	"os"
)

// Encoder define how to serialize and un-serialize value to bytes or file
type Encoder interface {
	DataToFile(interface{}, string) error
	DataFromFile(string, interface{}) error

	DataToBytes(interface{}) ([]byte, error)
	DataFromBytes([]byte, interface{}) error
}

// JsonEncoder encode or decode value by json string
type JsonEncoder struct{}

// marshal value to json bytes
func (js *JsonEncoder) DataToBytes(data interface{}) ([]byte, error) {
	return json.Marshal(data)
}

// marshal value to json bytes then write to file
func (js *JsonEncoder) DataToFile(data interface{}, file string) (e error) {
	bytes, e := json.Marshal(data)
	if e != nil {
		return
	}
	e = ioutil.WriteFile(file, bytes, os.ModePerm)
	return
}

// unmarshal from json bytes
func (js *JsonEncoder) DataFromBytes(bytes []byte, data interface{}) error {
	return json.Unmarshal(bytes, data)
}

// unmarshal from file bytes as json format
func (js *JsonEncoder) DataFromFile(file string, data interface{}) (e error) {
	bytes, e := ioutil.ReadFile(file)
	if e != nil {
		return
	}
	e = json.Unmarshal(bytes, data)
	return
}

// GoEncoder encode and decode value via gob
type GobEncoder struct{}

// encode value to gob bytes
func (g *GobEncoder) DataToBytes(data interface{}) ([]byte, error) {
	buff := bytes.NewBuffer(nil)
	encoder := gob.NewEncoder(buff)
	e := encoder.Encode(data)
	if e != nil {
		return nil, e
	}
	return buff.Bytes(), nil
}

// encode value to gob bytes then write to file
func (g *GobEncoder) DataToFile(data interface{}, file string) (e error) {
	bytes, e := g.DataToBytes(data)
	if e != nil {
		return
	}
	return ioutil.WriteFile(file, bytes, os.ModePerm)
}

// decode gob bytes to value
func (g *GobEncoder) DataFromBytes(byte []byte, data interface{}) error {
	buff := bytes.NewReader(byte)
	decoder := gob.NewDecoder(buff)
	return decoder.Decode(data)
}

// decode file bytes to value as gob bytes
func (g *GobEncoder) DataFromFile(file string, data interface{}) (e error) {
	bytes, e := ioutil.ReadFile(file)
	if e != nil {
		return
	}
	return g.DataFromBytes(bytes, data)
}
