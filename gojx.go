package gojx

import "errors"

const (
	VERSION = "0.2"

	INDEX_INT    = "INT"
	INDEX_STRING = "STRING"
	INDEX_PK     = "PK"

)

var (
	CHUNK_SIZE = 200


	ErrRegisterNeedStructPointer = "register type need struct pointer"

	ErrSchemaNeedField      = "schema '%s' need field"
	ErrSchemaPKNeedInt      = "schema '%s' pk field '%s' need int type"
	ErrSchemaIndexTypeError = "schema '%s' index field '%s' need string or int type"

	ErrPutMissingSchema = "put '%s' missing schema"

	ErrorNoData = errors.New("no data")
)


