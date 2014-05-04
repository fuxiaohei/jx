package gojx

const (
	VERSION = "0.1"


	INDEX_INT    = "INT"
	INDEX_STRING = "STRING"
	INDEX_PK     = "PK"

	CHUNK_SIZE = 100

)

var (
	ErrRegisterNeedStructPointer = "register type need struct pointer"

	ErrSchemaNeedField      = "schema '%s' need field"
	ErrSchemaPKNeedInt      = "schema '%s' pk field '%s' need int type"
	ErrSchemaIndexTypeError = "schema '%s' index field '%s' need string or int type"
	ErrSchemaWriteNoFile    = "schema `%s` write to no file"

	ErrInsertNoType  = "insert unregistered type '%s'"
	ErrInsertNoTable = "insert no-table type '%s'"
)


