package gojx

import "errors"

const (
	CHUNK_SIZE = 100
	INDEX_SIZE = 1000
)

var (
	ErrorNeedPointer = errors.New("need struct pointer")
	ErrorNeedField   = errors.New("need struct fields")
	ErrorNeedPKInt   = errors.New("need pk int type")
	ErrorFlushNull   = errors.New("flush null or no-load chunk")
	ErrorGetNoPk     = errors.New("get 0 pk value")
	ErrorNoData      = errors.New("no data")

	ErrPutNoSchema = "put no schema type '%s'"
)
