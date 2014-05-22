package gojx

import (
	"errors"
	"fmt"
)

const (
	MAPPER_JSON = "JSON"

	ORDER_DESC = 9
	ORDER_ASC  = 6
)

var (
	ErrStrSaverUnknown      = "unknown saver '%s'"
	ErrStrStructNeedPointer = "register '%s' need struct pointer"
	ErrStrStructNeedField   = "struct '%s' need field"
	ErrStrStructPkNeedInt   = "struct '%s' pk field '%d' need int type"
	ErrStrStructPkZero      = "struct '%s' pk field < 1"

	ErrStrSchemaUnknown = "unknown schema '%s'"

	ErrStrUpdateNullData = "update null '%s' with pk '%d'"

	ErrorNoData           = errors.New("no data") // no data error
	ErrorToSliceNeedSlice = errors.New("need a reference of slice of struct pointer")
)

func fmtError(msg string, a ...interface{}) error {
	return errors.New(fmt.Sprintf(msg, a...))
}
