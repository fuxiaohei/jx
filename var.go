package gojx

import (
	"errors"
	"fmt"
)

const (
	MAPPER_JSON = "JSON"
)

var (
	ErrStrSaverUnknown      = "unknown saver '%s'"
	ErrStrStructNeedPointer = "register '%s' need struct pointer"
	ErrStrStructNeedField   = "struct '%s' need field"
	ErrStrStructPkNeedInt   = "struct '%s' pk field '%d' need int type"
	ErrStrStructPkZero      = "struct '%s' pk field < 1"

	ErrStrSchemaUnknown = "unknown schema '%s'"

	ErrStrUpdateNullData = "update null '%s' with pk '%d'"



	ErrorNoData = errors.New("no data")
)

func fmtError(msg string, a ...interface{}) error {
	return errors.New(fmt.Sprintf(msg, a...))
}
