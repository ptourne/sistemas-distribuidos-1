package filter

import "fmt"

type MissingFieldError struct {
	Field string
}

func (e *MissingFieldError) Error() string {
	return fmt.Sprintf("missing field: %s\n", e.Field)
}

type InvalidFieldError struct {
	Field string
	Value any
}

func (e *InvalidFieldError) Error() string {
	return fmt.Sprintf("invalid field: %s, value: %+v\n", e.Field, e.Value)
}
