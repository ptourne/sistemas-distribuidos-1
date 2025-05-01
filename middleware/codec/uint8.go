package codec

import (
	"fmt"
	"io"
)

func Uint8Encode(value uint8) ([]byte, error) {
	return []byte{value}, nil
}

func Uint8Decode(r io.Reader) (uint8, error) {
	value, err := DoRead(1, r)
	if err != nil {
		return 0, fmt.Errorf("failed to read uint8: %w", err)
	}
	return value[0], nil
}
