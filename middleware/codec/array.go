package codec

import (
	"fmt"
	"io"
)

func ArrayEncode[T any](value []T, encoder func(T) ([]byte, error)) ([]byte, error) {
	bytes, err := Uint64Encode(uint64(len(value)))
	if err != nil {
		return nil, fmt.Errorf("failed to encode array length: %w", err)
	}

	for _, v := range value {
		data, err := encoder(v)
		if err != nil {
			return nil, fmt.Errorf("failed to encode array element: %w", err)
		}
		bytes = append(bytes, data...)
	}
	return bytes, nil
}

func ArrayDecode[T any](r io.Reader, decoder func(io.Reader) (T, error)) ([]T, error) {
	length, err := Uint64Decode(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode array length: %w", err)
	}

	result := make([]T, length)
	for i := range length {
		value, err := decoder(r)
		if err != nil {
			return nil, fmt.Errorf("failed to decode array element at index %d: %w", i, err)
		}
		result[i] = value
	}
	return result, nil
}
