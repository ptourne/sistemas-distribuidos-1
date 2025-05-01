package codec

import (
	"fmt"
	"io"
)

func MapEncode[T any](data map[string]T, encoder func(T) ([]byte, error)) ([]byte, error) {
	buf, err := Uint64Encode(uint64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("failed to encode map length: %w", err)
	}
	for key, value := range data {
		keyBytes, err := StringEncode(key)
		if err != nil {
			return nil, fmt.Errorf("failed to encode key %q: %w", key, err)
		}
		valueBytes, err := encoder(value)
		if err != nil {
			return nil, fmt.Errorf("failed to encode value for key %q: %w", key, err)
		}
		buf = append(buf, keyBytes...)
		buf = append(buf, valueBytes...)
	}
	return buf, nil
}

func MapDecode[T any](r io.Reader, decoder func(io.Reader) (T, error)) (map[string]T, error) {
	size, err := Uint64Decode(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode map length: %w", err)
	}
	mapData := make(map[string]T, size)
	for i := uint64(0); i < size; i++ {
		key, err := StringDecode(r)
		if err != nil {
			return nil, fmt.Errorf("failed to decode key %d: %w", i, err)
		}
		value, err := decoder(r)
		if err != nil {
			return nil, fmt.Errorf("failed to decode value for key %q: %w", key, err)
		}
		mapData[key] = value
	}
	return mapData, nil
}
