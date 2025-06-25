package codec

import (
	"fmt"
	"io"
)

func StringEncode(data string) ([]byte, error) {
	dataLen := len(data)
	lenBytes, err := Uint64Encode(uint64(dataLen))
	if err != nil {
		return nil, fmt.Errorf("failed to encode string length: %w", err)
	}
	buf := make([]byte, len(lenBytes)+dataLen)
	copy(buf, lenBytes)
	copy(buf[len(lenBytes):], data)
	return buf, nil
}

func StringDecode(r io.Reader) (string, error) {
	dataLen, err := Uint64Decode(r)
	if err != nil {
		return "", fmt.Errorf("failed to decode string length:2 %w", err)
	}
	if dataLen == 0 {
		return "", nil
	}
	stringData, err := DoRead(dataLen, r)
	if err != nil {
		return "", fmt.Errorf("failed to read string data: %w", err)
	}
	return string(stringData), nil
}
