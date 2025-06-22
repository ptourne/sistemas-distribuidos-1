package codec

import (
	"encoding/binary"
	"fmt"
	"io"
)

const USE_VARIABLE_LEN_NUMBERS = false

func Uint64Encode(data uint64) ([]byte, error) {
	if USE_VARIABLE_LEN_NUMBERS {
		return uint64EncodeVariableLength(data)
	}
	return uint64EncodeFixedLength(data)
}

func Uint64Decode(r io.Reader) (uint64, error) {
	if USE_VARIABLE_LEN_NUMBERS {
		return uint64DecodeVariableLength(r)
	}
	len, err := DoRead(8, r)
	if err != nil {
		if err.Error() == "EOF" {
			return 0, err
		}
		return 0, fmt.Errorf("failed to read data: %w", err)
	}
	return uint64DecodeFixedLength(len)
}

func uint64EncodeFixedLength(data uint64) ([]byte, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, data)
	return buf, nil
}

func uint64DecodeFixedLength(data []byte) (uint64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("invalid length")
	}
	return binary.BigEndian.Uint64(data), nil
}

const BYTE_VALUE = 0b01111111
const BYTE_FLAG = 0b10000000

func uint64EncodeVariableLength(data uint64) ([]byte, error) {
	if data == 0 {
		return []byte{0}, nil
	}
	var buf []byte
	for data > 0 {
		byte := byte(data & BYTE_VALUE)
		data >>= 7
		if data > 0 {
			byte |= BYTE_FLAG
		}
		buf = append(buf, byte)
	}
	return buf, nil
}

func uint64DecodeVariableLength(r io.Reader) (uint64, error) {
	var result uint64
	i := 0
	for {
		buf, err := DoRead(1, r)
		if err != nil {
			return 0, err
		}

		b := buf[0]
		result |= uint64(b&BYTE_VALUE) << (7 * i)
		if b&BYTE_FLAG == 0 {
			return result, nil
		}
		if i == 9 {
			return 0, fmt.Errorf("invalid length")
		}
		i++
	}
}
