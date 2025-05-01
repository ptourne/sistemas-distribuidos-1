package codec

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

func Float64Encode(f float64) ([]byte, error) {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, math.Float64bits(f))
	return buf, nil
}

func Float64Decode(r io.Reader) (float64, error) {
	buf, err := DoRead(8, r)
	if err != nil {
		return 0, fmt.Errorf("error reading float64: %w", err)
	}
	return math.Float64frombits(binary.BigEndian.Uint64(buf)), nil
}
