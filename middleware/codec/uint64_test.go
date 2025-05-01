package codec

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUint64Codec(t *testing.T) {
	tests := []struct {
		name  string
		input uint64
	}{
		{"Zero", 0},
		{"One", 1},
		{"Two", 2},
		{"Three", 3},
		{"TwoBytes", 0x0102},
		{"ThreeBytes", 0x010203},
		{"FourBytes", 0x01020304},
		{"FiveBytes", 0x0102030405},
		{"SixBytes", 0x010203040506},
		{"SevenBytes", 0x01020304050607},
		{"EightBytes", 0x0102030405060708},
		{"MaxUint64", 0xFFFFFFFFFFFFFFFF},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := Uint64Encode(test.input)
			assert.NoError(t, err, "Uint64Encode failed: %v", err)

			garbage := []byte("garbage")
			blob := append(encoded, garbage...)
			r := bytes.NewReader(blob)

			decoded, err := Uint64Decode(r)
			assert.NoError(t, err, "Uint64Decode failed: %v", err)

			assert.Equal(t, test.input, decoded, "Decoded value does not match original")
			assert.Equal(t, r.Len(), len(garbage), "Decoder consumed more bytes than expected")
		})
	}

}
