package codec

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestArrayCodec(t *testing.T) {
	tests := []struct {
		name  string
		value []uint64
	}{
		{"empty", []uint64{}},
		{"single element", []uint64{1}},
		{"multiple elements", []uint64{1, 2, 3}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := ArrayEncode(test.value, Uint64Encode)
			assert.NoError(t, err, "ArrayEncode failed: %v", err)

			garbage := []byte("garbage")
			blob := append(encoded, garbage...)
			r := bytes.NewReader(blob)

			decoded, err := ArrayDecode(r, Uint64Decode)
			assert.NoError(t, err, "ArrayDecode failed: %v", err)

			assert.Equal(t, test.value, decoded)
			assert.Equal(t, r.Len(), len(garbage), "Decoder consumed more bytes than expected")
		})
	}
}
