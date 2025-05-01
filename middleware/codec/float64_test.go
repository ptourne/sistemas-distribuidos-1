package codec

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFloat64Codec(t *testing.T) {
	tests := []struct {
		name  string
		input float64
	}{
		{"zero", 0.0},
		{"positive", 123.456},
		{"negative", -789.012},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := Float64Encode(test.input)
			assert.NoError(t, err, "Float64Encode failed: %v", err)

			garbage := []byte("garbage")
			blob := append(encoded, garbage...)
			r := bytes.NewReader(blob)

			decoded, err := Float64Decode(r)
			assert.NoError(t, err, "Float64Decode failed: %v", err)

			assert.Equal(t, test.input, decoded, "Decoded value does not match original")
			assert.Equal(t, r.Len(), len(garbage), "Decoder consumed more bytes than expected")
		})
	}
}
