package codec

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestStringCodec(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"Empty", ""},
		{"One", "a"},
		{"Two", "ab"},
		{"Three", "abc"},
		{"Four", "abcd"},
		{"Five", "abcde"},
		{"Six", "abcdef"},
		{"Seven", "abcdefg"},
		{"Eight", "abcdefgh"},
		{"MaxString", "abcdefghijklmnopqrstuvwxyz"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := StringEncode(test.input)
			assert.NoError(t, err, "StringEncode failed: %v", err)

			garbage := []byte("garbage")
			blob := append(encoded, garbage...)
			r := bytes.NewReader(blob)

			decoded, err := StringDecode(r)
			assert.NoError(t, err, "StringDecode failed: %v", err)

			assert.Equal(t, test.input, decoded, "Decoded value does not match original")
			assert.Equal(t, r.Len(), len(garbage), "Decoder consumed more bytes than expected")
		})
	}

}
