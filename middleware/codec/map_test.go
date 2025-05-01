package codec

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMapCodec(t *testing.T) {
	tests := []struct {
		name  string
		input map[string]string
	}{
		{"empty", map[string]string{}},
		{"single", map[string]string{"key": "value"}},
		{"multiple", map[string]string{"key1": "value1", "key2": "value2"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			encoded, err := MapEncode(test.input, StringEncode)
			assert.NoError(t, err, "MapEncode failed: %v", err)

			garbage := []byte("garbage")
			blob := append(encoded, garbage...)
			r := bytes.NewReader(blob)

			decoded, err := MapDecode(r, StringDecode)
			assert.NoError(t, err, "MapDecode failed: %v", err)

			assert.Equal(t, test.input, decoded)
			assert.Equal(t, r.Len(), len(garbage), "Decoder consumed more bytes than expected")
		})
	}
}
