package codec

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBytesDecode(t *testing.T) {
	tests := []struct {
		name    string
		input   []byte
		want    []byte
		wantErr bool
	}{
		{
			name:    "Empty byte slice",
			input:   []byte{},
			want:    []byte{},
			wantErr: false,
		},
		{
			name:    "Single byte",
			input:   []byte{0x01},
			want:    []byte{0x01},
			wantErr: false,
		},
		{
			name:    "Multiple bytes",
			input:   []byte{0x01, 0x02, 0x03, 0x04},
			want:    []byte{0x01, 0x02, 0x03, 0x04},
			wantErr: false,
		},
		{
			name:    "Large byte slice",
			input:   make([]byte, 2048),
			want:    make([]byte, 2048),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := BytesEncode(tt.input)
			assert.NoError(t, err)
			decoded, err := BytesDecode(bytes.NewReader(encoded))
			if (err != nil) != tt.wantErr {
				t.Errorf("BytesDecode() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.want, decoded)
		})
	}
}
