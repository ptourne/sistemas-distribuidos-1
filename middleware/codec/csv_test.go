package codec

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCsvRecordEncodeDecode(t *testing.T) {
	tests := []struct {
		name  string
		input []string
	}{
		{
			name:  "Empty record",
			input: []string{},
		},
		{
			name:  "Single field",
			input: []string{"field1"},
		},
		{
			name:  "Multiple fields",
			input: []string{"field1", "field2", "field3"},
		},
		{
			name:  "Fields with comma",
			input: []string{"field,1", "field2"},
		},
		{
			name:  "Fields with quotes",
			input: []string{`field"1"`, "field2"},
		},
		{
			name:  "Fields with newline",
			input: []string{"field1\nfield1b", "field2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := CsvRecordEncode(tt.input)
			assert.NoError(t, err)
			assert.NotNil(t, encoded)
			r := bytes.NewReader(encoded)
			decoded, err := CsvRecordDecode(r)
			assert.NoError(t, err)
			assert.Equal(t, tt.input, decoded)
		})
	}
}
