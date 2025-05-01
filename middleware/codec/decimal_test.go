package codec

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDecimalPlus(t *testing.T) {
	tests := []struct {
		name     string
		d1       Decimal
		d2       Decimal
		expected Decimal
	}{
		{"zeros", Decimal{}, Decimal{}, Decimal{}},
		{"integer", Decimal{1, 0, 0}, Decimal{2, 0, 0}, Decimal{3, 0, 0}},
		{"one decimal digit", Decimal{0, 1, 1}, Decimal{0, 2, 1}, Decimal{0, 3, 1}},
		{"two decimal digits", Decimal{0, 1, 2}, Decimal{0, 2, 2}, Decimal{0, 3, 2}},
		{"different decimal digits", Decimal{0, 1, 1}, Decimal{0, 2, 2}, Decimal{0, 12, 2}},
		{"mixed", Decimal{1, 50, 2}, Decimal{2, 75, 2}, Decimal{4, 25, 2}},
		{"addition with carry", Decimal{999, 99, 2}, Decimal{1, 1, 2}, Decimal{1001, 0, 2}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := test.d1.Plus(test.d2)
			if result != test.expected {
				t.Errorf("expected %s, got %s", test.expected.Debug(), result.Debug())
			}
		})
	}
}

func TestDecimalCodec(t *testing.T) {
	tests := []struct {
		name  string
		value Decimal
	}{
		{"zero", Decimal{}},
		{"integer", Decimal{1, 0, 0}},
		{"one decimal digit", Decimal{0, 1, 1}},
		{"two decimal digits", Decimal{0, 1, 2}},
		{"two byte fractional", Decimal{0, 0x0102, 3}},
		{"mixed", Decimal{1, 50, 3}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fmt.Printf("\n==Testing %s==\n", test.name)
			encoded, err := test.value.Encode()
			assert.NoError(t, err, "Encode failed: %v", err)

			garbage := []byte("garbage")
			blob := append(encoded, garbage...)
			r := bytes.NewReader(blob)

			decoded := NewDecimal(test.value.FractionDigits)
			err = decoded.Decode(r)
			assert.NoError(t, err, "Decode failed: %v", err)

			assert.Equal(t, test.value, *decoded)
			fmt.Printf("Encoded:\n%x\n               /\\", encoded)
			assert.Equal(t, r.Len(), len(garbage), "Decoder consumed more bytes than expected")
		})
	}
}
