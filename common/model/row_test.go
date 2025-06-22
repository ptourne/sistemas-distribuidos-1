package model

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionLog(t *testing.T) {

	t.Run("Test EqualsRows", func(t *testing.T) {
		// Test con rows idénticos
		row1 := &Row{
			Type:     QueryRow,
			Numerics: map[string]uint64{"a": 1, "b": 2},
			Strings:  map[string]string{"x": "hello", "y": "world"},
			Arrays:   map[string][]string{"tags": {"tag1", "tag2"}},
			Floats:   map[string]float64{"score": 3.14},
		}
		row2 := &Row{
			Type:     QueryRow,
			Numerics: map[string]uint64{"a": 1, "b": 2},
			Strings:  map[string]string{"x": "hello", "y": "world"},
			Arrays:   map[string][]string{"tags": {"tag1", "tag2"}},
			Floats:   map[string]float64{"score": 3.14},
		}
		assert.True(t, EqualsRows(row1, row2))

		// Test con rows diferentes en numerics
		row3 := &Row{
			Type:     QueryRow,
			Numerics: map[string]uint64{"a": 1, "b": 3}, // diferente valor
			Strings:  map[string]string{"x": "hello", "y": "world"},
			Arrays:   map[string][]string{"tags": {"tag1", "tag2"}},
			Floats:   map[string]float64{"score": 3.14},
		}
		assert.False(t, EqualsRows(row1, row3))

		// Test con rows diferentes en strings
		row4 := &Row{
			Type:     QueryRow,
			Numerics: map[string]uint64{"a": 1, "b": 2},
			Strings:  map[string]string{"x": "hello", "y": "different"}, // diferente valor
			Arrays:   map[string][]string{"tags": {"tag1", "tag2"}},
			Floats:   map[string]float64{"score": 3.14},
		}
		assert.False(t, EqualsRows(row1, row4))

		// Test con rows diferentes en arrays
		row5 := &Row{
			Type:     QueryRow,
			Numerics: map[string]uint64{"a": 1, "b": 2},
			Strings:  map[string]string{"x": "hello", "y": "world"},
			Arrays:   map[string][]string{"tags": {"tag1", "tag3"}}, // diferente valor
			Floats:   map[string]float64{"score": 3.14},
		}
		assert.False(t, EqualsRows(row1, row5))

		// Test con rows diferentes en floats
		row6 := &Row{
			Type:     QueryRow,
			Numerics: map[string]uint64{"a": 1, "b": 2},
			Strings:  map[string]string{"x": "hello", "y": "world"},
			Arrays:   map[string][]string{"tags": {"tag1", "tag2"}},
			Floats:   map[string]float64{"score": 2.71}, // diferente valor
		}
		assert.False(t, EqualsRows(row1, row6))

		// Test con nil
		assert.True(t, EqualsRows(nil, nil))
		assert.False(t, EqualsRows(row1, nil))
		assert.False(t, EqualsRows(nil, row1))

		// Test con rows vacíos
		emptyRow1 := &Row{}
		emptyRow2 := &Row{}
		assert.True(t, EqualsRows(emptyRow1, emptyRow2))
	})
}

func TestRowEncodeDecode(t *testing.T) {
	t.Run("TestComplexRowEncodeDecode", func(t *testing.T) {
		// Create the same complex row as in the test
		originalRow := &Row{
			Type:     QueryRow,
			Numerics: map[string]uint64{"budget_sum": 123, "count": 456},
			Strings:  map[string]string{"country": "US"},
			Arrays:   map[string][]string{"genres": {"action", "drama"}},
			Floats:   map[string]float64{"rating": 4.5, "score": 8.7},
		}

		// Encode the row
		encoded, err := originalRow.Encode()
		require.NoError(t, err)
		assert.NotNil(t, encoded)
		assert.Greater(t, len(encoded), 0)

		// Decode the row
		decoded, err := originalRow.Decode(encoded)
		require.NoError(t, err)
		assert.NotNil(t, decoded)

		// Verify the decoded row matches the original
		assert.True(t, EqualsRows(originalRow, decoded))
	})

	t.Run("TestRowDecodeFromReader", func(t *testing.T) {
		// Create the same complex row as in the test
		originalRow := &Row{
			Type:     QueryRow,
			Numerics: map[string]uint64{"budget_sum": 123, "count": 456},
			Strings:  map[string]string{"country": "US"},
			Arrays:   map[string][]string{"genres": {"action", "drama"}},
			Floats:   map[string]float64{"rating": 4.5, "score": 8.7},
		}

		// Encode the row
		encoded, err := originalRow.Encode()
		require.NoError(t, err)

		// Create a reader from the encoded data
		reader := bytes.NewReader(encoded)

		// Decode using RowDecode (the function used in transaction_log)
		decoded, err := RowDecode(reader)
		require.NoError(t, err)
		assert.NotNil(t, decoded)

		// Verify the decoded row matches the original
		assert.True(t, EqualsRows(originalRow, decoded))
	})

	t.Run("TestSimpleRowEncodeDecode", func(t *testing.T) {
		// Test with a simpler row
		originalRow := &Row{
			Type:     QueryRow,
			Numerics: map[string]uint64{"id": 1},
			Strings:  map[string]string{"name": "test"},
		}

		// Encode the row
		encoded, err := originalRow.Encode()
		require.NoError(t, err)

		// Decode using RowDecode
		reader := bytes.NewReader(encoded)
		decoded, err := RowDecode(reader)
		require.NoError(t, err)

		// Verify the decoded row matches the original
		assert.True(t, EqualsRows(originalRow, decoded))
	})
}
