package model

import (
	"testing"

	"github.com/stretchr/testify/assert"
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

		// Test con rows diferentes en Type
		row7 := &Row{
			Type:     QueryName, // diferente tipo
			Numerics: map[string]uint64{"a": 1, "b": 2},
			Strings:  map[string]string{"x": "hello", "y": "world"},
			Arrays:   map[string][]string{"tags": {"tag1", "tag2"}},
			Floats:   map[string]float64{"score": 3.14},
		}
		assert.False(t, EqualsRows(row1, row7))

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
