package model

import (
	"bytes"
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type Row struct {
	Numerics map[string]uint64
	Strings  map[string]string
	Arrays   map[string][]string
	Floats   map[string]float64
	Type     TypeRow
}

type TypeRow int

const (
	QueryName TypeRow = iota
	QueryRow
	FinishQuerys
)

func RowQueryName(query string) *Row {
	return &Row{Strings: map[string]string{"type": query}, Type: QueryName}
}

func RowQuery(row Row) *Row {
	row.Type = QueryRow
	return &row
}

func (r Row) Encode() ([]byte, error) {
	numerics, err := codec.MapEncode(r.Numerics, codec.Uint64Encode)
	if err != nil {
		return nil, fmt.Errorf("error encoding numerics: %w", err)
	}
	strings, err := codec.MapEncode(r.Strings, codec.StringEncode)
	if err != nil {
		return nil, fmt.Errorf("error encoding strings: %w", err)
	}
	arrays, err := codec.MapEncode(r.Arrays, codec.ArrayEncode)
	if err != nil {
		return nil, fmt.Errorf("error encoding arrays: %w", err)
	}
	floats, err := codec.MapEncode(r.Floats, codec.Float64Encode)
	if err != nil {
		return nil, fmt.Errorf("error encoding floats: %w", err)
	}
	return bytes.Join([][]byte{numerics, strings, arrays, floats}, []byte{}), nil
}

func (r *Row) Decode(data []byte) error {
	r := bytes.NewReader(data)
	// codec.MapDecode(r,

}
