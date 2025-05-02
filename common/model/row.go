package model

import (
	"bytes"
	"fmt"
	"io"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type Row struct {
	Numerics map[string]uint64
	Strings  map[string]string
	Arrays   map[string][]string
	Floats   map[string]float64
	Type     TypeRow
}

type TypeRow uint8

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
	typeBytes, err := codec.Uint8Encode(uint8(r.Type))
	if err != nil {
		return nil, fmt.Errorf("error encoding type: %w", err)
	}
	numerics, err := codec.MapEncode(r.Numerics, codec.Uint64Encode)
	if err != nil {
		return nil, fmt.Errorf("error encoding numerics: %w", err)
	}
	strings, err := codec.MapEncode(r.Strings, codec.StringEncode)
	if err != nil {
		return nil, fmt.Errorf("error encoding strings: %w", err)
	}
	arrays, err := codec.MapEncode(r.Arrays, func(arr []string) ([]byte, error) {
		return codec.ArrayEncode(arr, codec.StringEncode)
	})
	if err != nil {
		return nil, fmt.Errorf("error encoding arrays: %w", err)
	}
	floats, err := codec.MapEncode(r.Floats, codec.Float64Encode)
	if err != nil {
		return nil, fmt.Errorf("error encoding floats: %w", err)
	}
	return bytes.Join([][]byte{typeBytes, numerics, strings, arrays, floats}, []byte{}), nil
}

func (r *Row) Decode(data []byte) (*Row, error) {
	reader := bytes.NewReader(data)
	rowType, err := codec.Uint8Decode(reader)
	if err != nil {
		return nil, fmt.Errorf("error decoding type: %w", err)
	}
	numerics, err := codec.MapDecode(reader, codec.Uint64Decode)
	if err != nil {
		return nil, fmt.Errorf("error decoding numerics: %w", err)
	}
	strings, err := codec.MapDecode(reader, codec.StringDecode)
	if err != nil {
		return nil, fmt.Errorf("error decoding strings: %w", err)
	}
	arrays, err := codec.MapDecode(reader, func(r io.Reader) ([]string, error) {
		return codec.ArrayDecode(r, codec.StringDecode)
	})
	if err != nil {
		return nil, fmt.Errorf("error decoding arrays: %w", err)
	}
	floats, err := codec.MapDecode(reader, codec.Float64Decode)
	if err != nil {
		return nil, fmt.Errorf("error decoding floats: %w", err)
	}
	return &Row{
		Type:     TypeRow(rowType),
		Numerics: numerics,
		Strings:  strings,
		Arrays:   arrays,
		Floats:   floats,
	}, nil
}
