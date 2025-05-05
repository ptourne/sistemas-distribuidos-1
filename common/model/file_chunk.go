package model

import (
	"bytes"
	"fmt"

	codec "github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type FileChunk struct {
	Bytes []byte
}

func (c FileChunk) Encode() ([]byte, error) {
	len, err := codec.Uint64Encode(uint64(len(c.Bytes)))
	if err != nil {
		return nil, fmt.Errorf("failed to encode length: %w", err)
	}
	return append(len, c.Bytes...), nil
}

func (c *FileChunk) DecodeReader(r *bytes.Reader) (*FileChunk, error) {
	len, err := codec.Uint64Decode(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode length: %w", err)
	}
	bytes, err := codec.DoRead(len, r)
	if err != nil {
		return nil, fmt.Errorf("failed to read bytes: %w", err)
	}
	return &FileChunk{Bytes: bytes}, nil
}

func (c *FileChunk) Decode(data []byte) (*FileChunk, error) {
	r := bytes.NewReader(data)
	len, err := codec.Uint64Decode(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode length: %w", err)
	}
	bytes, err := codec.DoRead(len, r)
	if err != nil {
		return nil, fmt.Errorf("failed to read bytes: %w", err)
	}
	return &FileChunk{Bytes: bytes}, nil
}
