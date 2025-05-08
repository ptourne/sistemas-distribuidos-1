package model

import (
	"bytes"
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type SlowFileChunk struct {
	Bytes []byte
}

func (c SlowFileChunk) Encode() ([]byte, error) {
	len, err := codec.Uint64Encode(uint64(len(c.Bytes)))
	if err != nil {
		return nil, fmt.Errorf("failed to encode length: %w", err)
	}
	return append(len, c.Bytes...), nil
}

func (c *SlowFileChunk) DecodeReader(r *bytes.Reader) (*SlowFileChunk, error) {
	len, err := codec.Uint64Decode(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode length: %w", err)
	}
	bytes, err := codec.DoRead(len, r)
	if err != nil {
		return nil, fmt.Errorf("failed to read bytes: %w", err)
	}
	return &SlowFileChunk{Bytes: bytes}, nil
}

func (c *SlowFileChunk) Decode(data []byte) (*SlowFileChunk, error) {
	r := bytes.NewReader(data)
	len, err := codec.Uint64Decode(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode length: %w", err)
	}
	bytes, err := codec.DoRead(len, r)
	if err != nil {
		return nil, fmt.Errorf("failed to read bytes: %w", err)
	}
	return &SlowFileChunk{Bytes: bytes}, nil
}

type FileChunk struct {
	Bytes []byte
}

func (c FileChunk) Encode() ([]byte, error) {
	return c.Bytes, nil
}

func (c *FileChunk) Decode(data []byte) (*FileChunk, error) {
	return &FileChunk{Bytes: data}, nil
}
