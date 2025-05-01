package model

import (
	"fmt"
	"io"

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

func (c *FileChunk) Decode(r io.Reader) error {
	len, err := codec.Uint64Decode(r)
	if err != nil {
		return fmt.Errorf("failed to decode length: %w", err)
	}
	data, err := codec.DoRead(len, r)
	if err != nil {
		return fmt.Errorf("failed to read data: %w", err)
	}
	c.Bytes = data
	return nil
}
