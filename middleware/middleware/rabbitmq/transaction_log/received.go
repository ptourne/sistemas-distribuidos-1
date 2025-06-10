package transaction_log

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type received struct {
	cid  uint64
	id   uint64
	data []byte
}

func (r received) Encode() []byte {
	dataLen := len(r.data)
	if dataLen > 0xFFFFFFFF {
		panic("data length exceeds maximum size")
	}
	var headerSize = 1 + // log type
		8 + // cid
		8 + // id
		4 // data length

	bufLen := headerSize + dataLen
	buf := make([]byte, bufLen)
	buf[0] = byte(LogType_Received)
	binary.BigEndian.PutUint64(buf[1:], r.cid)
	binary.BigEndian.PutUint64(buf[1+8:], r.id)
	binary.BigEndian.PutUint32(buf[1+8+8:], uint32(dataLen))
	copy(buf[headerSize:], r.data)

	return buf
}

func (r *received) Decode(reader io.Reader) error {
	var err error
	r.cid, err = codec.Uint64Decode(reader)
	if err != nil {
		return fmt.Errorf("failed to read msg cid: %w", err)
	}
	r.id, err = codec.Uint64Decode(reader)
	if err != nil {
		return fmt.Errorf("failed to read msg id: %w", err)
	}
	dataLen, err := codec.DoRead(4, reader)
	if err != nil {
		return fmt.Errorf("failed to read data length: %w", err)
	}
	dataLenInt := binary.BigEndian.Uint32(dataLen)
	r.data, err = codec.DoRead(uint64(dataLenInt), reader)
	if err != nil {
		return fmt.Errorf("failed to read data: %w", err)
	}
	return nil
}
