package transaction_log

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type received struct {
	id   uint64
	data []byte
}

func (r received) Encode() []byte {
	dataLen := len(r.data)
	if dataLen > 0xFFFFFFFF {
		panic("data length exceeds maximum size")
	}
	const headerSize = 1 + // log type
		8 + // id
		4 // data length
	buf := make([]byte, headerSize+dataLen)
	buf[0] = byte(LogType_Received)
	binary.BigEndian.PutUint64(buf[1:], r.id)
	binary.BigEndian.PutUint32(buf[1+8:], uint32(dataLen))
	copy(buf[headerSize:], r.data)
	return buf
}

func (r *received) Decode(reader io.Reader) error {
	id, err := codec.DoRead(8, reader)
	if err != nil {
		fmt.Errorf("failed to read log type: %w", err)
	}
	r.id = binary.BigEndian.Uint64(id)
	dataLen, err := codec.DoRead(4, reader)
	if err != nil {
		return fmt.Errorf("failed to read log type: %w", err)
	}
	dataLenInt := binary.BigEndian.Uint32(dataLen)
	r.data, err = codec.DoRead(uint64(dataLenInt), reader)
	if err != nil {
		return fmt.Errorf("failed to read log type: %w", err)
	}
	return nil
}
