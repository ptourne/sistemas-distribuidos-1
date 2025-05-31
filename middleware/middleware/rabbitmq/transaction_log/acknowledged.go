package transaction_log

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type acknowledged struct {
	id uint64
}

func (a acknowledged) Encode() []byte {
	const headerSize = 1 + // log type
		8 // id
	buf := make([]byte, headerSize)
	buf[0] = byte(LogType_Acknowledged)
	binary.BigEndian.PutUint64(buf[1:], a.id)
	return buf
}

func (r *acknowledged) Decode(reader io.Reader) error {
	id, err := codec.DoRead(8, reader)
	if err != nil {
		fmt.Errorf("failed to read log type: %w", err)
	}
	r.id = binary.BigEndian.Uint64(id)
	return nil
}
