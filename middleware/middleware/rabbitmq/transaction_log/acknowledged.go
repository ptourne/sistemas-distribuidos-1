package transaction_log

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type acknowledged struct {
	cid uint64
	id  uint64
}

func (a acknowledged) Encode() []byte {
	const headerSize = 1 + // log type
		8 + // cid
		8 // id
	buf := make([]byte, headerSize)
	buf[0] = byte(LogType_Acknowledged)
	binary.BigEndian.PutUint64(buf[1:], a.cid)
	binary.BigEndian.PutUint64(buf[1+8:], a.id)
	return buf
}

func (r *acknowledged) Decode(reader io.Reader) error {
	var err error
	r.cid, err = codec.Uint64Decode(reader)
	if err != nil {
		fmt.Errorf("failed to read log type: %w", err)
	}
	r.id, err = codec.Uint64Decode(reader)
	if err != nil {
		fmt.Errorf("failed to read log type: %w", err)
	}
	return nil
}
