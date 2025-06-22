package transaction_log

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type receivedPrune struct {
	cid uint64
}

func (a receivedPrune) Encode() []byte {
	const headerSize = 1 + // log type
		8 // cid
	buf := make([]byte, headerSize)
	buf[0] = byte(LogType_ReceivedPrune)
	binary.BigEndian.PutUint64(buf[1:], a.cid)
	return buf
}

func (r *receivedPrune) Decode(reader io.Reader) error {
	var err error
	r.cid, err = codec.Uint64Decode(reader)
	if err != nil {
		return fmt.Errorf("failed to read cid: %w", err)
	}
	return nil
}
