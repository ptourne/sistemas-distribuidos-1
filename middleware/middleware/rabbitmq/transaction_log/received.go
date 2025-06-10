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
	t    ReceivedType
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
		1 // received type
	if r.t == ReceivedType_Normal {
		headerSize += 4 // data length
	}
	bufLen := headerSize
	if r.t == ReceivedType_Normal {
		bufLen += dataLen
	}
	buf := make([]byte, bufLen)
	buf[0] = byte(LogType_Received)
	binary.BigEndian.PutUint64(buf[1:], r.cid)
	binary.BigEndian.PutUint64(buf[1+8:], r.id)
	buf[1+8+8] = byte(r.t)
	if r.t == ReceivedType_Normal {
		binary.BigEndian.PutUint32(buf[1+8+8+1:], uint32(dataLen))
		copy(buf[headerSize:], r.data)
	}
	return buf
}

type ReceivedType rune

const (
	ReceivedType_Normal ReceivedType = 'N'
	ReceivedType_EOF    ReceivedType = 'E'
)

func (r *received) Decode(reader io.Reader) error {
	var err error
	r.cid, err = codec.Uint64Decode(reader)
	if err != nil {
		fmt.Errorf("failed to read msg cid: %w", err)
	}
	r.id, err = codec.Uint64Decode(reader)
	if err != nil {
		fmt.Errorf("failed to read msg id: %w", err)
	}
	typeR, err := codec.DoRead(1, reader)
	if err != nil {
		return fmt.Errorf("failed to read log type: %w", err)
	}
	switch ReceivedType(typeR[0]) {
	case ReceivedType_Normal:
		r.t = ReceivedType_Normal
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
	case ReceivedType_EOF:
		r.data = nil
		r.t = ReceivedType_EOF
		return nil
	default:
		return fmt.Errorf("unknown received type: %v", typeR[0])
	}
}
