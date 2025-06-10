package transaction_log

type acknowledged struct {
}

func (a acknowledged) Encode() []byte {
	const headerSize = 1 // log type
	buf := make([]byte, headerSize)
	buf[0] = byte(LogType_Acknowledged)
	return buf
}
