package transaction_log

type LogType rune

const (
	LogType_Received     LogType = 'R'
	LogType_Acknowledged LogType = 'A'
	LogType_ReceivedEOF  LogType = 'E'
)
