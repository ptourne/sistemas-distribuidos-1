package transaction_log

type LogType rune

const (
	LogType_ReceivedNormal LogType = 'N'
	LogType_ReceivedPrune  LogType = 'P'
	LogType_Acknowledged   LogType = 'A'
	LogType_ReceivedEOF    LogType = 'E'
)
