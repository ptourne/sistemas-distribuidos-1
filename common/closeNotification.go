package common
type CloseNotification struct{
	T 	closeNotificationType
	Cid string
}

type closeNotificationType int
const (
	Finish closeNotificationType = iota
	FinishDone
)