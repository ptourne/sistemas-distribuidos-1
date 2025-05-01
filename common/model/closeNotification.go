package model

type CloseNotification struct{
	T 	closeNotificationType
	Cid string
}

type closeNotificationType int
const (
	Finish closeNotificationType = iota
	FinishDone
)


func (c CloseNotification) Encode() ([]byte, error) {
	return nil, nil
}

func (c CloseNotification) Decode(data []byte) error {
	return nil
}
