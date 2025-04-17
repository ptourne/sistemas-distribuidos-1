package task

import "github.com/ptourne/sistemas-distribuidos-1/common"

type Operation interface {
	Process(row common.Row) *common.Row
}

type Task interface {
	Process(row common.Row) *common.Row
	Input() string
	Name() string
}
