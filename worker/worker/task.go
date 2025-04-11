package worker

import "github.com/ptourne/sistemas-distribuidos-1/common"

type Task struct {
	input     string
	name      string
	operation Operation
}

type Operation interface {
	Process(row common.Row) *common.Row
}
