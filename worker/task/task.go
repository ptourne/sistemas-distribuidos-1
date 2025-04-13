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

type OTask struct {
	input     string
	name      string
	Operation Operation
}

func (t OTask) Process(row common.Row) *common.Row {
	return t.Operation.Process(row)
}

func (t OTask) Input() string {
	return t.input
}

func (t OTask) Name() string {
	return t.name
}

func NewTask(input string, name string, operation Operation) Task {
	return OTask{
		input:     input,
		name:      name,
		Operation: operation,
	}
}
