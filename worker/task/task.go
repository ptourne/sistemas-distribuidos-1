package task

import "github.com/ptourne/sistemas-distribuidos-1/common"

type Task struct {
	Input     string
	Name      string
	Operation Operation
}

type Operation interface {
	Process(row common.Row) *common.Row
}

func NewTask(input string, name string, operation Operation) Task {
	return Task{
		Input:     input,
		Name:      name,
		Operation: operation,
	}
}
