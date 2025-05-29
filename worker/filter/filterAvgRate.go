package filter

import (
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

func NewFilterAvgRate(input string, subscribers []string, cantConsumersSender uint, cantWorkers uint) task.Task[*model.Row, *model.Row] {
	return &GenericFilter{
		name:                "filter_avg_rate",
		input:               input,
		Conditions:          []Condition{},
		KeptStringFields:    []string{"sentiment"},
		KeptNumericFields:   []string{},
		KeptFloatFields:     []string{},
		KeptArrayFields:     []string{},
		Maps:                []Map{MapAvgRate{}},
		subscribers:         subscribers,
		cantConsumersSender: cantConsumersSender,
		cantWorkers:         cantWorkers,
	}
}

type MapAvgRate struct {
}

func (m MapAvgRate) Transform(input *model.Row, output *model.Row) error {
	output.Floats["avg_rate"] = input.Floats["rate"] / float64(input.Numerics["count"])
	return nil
}
