package filter

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

func NewFilterAvgRate(input string, subscribers []string) task.Task {
	return &GenericFilter{
		name:              "filter_avg_rate",
		input:             input,
		Conditions:        []Condition{},
		KeptStringFields:  []string{"sentiment"},
		KeptNumericFields: []string{},
		KeptFloatFields:   []string{},
		KeptArrayFields:   []string{},
		Maps:              []Map{MapAvgRate{}},
		subscribers:       subscribers,
	}
}


type MapAvgRate struct {
}

func (m MapAvgRate) Transform(input *common.Row, output *common.Row) error {
	output.Floats["avg_rate"] = input.Floats["rate"] / float64(input.Numerics["count"])
	return nil
}

