package filter

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

func NewFilterAvgRate(input string, subscribers []string) task.Task {
	return &GenericFilter{
		FilterName:        "filter_avg_rate",
		InputName:         input,
		Conditions:        []Condition{},
		KeptStringFields:  []string{"sentiment"},
		KeptNumericFields: []string{},
		KeptFloatFields:   []string{},
		KeptArrayFields:   []string{},
		Maps:              []Map{MapAvgRate{}},
		Subscribers:       subscribers,
	}
}

type MapAvgRate struct {
}

func (m MapAvgRate) Transform(input *common.Row, output *common.Row) error {
	log.Infof("Transforming row avg: %v", input)
	output.Floats["avg_rate"] = input.Floats["rate"] / float64(input.Numerics["count"])
	return nil
}
