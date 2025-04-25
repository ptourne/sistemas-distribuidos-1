package filter

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

func NewFilterAvgRating(input string, subscribers []string) task.Task[common.Row, common.Row] {
	return &GenericFilter{
		name:              "filter_avg_rating",
		input:             input,
		Conditions:        []Condition{},
		KeptStringFields:  []string{"movieID"},
		KeptNumericFields: []string{},
		KeptFloatFields:   []string{},
		KeptArrayFields:   []string{},
		Maps:              []Map{MapAvgRating{}},
		subscribers:       subscribers,
	}
}

type MapAvgRating struct {
}

func (m MapAvgRating) Transform(input *common.Row, output *common.Row) error {
	output.Floats["avg_rating"] = (float64(input.Numerics["rating"]) / float64(input.Numerics["count"])) / float64(10)
	return nil
}
