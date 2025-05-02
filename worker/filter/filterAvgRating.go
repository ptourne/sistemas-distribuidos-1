package filter

import (
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

func NewFilterAvgRating(input string, subscribers []string) task.Task[*model.Row, *model.Row] {
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

func (m MapAvgRating) Transform(input *model.Row, output *model.Row) error {
	output.Floats["avg_rating"] = (float64(input.Numerics["rating"]) / float64(input.Numerics["count"])) / float64(10)
	return nil
}
