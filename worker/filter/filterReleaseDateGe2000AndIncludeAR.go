package filter

import (
	"slices"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

func NewFilterReleaseDateGe2000AndIncludeAR(input task.Task) task.Task {
	return &GenericFilter{
		name:              "filter_release_date_ge_2000_and_include_ar",
		input:             input,
		Conditions:        []Condition{NumericCondition{"release_date", GreaterThanOrEqual, 2000}, ArrayIncludes{"production_countries", "AR"}},
		KeptStringFields:  []string{"movieID", "title"},
		KeptNumericFields: []string{"release_date"},
		KeptFloatFields:   []string{},
		KeptArrayFields:   []string{"production_countries", "genres"},
		Maps:              []Map{MapProductionCountriesa{}},
	}
}

type MapProductionCountriesa struct {
}

func (m MapProductionCountriesa) Transform(input *common.Row, output *common.Row) error {
	output.Strings["country"] = input.Arrays["production_countries"][0]
	return nil
}

type ArrayIncludes struct {
	Column   string
	Expected string
}

func (c ArrayIncludes) Passes(row common.Row) (bool, error) {
	if val, ok := row.Arrays[c.Column]; ok {
		return slices.Contains(val, c.Expected), nil
	}
	return false, nil
}
