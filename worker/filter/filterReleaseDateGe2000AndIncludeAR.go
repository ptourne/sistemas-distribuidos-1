package filter

import (
	"slices"

	"github.com/ptourne/sistemas-distribuidos-1/common"
)

func NewFilterReleaseDateGe2000AndIncludeAR() GenericFilter {
	return GenericFilter{
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
