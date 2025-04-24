package filter

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

func NewFilterProductionCountriesLen1(input string, subscribers []string) task.Task[common.Row, common.Row] {
	return &GenericFilter{
		name:              "filter_one_production_country",
		input:             input,
		Conditions:        []Condition{SingleProductionCountryCondition{}, NumericCondition{"budget", GreaterThan, 0}},
		KeptStringFields:  []string{"movieID", "title"},
		KeptNumericFields: []string{"budget"},
		KeptFloatFields:   []string{},
		KeptArrayFields:   []string{},
		Maps:              []Map{MapProductionCountries{}},
		subscribers:       subscribers,
	}
}

type SingleProductionCountryCondition struct {
}

func (c SingleProductionCountryCondition) Passes(row common.Row) (bool, error) {
	if val, ok := row.Arrays["production_countries"]; ok {
		return len(val) == 1, nil
	}
	return false, &MissingFieldError{"production_countries"}
}

type MapProductionCountries struct {
}

func (m MapProductionCountries) Transform(input *common.Row, output *common.Row) error {
	output.Strings["country"] = input.Arrays["production_countries"][0]
	return nil
}
