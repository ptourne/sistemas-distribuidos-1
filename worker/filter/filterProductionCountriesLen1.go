package filter

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
)

func NewFilterProductionCountriesLen1() GenericFilter {
	return GenericFilter{
		Conditions:        []Condition{SingleProductionCountryCondition{}},
		KeptStringFields:  []string{"movieID", "title"},
		KeptNumericFields: []string{"budget"},
		KeptFloatFields:   []string{},
		KeptArrayFields:   []string{},
		Maps:              []Map{MapProductionCountries{}},
	}
}

// func (f FilterProductionCountriesLen1) Process(row common.Row) (*common.Row, error) { // TODO add error to interface
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
