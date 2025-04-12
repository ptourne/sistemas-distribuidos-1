package filter

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
)

type FilterProductionCountriesLen1 struct{}

// func (f FilterProductionCountriesLen1) Process(row common.Row) (*common.Row, error) {
func (f FilterProductionCountriesLen1) Process(row common.Row) *common.Row {
	if val, ok := row.Arrays["production_countries"]; ok {
		if !(len(val) == 1) {
			return nil
		}
	} else {
		// return MissingFieldError{
		// 	Field: "production_countries",
		// },
		return nil
	}
	return &common.Row{
		Strings: map[string]string{
			"movieID": row.Strings["movieID"],
			"title":   row.Strings["title"],
			"country": row.Arrays["production_countries"][0],
		},
		Numerics: map[string]uint{
			"budget": row.Numerics["budget"],
		},
	}
}
