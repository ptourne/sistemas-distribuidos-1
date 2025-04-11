package worker

import (
	"slices"

	"github.com/ptourne/sistemas-distribuidos-1/common"
)

type FilterReleaseDateGe2000AndIncludeAR struct{}

func (f FilterReleaseDateGe2000AndIncludeAR) Process(row common.Row) *common.Row {
	if val, ok := row.Numerics["release_date"]; ok {
		if !(val >= 2000) {
			return nil
		}
	} else {
		return nil
	}
	if val, ok := row.Arrays["production_countries"]; ok {
		if !slices.Contains(val, "AR") {
			return nil
		}
	} else {
		return nil
	}
	return &common.Row{
		Strings: map[string]string{
			"movieID": row.Strings["movieID"],
			"title":   row.Strings["title"],
		},
		Arrays: map[string][]string{
			"production_countries": row.Arrays["production_countries"],
			"genres":               row.Arrays["genres"],
		},
		Numerics: map[string]uint{
			"release_date": row.Numerics["release_date"],
		},
	}
}
