package filter

import (
	"log"
	"slices"

	"github.com/ptourne/sistemas-distribuidos-1/common"
)

type FilterReleaseDateGe2000AndIncludeAR struct{}

func (f FilterReleaseDateGe2000AndIncludeAR) Process(row common.Row) *common.Row {
	log.Printf("MSG FROM FILTER ARG: title: %v release_date: %v prod: %v", row.Strings["title"], row.Numerics["release_date"], row.Arrays["production_countries"])

	if val, ok := row.Numerics["release_date"]; ok {
		if !(val >= 2000) {
			log.Println("Filter: release_date < 2000, title:", row.Strings["title"])
			return nil
		}
	} else {
		return nil
	}
	if val, ok := row.Arrays["production_countries"]; ok {
		if !slices.Contains(val, "AR") {
			log.Printf("Filter: production_countries does not contain AR, title: %s", row.Strings["title"])
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
