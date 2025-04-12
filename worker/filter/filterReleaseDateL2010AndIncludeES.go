package filter

import (
	"log"
	"slices"

	"github.com/ptourne/sistemas-distribuidos-1/common"
)

type FilterReleaseDateL2010AndIncludeES struct{}

func (f FilterReleaseDateL2010AndIncludeES) Process(row common.Row) *common.Row {
	log.Printf("MSG FROM FILTER ES: title: %v release_date: %v prod: %v", row.Strings["title"], row.Numerics["release_date"], row.Arrays["production_countries"])
	if val, ok := row.Numerics["release_date"]; ok {
		if !(val < 2010) {
			return nil
		}
	} else {
		return nil
	}
	if val, ok := row.Arrays["production_countries"]; ok {
		if !slices.Contains(val, "ES") {
			return nil
		}
	} else {
		return nil
	}
	return &common.Row{
		Strings: map[string]string{
			"title": row.Strings["title"],
		},
		Arrays: map[string][]string{
			"genres": row.Arrays["genres"],
		},
	}
}
