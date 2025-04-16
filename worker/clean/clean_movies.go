package clean

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

type CleanMovies struct{ input task.Task }

func NewCleanMovies(input task.Task) task.Task {
	return &CleanMovies{input}
}

func (f CleanMovies) Input() string {
	return f.input.Name()
}

func (f CleanMovies) Name() string {
	return "clean_movies"
}

func (f CleanMovies) Process(row common.Row) *common.Row {
	requiredFields := []string{
		row.Strings["movieID"],
		row.Strings["title"],
		row.Strings["overview"],
		row.Strings["production_countries"],
		row.Strings["genres"],
		row.Strings["release_date"],
		row.Strings["budget"],
		row.Strings["revenue"],
	}

	log.Debugf("Clean: movieID: %s, title: %s, overview: %s, production_countries: %s, genres: %s, release_date: %s, budget: %s, revenue: %s",
		row.Strings["movieID"],
		row.Strings["title"],
		row.Strings["overview"],
		row.Strings["production_countries"],
		row.Strings["genres"],
		row.Strings["release_date"],
		row.Strings["budget"],
		row.Strings["revenue"],
	)

	for _, field := range requiredFields {
		if utils.MustDropRow(field) {
			log.Debugf("warning: dropping row due to empty field: %s", field)
			return nil
		}
	}

	productionCountries, ok := utils.DictionaryToListIso(row.Strings["production_countries"])
	if !ok {
		log.Debugf("warning: could not parse production countries: %s", row.Strings["production_countries"])
		return nil
	}

	genres, err := utils.DictionaryToListName(row.Strings["genres"])
	if err != nil {
		log.Warnf("could not parse genres: %s", row.Strings["genres"])
		return nil
	}

	releaseYear, ok := utils.ExtractYear(row.Strings["release_date"])
	if !ok {
		log.Warnf("could not parse release date: %s", row.Strings["release_date"])
		return nil
	}

	budget, ok := utils.ParseFloat(row.Strings["budget"])
	if !ok {
		log.Warnf("could not parse budget: %s", row.Strings["budget"])
		return nil
	}

	revenue, ok := utils.ParseFloat(row.Strings["revenue"])
	if !ok {
		log.Warnf("could not parse revenue: %s", row.Strings["revenue"])
		return nil
	}

	log.Debugf("Clean ALL: title: %s, production_countries: %v, release_date: %v", row.Strings["title"], productionCountries, releaseYear)

	return &common.Row{
		Strings: map[string]string{
			"movieID":  row.Strings["movieID"],
			"title":    row.Strings["title"],
			"overview": row.Strings["overview"],
		},
		Arrays: map[string][]string{
			"production_countries": productionCountries,
			"genres":               genres,
		},
		Numerics: map[string]uint{
			"release_date": releaseYear,
		},
		Floats: map[string]float64{
			"budget":  budget,
			"revenue": revenue,
		},
	}
}
