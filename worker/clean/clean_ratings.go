package clean

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

type CleanRatings struct{ input task.Task }

func NewCleanRatings(input task.Task) task.Task {
	return &CleanRatings{input}
}

func (f CleanRatings) Input() string {
	return f.input.Name()
}

func (f CleanRatings) Name() string {
	return "clean_ratings"
}

func (f CleanRatings) Process(row common.Row) *common.Row {
	requiredFields := []string{
		row.Strings["movieID"],
		row.Strings["rating"],
	}

	log.Debugf("Clean: movieID: %s, rating: %s",
		row.Strings["movieID"],
		row.Strings["rating"],
	)

	for _, field := range requiredFields {
		if utils.MustDropRow(field) {
			log.Debugf("warning: dropping row due to empty field: %s", field)
			return nil
		}
	}

	rating, ok := utils.ParseFloat(row.Strings["rating"])
	if !ok {
		log.Warnf("could not parse rating: %s", row.Strings["rating"])
		return nil
	}

	log.Debugf("Clean ALL: movieID: %s, rating: %v", row.Strings["movieID"], rating)

	return &common.Row{
		Strings: map[string]string{
			"movieID": row.Strings["movieID"],
		},
		Floats: map[string]float64{
			"rating": rating,
		},
	}
}
