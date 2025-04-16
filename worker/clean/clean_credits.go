package clean

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

type CleanCredits struct{ input task.Task }

func NewCleanCredits(input task.Task) task.Task {
	return &CleanCredits{input}
}

func (f CleanCredits) Input() string {
	return f.input.Name()
}

func (f CleanCredits) Name() string {
	return "clean_credits"
}

func (f CleanCredits) Process(row common.Row) *common.Row {
	requiredFields := []string{
		row.Strings["ID"],
		row.Strings["cast"],
	}

	log.Debugf("Clean: ID: %s, cast: %s",
		row.Strings["ID"],
		row.Strings["cast"],
	)

	for _, field := range requiredFields {
		if utils.MustDropRow(field) {
			log.Debugf("warning: dropping row due to empty field: %s", field)
			return nil
		}
	}

	cast, err := utils.DictionaryToListName(row.Strings["cast"])
	if err != nil {
		log.Warnf("err: %v, could not parse cast: %s", err, row.Strings["cast"])
		return nil
	}

	log.Debugf("Clean ALL: ID: %s, cast: %v", row.Strings["ID"], cast)

	return &common.Row{
		Strings: map[string]string{
			"ID": row.Strings["ID"],
		},
		Arrays: map[string][]string{
			"cast": cast,
		},
	}
}
