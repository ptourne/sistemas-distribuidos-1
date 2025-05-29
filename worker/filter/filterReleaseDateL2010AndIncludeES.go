package filter

import (
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

func NewFilterReleaseDateL2010AndIncludeES(input string, subscribers []string, cantConsumersSender uint, cantWorkers uint) task.Task[*model.Row, *model.Row] {
	return &GenericFilter{
		name:                "filter_release_date_l_2010_and_include_es",
		input:               input,
		Conditions:          []Condition{NumericCondition{"release_date", LessThan, 2010}, ArrayIncludes{"production_countries", "ES"}},
		KeptStringFields:    []string{"movieID", "title"},
		KeptNumericFields:   []string{},
		KeptFloatFields:     []string{},
		KeptArrayFields:     []string{"genres", "production_countries"},
		Maps:                []Map{},
		subscribers:         subscribers,
		cantConsumersSender: cantConsumersSender,
		cantWorkers:         cantWorkers,
	}
}
