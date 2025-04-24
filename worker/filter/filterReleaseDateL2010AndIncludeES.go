package filter

import "github.com/ptourne/sistemas-distribuidos-1/worker/task"

func NewFilterReleaseDateL2010AndIncludeES(input string, subscribers []string) task.Task {
	return &GenericFilter{
		FilterName:        "filter_release_date_l_2010_and_include_es",
		InputName:         input,
		Conditions:        []Condition{NumericCondition{"release_date", LessThan, 2010}, ArrayIncludes{"production_countries", "ES"}},
		KeptStringFields:  []string{"movieID", "title"},
		KeptNumericFields: []string{},
		KeptFloatFields:   []string{},
		KeptArrayFields:   []string{"genres", "production_countries"},
		Maps:              []Map{},
		Subscribers:       subscribers,
	}
}
