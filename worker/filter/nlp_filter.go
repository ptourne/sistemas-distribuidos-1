package filter

import (
	nlp "github.com/ptourne/sistemas-distribuidos-1/worker/nlp/go_client" // Add this import
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

func NewFilterSentimentAndRate(input task.Task, subscribers []string, grpcAddr string) task.Task {
	mapper, err := nlp.NewSentimentAndRateMap(grpcAddr)
	if err != nil {
		panic(err)
	}

	return &GenericFilter{
		name:              "map_sentiment_rate",
		input:             input,
		Conditions:        []Condition{FloatCondition{"revenue", NotEqual, 0.0}, FloatCondition{"budget", NotEqual, 0.0}},
		KeptStringFields:  []string{"movieID", "title"},
		KeptNumericFields: []string{},
		KeptFloatFields:   []string{}, // rate lo agrega el map
		KeptArrayFields:   []string{},
		Maps:              []Map{mapper},
		subscribers:       subscribers,
	}
}
