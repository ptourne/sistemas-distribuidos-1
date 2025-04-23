package filter

import (
	nlp "github.com/ptourne/sistemas-distribuidos-1/worker/nlp/go_client" // Add this import
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

func NewFilterSentimentAndRate(input string, subscribers []string, grpcAddr string) task.Task {
	mapper, err := nlp.NewSentimentAndRateMap(grpcAddr)
	if err != nil {
		log.Errorf("Error creating SentimentAndRateMap: %v", err)
		return nil
	}

	return &GenericFilter{
		name:              "map_sentiment_rate",
		input:             input,
		Conditions:        []Condition{NumericCondition{"revenue", NotEqual, 0}, NumericCondition{"budget", NotEqual, 0}},
		KeptStringFields:  []string{"movieID", "title"},
		KeptNumericFields: []string{},
		KeptFloatFields:   []string{}, // rate lo agrega el map
		KeptArrayFields:   []string{},
		Maps:              []Map{mapper},
		subscribers:       subscribers,
	}
}
