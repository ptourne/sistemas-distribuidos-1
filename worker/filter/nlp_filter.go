package filter

import (
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	nlp "github.com/ptourne/sistemas-distribuidos-1/nlp/go_client"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

func NewFilterSentimentAndRate(input string, subscribers []string, grpcAddrs []string, cantConsumersSender uint, cantWorkers uint) task.Task[*model.Row, *model.Row] {
	mapper, err := nlp.NewSentimentAndRateMap(grpcAddrs)
	if err != nil {
		log.Errorf("Error creating SentimentAndRateMap: %v", err)
		return nil
	}

	return &GenericFilter{
		name:                "map_sentiment_rate",
		input:               input,
		Conditions:          []Condition{NumericCondition{"revenue", NotEqual, 0}, NumericCondition{"budget", NotEqual, 0}},
		KeptStringFields:    []string{"movieID", "title"},
		KeptNumericFields:   []string{},
		KeptFloatFields:     []string{}, // rate lo agrega el map
		KeptArrayFields:     []string{},
		Maps:                []Map{mapper},
		subscribers:         subscribers,
		cantConsumersSender: cantConsumersSender,
		cantWorkers:         cantWorkers,
	}
}
