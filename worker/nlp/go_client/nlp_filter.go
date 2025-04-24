package nlp

import (
	"context"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/worker/filter"
	pb "github.com/ptourne/sistemas-distribuidos-1/worker/nlp/proto"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NewFilterSentimentAndRate(input string, subscribers []string, grpcAddr string) task.Task {
	mapper, err := NewSentimentAndRateMap(grpcAddr)
	if err != nil {
		log.Errorf("Error creating SentimentAndRateMap: %v", err)
		return nil
	}

	return &filter.GenericFilter{
		FilterName:        "map_sentiment_rate",
		InputName:         input,
		Conditions:        []filter.Condition{filter.NumericCondition{"revenue", filter.NotEqual, 0}, filter.NumericCondition{"budget", filter.NotEqual, 0}},
		KeptStringFields:  []string{"movieID", "title"},
		KeptNumericFields: []string{},
		KeptFloatFields:   []string{}, // rate lo agrega el map
		KeptArrayFields:   []string{},
		Maps:              []filter.Map{mapper},
		Subscribers:       subscribers,
	}
}

type SentimentAndRateMap struct {
	client pb.SentimentAnalyzerClient
}

func NewSentimentAndRateMap(addr string) (*SentimentAndRateMap, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := pb.NewSentimentAnalyzerClient(conn)
	return &SentimentAndRateMap{client: client}, nil
}

func (m *SentimentAndRateMap) Transform(row *common.Row, output *common.Row) error {
	// Sentiment
	overview := row.Strings["overview"]
	resp, err := m.client.Analyze(context.Background(), &pb.SentimentRequest{Text: overview})
	if err != nil {
		log.Errorf("Error during sentiment analysis: %v", err)
		return err
	} else {
		output.Strings["sentiment"] = resp.Label
		log.Infof("Sentiment analysis result: %s", resp.Label)
	}

	// Rate
	revenue, revenueOk := row.Numerics["revenue"]
	budget, budgetOk := row.Numerics["budget"]
	if revenueOk && budgetOk && budget != 0 {
		output.Floats["rate"] = float64(revenue) / float64(budget)
	}

	return nil
}
