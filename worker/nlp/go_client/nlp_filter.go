package nlp

import (
	"context"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	pb "github.com/ptourne/sistemas-distribuidos-1/worker/nlp/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

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

func (m *SentimentAndRateMap) Transform(row *model.Row, output *model.Row) error {
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
