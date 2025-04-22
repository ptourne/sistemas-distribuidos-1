package nlp

import (
	"context"

	"github.com/ptourne/sistemas-distribuidos-1/common"
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

func (m *SentimentAndRateMap) Transform(row *common.Row, output *common.Row) error {
	// Sentiment
	overview := row.Strings["overview"]
	resp, err := m.client.Analyze(context.Background(), &pb.SentimentRequest{Text: overview})
	if err != nil {
		log.Errorf("Error during sentiment analysis: %v", err)
		// ToDo: Error during sentiment analysis: rpc error: code = Unknown desc = Exception calling application: The size of tensor a (649) must match the size of tensor b (512) at non-singleton dimension 1
		return err
	} else {
		output.Strings["sentiment"] = resp.Label
		//log.Infof("Sentiment analysis result: %s for movie %s", resp.Label, row.Strings["title"])
	}

	// Rate
	revenue, revenueOk := row.Floats["revenue"]
	budget, budgetOk := row.Floats["budget"]
	if revenueOk && budgetOk && budget != 0 {
		output.Floats["rate"] = float64(revenue) / float64(budget)
	}

	return nil
}
