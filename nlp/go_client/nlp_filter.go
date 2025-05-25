package nlp

import (
	"context"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	pb "github.com/ptourne/sistemas-distribuidos-1/nlp/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type SentimentAndRateMap struct {
	client pb.SentimentAnalyzerClient
	conn   *grpc.ClientConn
	addr   string
}

func NewSentimentAndRateMap(addr string) (*SentimentAndRateMap, error) {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	client := pb.NewSentimentAnalyzerClient(conn)
	return &SentimentAndRateMap{client: client, conn: conn, addr: addr}, nil
}

func (m *SentimentAndRateMap) Transform(row *model.Row, output *model.Row) error {
	// Sentiment
	overview := row.Strings["overview"]
	resp, err := m.client.Analyze(context.Background(), &pb.SentimentRequest{Text: overview})
	if err != nil {

		// Intentar reconectar una vez
		if recErr := m.reconnect(); recErr != nil {
			return recErr
		}
		resp, err = m.client.Analyze(context.Background(), &pb.SentimentRequest{Text: overview})
		if err != nil {
			log.Errorf("Retry failed: %v", err)
			return err
		}
	}
	output.Strings["sentiment"] = resp.Label
	log.Infof("Sentiment analysis result: %s", resp.Label)

	// Rate
	revenue, revenueOk := row.Numerics["revenue"]
	budget, budgetOk := row.Numerics["budget"]
	if revenueOk && budgetOk && budget != 0 {
		output.Floats["rate"] = float64(revenue) / float64(budget)
	}

	log.Infof("Rate %f and sentimenr %s", output.Floats["rate"], output.Strings["sentiment"])

	return nil
}

func (m *SentimentAndRateMap) reconnect() error {
	log.Warnf("Reconnecting to gRPC server at %s", m.addr)

	if m.conn != nil {
		m.conn.Close()
	}

	conn, err := grpc.NewClient(m.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Errorf("Reconnection failed: %v", err)
		return err
	}

	m.conn = conn
	m.client = pb.NewSentimentAnalyzerClient(conn)
	log.Infof("Reconnected to gRPC server")
	return nil
}
