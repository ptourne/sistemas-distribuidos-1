package nlp

import (
	"context"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	pb "github.com/ptourne/sistemas-distribuidos-1/nlp/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type SentimentAndRateMap struct {
	client       pb.SentimentAnalyzerClient
	conn         *grpc.ClientConn
	addrs        []string
	currentIndex int
}

func NewSentimentAndRateMap(addrs []string) (*SentimentAndRateMap, error) {
	var conn *grpc.ClientConn
	var err error
	log.Infof("Connecting to gRPC %v", addrs)
	idx := 0
	for i, addr := range addrs {
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			log.Infof("Connected to gRPC server at %s", addr)
			idx = i
			break
		}
	}
	if err != nil {
		return nil, err
	}
	client := pb.NewSentimentAnalyzerClient(conn)
	return &SentimentAndRateMap{client: client, conn: conn, addrs: addrs, currentIndex: idx}, nil
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
	if m.conn != nil {
		m.conn.Close()
	}

	var conn *grpc.ClientConn
	var err error
	total := len(m.addrs)
	start := m.currentIndex + 1

	for i := range total {
		index := (start + i) % total
		addr := m.addrs[index]
		conn, err = grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err == nil {
			m.conn = conn
			m.client = pb.NewSentimentAnalyzerClient(conn)
			m.currentIndex = index
			log.Infof("Reconnected to gRPC server at %s", addr)
			break
		}
	}
	if err != nil {
		return err
	}
	return nil
}
