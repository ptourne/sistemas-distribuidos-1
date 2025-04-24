package filter

import (
	"reflect"
	"strings"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
	nlp "github.com/ptourne/sistemas-distribuidos-1/worker/nlp/go_client" // Add this import
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

type SentimentAndRateMap struct {
}

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

func (f *GenericFilter) Run() error {
	middlewareConnection, err := middleware.NewRabbitmq[common.Row]()
	if err != nil {
		unwrap(err, "Failed to create middleware")
		return err
	}
	log.Infof("Connected to middleware")
	defer middlewareConnection.Close()

	channels, err := f.Connect(middlewareConnection)
	inputChannel := channels[0]
	unwrap(err, "Failed to create channel for task")
	cases := make([]reflect.SelectCase, 0)
	cases = append(cases, reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(inputChannel),
	})

	for {
		if len(cases) == 0 {
			log.Infof("All channels closed")
			break
		}
		i, val, ok := reflect.Select(cases)
		//currentTask := w.Tasks[i]
		if !ok {
			log.Infof("Channel closed from task: %s", f.Name())
			//cases = make([]reflect.SelectCase, 0)
			f.Finish()
			break
		}

		log.Debugf("Received message from channel %d", i)
		envelope, ok := val.Interface().(middleware.Envelope[common.Row])
		if !ok {
			panic("Failed to cast to envelope")
		}
		row := envelope.Msg()
		result := f.ProcessAndSend(row)
		if result != nil {
			log.Errorf("Failed to process row: %v by task: %v", row, f.Name())
			continue
		}
		err = envelope.Ack(false)
		unwrap(err, "Failed to ack message")
		//log.Debugf("Row processed: %v name: %v", row.Strings["title"], f.Name())
	}
	return nil
}

func unwrap(err error, msg string) {
	if err != nil {
		if strings.Contains(err.Error(), "channel/connection is not open") {
			log.Warnf("%s: %s", msg, err)
		} else {
			log.Fatalf("%s: %s", msg, err)
			panic(err)
		}
	}
}
