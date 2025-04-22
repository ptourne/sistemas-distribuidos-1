package clean

import (
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

type CleanRatings struct {
	input        task.Task
	taskReceiver middleware.Receiver[common.Row]
	taskSender   middleware.Sender[common.Row]
	subscribers  []string
}

func NewCleanRatings(input task.Task, subscribers []string) task.Task {
	return &CleanRatings{input, nil, nil, subscribers}
}

func (f CleanRatings) Input() string {
	return f.input.Name()
}

func (f CleanRatings) Name() string {
	return "clean_ratings"
}

func (f CleanRatings) ProcessAndSend(row common.Row) error {
	output := f.process(row)
	if output == nil {
		return nil
	}
	return f.taskSender.Send(output)
}

func (f CleanRatings) process(row common.Row) *common.Row {
	requiredFields := []string{
		row.Strings["movieID"],
		row.Strings["rating"],
	}

	log.Debugf("Clean: movieID: %s, rating: %s",
		row.Strings["movieID"],
		row.Strings["rating"],
	)

	for _, field := range requiredFields {
		if utils.MustDropRow(field) {
			log.Debugf("warning: dropping row due to empty field: %s", field)
			return nil
		}
	}

	rating, ok := utils.ParseFloat(row.Strings["rating"])
	if !ok {
		log.Warnf("could not parse rating: %s", row.Strings["rating"])
		return nil
	}

	log.Debugf("Clean ALL: movieID: %s, rating: %v", row.Strings["movieID"], rating)

	return &common.Row{
		Strings: map[string]string{
			"movieID": row.Strings["movieID"],
		},
		Floats: map[string]float64{
			"rating": rating,
		},
	}
}

func (f *CleanRatings) Connect(middlewareConnection middleware.MiddlewareCola[common.Row]) ([]chan middleware.Envelope[common.Row], error) {
	var err error
	f.taskReceiver, err = middlewareConnection.ConsumeFrom(f.Input(), f.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue for task %s", f.Name())
	}
	f.taskSender, err = middlewareConnection.WriteTo(f.Name(), f.subscribers)
	//f.taskReceiver.LimitUnacked(10000)

	if err != nil {
		return nil, fmt.Errorf("failed to create write queue for task %s", f.Name())
	}

	//lint:ignore S1019 Ignorar reflect.Select en este archivo
	inputChannel := make(chan middleware.Envelope[common.Row], 0)
	go func() {
		for {
			envelope, ok, err := f.taskReceiver.Next(nil)
			if err != nil {
				if err.Error() == "read channel was closed" || err.Error() == "close channel was closed" {
					log.Infof("Channel closed: %v", f.Name())
					break
				}
				log.Errorf("Error reading from middleware: %v", err)
				continue
			}
			if !ok {
				log.Infof("Channel closed: %v", f.Name())
				break
			}
			inputChannel <- envelope
		}
		close(inputChannel)
	}()
	channels := []chan middleware.Envelope[common.Row]{inputChannel}

	return channels, nil
}

func (f *CleanRatings) Finish() error {
	if err := f.taskReceiver.Close(); err != nil {
		return fmt.Errorf("failed to close task receiver: %w", err)
	}
	if err := f.taskSender.Close(); err != nil {
		return fmt.Errorf("failed to close task sender: %w", err)
	}
	log.Infof("Closed task %s", f.Name())
	return nil
}
