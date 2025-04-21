package clean

import (
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

type CleanCredits struct {
	input        task.Task
	taskReceiver middleware.Receiver[common.Row]
	taskSender   middleware.Sender[common.Row]
	subscribers  []string
}

func NewCleanCredits(input task.Task, subscribers []string) task.Task {
	return &CleanCredits{input, nil, nil, subscribers}
}

func (f CleanCredits) Input() string {
	return f.input.Name()
}

func (f CleanCredits) Name() string {
	return "clean_credits"
}

func (f CleanCredits) ProcessAndSend(row common.Row) error {
	output := f.process(row)
	if output == nil {
		log.Infof("Row dropped: %+v by cleaner", row)
		return nil
	}
	return f.taskSender.Send(output)
}

func (f CleanCredits) process(row common.Row) *common.Row {
	requiredFields := []string{
		row.Strings["ID"],
		row.Strings["cast"],
	}

	log.Debugf("Clean: ID: %s, cast: %s",
		row.Strings["ID"],
		row.Strings["cast"],
	)

	for _, field := range requiredFields {
		if utils.MustDropRow(field) {
			log.Debugf("warning: dropping row due to empty field: %s", field)
			return nil
		}
	}

	cast, err := utils.DictionaryToListName(row.Strings["cast"])
	if err != nil {
		log.Warnf("err: %v, could not parse cast for movie %s", err, row.Strings["cast"])
		return nil
	}

	log.Debugf("Clean ALL: ID: %s, cast: %v", row.Strings["ID"], cast)

	return &common.Row{
		Strings: map[string]string{
			"ID": row.Strings["ID"],
		},
		Arrays: map[string][]string{
			"cast": cast,
		},
	}
}

func (f *CleanCredits) Connect(middlewareConnection middleware.MiddlewareCola[common.Row]) ([]chan middleware.Envelope[common.Row], error) {
	var err error
	f.taskReceiver, err = middlewareConnection.ConsumeFrom(f.Input(), f.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue for task %s", f.Name())
	}
	f.taskSender, err = middlewareConnection.WriteTo(f.Name(), f.subscribers)
	if err != nil {
		return nil, fmt.Errorf("failed to create write queue for task %s", f.Name())
	}

	//lint:ignore S1019 Ignorar reflect.Select en este archivo
	inputChannel := make(chan middleware.Envelope[common.Row], 0)
	go func() {
		for {
			envelope, ok, err := f.taskReceiver.Next(nil)
			if err != nil {
				if err.Error() == "read channel was closed" {
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

func (f *CleanCredits) Finish() error {
	if err := f.taskReceiver.Close(); err != nil {
		return fmt.Errorf("failed to close task receiver: %w", err)
	}
	if err := f.taskSender.Close(); err != nil {
		return fmt.Errorf("failed to close task sender: %w", err)
	}
	log.Infof("Closed task %s", f.Name())
	return nil
}
