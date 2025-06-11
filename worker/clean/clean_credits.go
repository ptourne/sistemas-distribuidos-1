package clean

import (
	"context"
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

type CleanCredits struct {
	input         task.Task[*model.Row, *model.Row]
	taskReceiver  middleware.Receiver[*model.Row]
	taskSender    middleware.Sender[*model.Row]
	subscribers   []string
	cantConsumers uint
	cantWorkers   uint
}

func NewCleanCredits(input task.Task[*model.Row, *model.Row], subscribers []string, cantConsumers uint, cantWorkers uint) task.Task[*model.Row, *model.Row] {
	return &CleanCredits{input, nil, nil, subscribers, cantConsumers, cantWorkers}
}

func (f CleanCredits) Input() string {
	return f.input.Name()
}

func (f CleanCredits) Name() string {
	return "clean_credits"
}

func (f CleanCredits) CantConsumers() uint {
	return f.cantConsumers
}
func (f CleanCredits) CantWorkers() uint {
	return f.cantWorkers
}

func (f CleanCredits) ProcessAndSend(envelope middleware.Envelope[*model.Row]) error {
	row := envelope.Msg()
	cid := envelope.Cid()
	id := envelope.Id()
	t := envelope.Type()
	switch t {
	case middleware.EOF:
		// log.Infof("EOF arrived for cid: %s in %s", cid, f.Name())

		err := f.taskSender.SendEOF(cid)
		if err != nil {
			return fmt.Errorf("failed to send EOF: %w", err)
		}
		return nil
	case middleware.Prune:
		// log.Infof("Prune arrived for cid: %s in %s movieID: %s", cid, f.Name(), row.Strings["movieID"])
		err := f.taskSender.Prune(cid)
		if err != nil {
			log.Errorf("cid %d | Prune failed in: %s with err:%s", cid, err, f.Name())
			envelope.Nack(true)
		}
		return nil
	default:
		output := f.process(row)
		if output == nil {
			log.Debugf("Row dropped: %+v by cleaner", row)
			return nil
		}
		return f.taskSender.Send(output, cid, id)
	}
}

func (f CleanCredits) process(row *model.Row) *model.Row {
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

	return &model.Row{
		Strings: map[string]string{
			"ID": row.Strings["ID"],
		},
		Arrays: map[string][]string{
			"cast": cast,
		},
	}
}

func (f *CleanCredits) Connect(inputMiddleware middleware.Connection[*model.Row], outputMiddleware middleware.Connection[*model.Row]) ([]chan middleware.Envelope[*model.Row], error) {
	var err error
	// prefetch, err := strconv.Atoi(os.Getenv("PREFETCH"))
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to parse PREFETCH: %w", err)
	// }
	prefetch := 2000
	idWorker := os.Getenv("WORKER_ID")
	if idWorker == "" {
		return nil, fmt.Errorf("WORKER_ID environment variable is not set")
	}
	f.taskReceiver, err = inputMiddleware.ConsumeFrom(f.Input(), f.Name(), idWorker, prefetch, f.CantWorkers())
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue for task %s", f.Name())
	}
	f.taskSender, err = outputMiddleware.WriteTo(f.Name(), f.subscribers, idWorker, f.CantConsumers())
	if err != nil {
		return nil, fmt.Errorf("failed to create write queue for task %s", f.Name())
	}

	//lint:ignore S1019 Ignorar reflect.Select en este archivo
	inputChannel := make(chan middleware.Envelope[*model.Row], 0)
	go func() {
		for {
			ctx := context.Background()
			envelope, err := f.taskReceiver.Next(ctx)
			if err != nil {
				if err.Error() == "read channel was closed" {
					log.Infof("Channel closed: %v", f.Name())
					break
				}
				log.Errorf("Error reading from middleware: %v", err)
				continue
			}
			switch envelope.Type() {
			case middleware.EOF:
				// log.Infof("finish arrived for cid: %s from clean_credits", envelope.Cid())
			case middleware.Prune:
				// envelope.Ack(true)
				// continue
			}
			inputChannel <- envelope
		}
		close(inputChannel)
	}()

	channels := []chan middleware.Envelope[*model.Row]{inputChannel}
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
