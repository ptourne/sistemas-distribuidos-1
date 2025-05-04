package clean

import (
	"context"
	"fmt"
	"os"
	"strconv"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

type CleanCredits struct {
	input        task.Task[*model.Row, *model.Row]
	taskReceiver middleware.Receiver[*model.Row]
	taskSender   middleware.Sender[*model.Row]
	subscribers  []string
}

func NewCleanCredits(input task.Task[*model.Row, *model.Row], subscribers []string) task.Task[*model.Row, *model.Row] {
	return &CleanCredits{input, nil, nil, subscribers}
}

func (f CleanCredits) Input() string {
	return f.input.Name()
}

func (f CleanCredits) Name() string {
	return "clean_credits"
}

func (f CleanCredits) ProcessAndSend(envelope middleware.Envelope[*model.Row]) error {
	row := envelope.Msg()
	cid := envelope.Cid()
	t := envelope.Type()
	if t == middleware.EOF {
		return f.taskSender.SendEOF(cid)
	} else {
		output := f.process(row)
		if output == nil {
			log.Infof("Row dropped: %+v by cleaner", row)
			return nil
		}
		return f.taskSender.Send(output, cid)
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

func (f *CleanCredits) Connect(middlewareConnection middleware.Connection[*model.Row], _ middleware.Connection[*model.Row]) ([]chan middleware.Envelope[*model.Row], error) {
	var err error
	prefetch, err := strconv.Atoi(os.Getenv("PREFETCH"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse PREFETCH: %w", err)
	}
	n_workers, err := strconv.Atoi(os.Getenv("N_WORKERS"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse N_WORKERS: %w", err)
	}
	f.taskReceiver, err = middlewareConnection.ConsumeFrom(f.Input(), f.Name(), prefetch, n_workers)
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue for task %s", f.Name())
	}
	f.taskSender, err = middlewareConnection.WriteTo(f.Name(), f.subscribers)
	if err != nil {
		return nil, fmt.Errorf("failed to create write queue for task %s", f.Name())
	}

	//lint:ignore S1019 Ignorar reflect.Select en este archivo
	inputChannel := make(chan middleware.Envelope[*model.Row], 0)
	go func() {
		for {
			ctx := context.Background()
			envelope, ok, err := f.taskReceiver.Next(ctx)
			if err != nil {
				if err.Error() == "read channel was closed" {
					log.Infof("Channel closed: %v", f.Name())
					break
				}
				log.Errorf("Error reading from middleware: %v", err)
				continue
			}
			if !ok {
				// log.Infof("Channel closed: %v", f.Name())
				// break
				log.Infof("finish arrived for cid: YESS %s", envelope.Cid())
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
