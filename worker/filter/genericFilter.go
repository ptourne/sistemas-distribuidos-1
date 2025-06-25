package filter

import (
	"context"
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
)

type NumericOperator int

const (
	Equal NumericOperator = iota
	NotEqual
	GreaterThan
	LessThan
	GreaterThanOrEqual
	LessThanOrEqual
)

type NumericCondition struct {
	Column   string
	Operator NumericOperator
	Value    uint64
}

func (c NumericCondition) Passes(row *model.Row) (bool, error) {
	if val, ok := row.Numerics[c.Column]; ok {
		switch c.Operator {
		case Equal:
			return val == c.Value, nil
		case NotEqual:
			return val != c.Value, nil
		case GreaterThan:
			return val > c.Value, nil
		case LessThan:
			return val < c.Value, nil
		case GreaterThanOrEqual:
			return val >= c.Value, nil
		case LessThanOrEqual:
			return val <= c.Value, nil
		}
	}
	return false, &MissingFieldError{c.Column}
}

type Condition interface {
	Passes(row *model.Row) (bool, error)
}

type Map interface {
	Transform(input *model.Row, output *model.Row) error
}

type GenericFilter struct {
	name                string
	input               string
	Conditions          []Condition
	KeptStringFields    []string
	KeptNumericFields   []string
	KeptFloatFields     []string
	KeptArrayFields     []string
	Maps                []Map
	taskReceiver        middleware.Receiver[*model.Row]
	taskSender          middleware.Sender[*model.Row]
	subscribers         []string
	cantConsumersSender uint
	cantWorkers         uint
}

func (f *GenericFilter) Name() string {
	return f.name
}

func (f *GenericFilter) Input() string {
	return f.input
}

func (f *GenericFilter) CantConsumers() uint {
	return f.cantConsumersSender
}

func (f *GenericFilter) CantWorkers() uint {
	return f.cantWorkers
}

func (f GenericFilter) ProcessAndSend(envelope middleware.Envelope[*model.Row]) error {
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
		err := f.taskSender.Prune(cid)
		if err != nil {
			return fmt.Errorf("cid %d | Prune failed in: %s with err:%s", cid, err, f.Name())
		}
		return nil
	default:
		// log.Infof("NORMAL arrived for cid: %s in %s movieID: %s", cid, f.Name(), row.Strings["movieID"])
		output, err := f.process(row)
		if err != nil {
			return fmt.Errorf("cid %d | Error processing row: %v", cid, err)
		}
		if output == nil {
			log.Debugf("Row dropped: %+v by cleaner", row)
			return nil
		}
		err = f.taskSender.Send(output, cid, id)
		if err != nil {
			return fmt.Errorf("failed to send message: %w", err)
		}
		return nil
	}
}

func (f GenericFilter) process(row *model.Row) (*model.Row, error) {
	for _, condition := range f.Conditions {
		passes, err := condition.Passes(row)
		if err != nil {
			log.Errorf("Error processing condition: %v", err)
			return nil, err
		}
		if !passes {
			f.Logf("Row %+v failed condition: %+v", row, condition)
			return nil, nil
		}
	}
	res := &model.Row{
		Strings:  make(map[string]string),
		Numerics: make(map[string]uint64),
		Floats:   make(map[string]float64),
		Arrays:   make(map[string][]string),
	}
	for _, field := range f.KeptStringFields {
		if val, ok := row.Strings[field]; ok {
			res.Strings[field] = val
		}
	}
	for _, field := range f.KeptNumericFields {
		if val, ok := row.Numerics[field]; ok {
			res.Numerics[field] = val
		}
	}
	for _, field := range f.KeptFloatFields {
		if val, ok := row.Floats[field]; ok {
			res.Floats[field] = val
		}
	}
	for _, field := range f.KeptArrayFields {
		if val, ok := row.Arrays[field]; ok {
			res.Arrays[field] = val
		}
	}
	for _, mapf := range f.Maps {
		err := mapf.Transform(row, res)
		if err != nil {
			log.Errorf("Error during map transformation: %v", err)
			return nil, err
		}
	}
	return res, nil
}

func (f GenericFilter) Logf(format string, args ...any) {
	log.Debugf(format, args...)
}

func (f GenericFilter) String() string {
	return fmt.Sprintf("GenericFilter{Conditions: %v}", f.Conditions)
}

func (f *GenericFilter) Connect(middlewareConnection middleware.Connection[*model.Row], _ middleware.Connection[*model.Row]) ([]chan middleware.Envelope[*model.Row], error) {
	var err error
	prefetch := 1000
	id_worker := os.Getenv("WORKER_ID")
	if id_worker == "" {
		return nil, fmt.Errorf("WORKER_ID environment variable is not set")
	}
	f.taskReceiver, err = middlewareConnection.ConsumeFrom(f.Input(), f.Name(), id_worker, prefetch, f.CantWorkers())
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue for task %s", f.Name())
	}
	f.taskSender, err = middlewareConnection.WriteTo(f.Name(), f.subscribers, id_worker, f.CantConsumers())
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
				if err.Error() == "read channel was closed" || err.Error() == "close channel was closed" {
					log.Infof("Channel closed: %v", f.Name())
					break
				}
				log.Errorf("Error reading from middleware: %v", err)
				continue
			}
			switch envelope.Type() {
			case middleware.EOF:
				// log.Infof("finish arrived for cid: YESS %s in %s", envelope.Cid(), f.Name())
			case middleware.Prune:
				// log.Infof("Prune arrived for cid: %s in %s", envelope.Cid(), f.Name())
			default:
				// log.Infof("NORMAL arrived for cid: %s in %s", envelope.Cid(), f.Name())
			}
			inputChannel <- envelope
		}
		close(inputChannel)
	}()
	channels := []chan middleware.Envelope[*model.Row]{inputChannel}

	return channels, nil
}

func (f *GenericFilter) Finish() error {
	if err := f.taskReceiver.Close(); err != nil {
		return fmt.Errorf("failed to close task receiver: %w", err)
	}
	if err := f.taskSender.Close(); err != nil {
		return fmt.Errorf("failed to close task sender: %w", err)
	}
	log.Infof("Closed task %s", f.Name())
	return nil
}
