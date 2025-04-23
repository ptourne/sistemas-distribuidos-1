package filter

import (
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
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
	Value    uint
}

func (c NumericCondition) Passes(row common.Row) (bool, error) {
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
	Passes(row common.Row) (bool, error)
}

type Map interface {
	Transform(input *common.Row, output *common.Row) error
}

type GenericFilter struct {
	name              string
	input             string
	Conditions        []Condition
	KeptStringFields  []string
	KeptNumericFields []string
	KeptFloatFields   []string
	KeptArrayFields   []string
	Maps              []Map
	taskReceiver      middleware.Receiver[common.Row]
	taskSender        middleware.Sender[common.Row]
	subscribers       []string
}

func (f *GenericFilter) Name() string {
	return f.name
}

func (f *GenericFilter) Input() string {
	return f.input
}

func (f GenericFilter) ProcessAndSend(row common.Row) error {
	output := f.process(row)
	if output == nil {
		return nil
	}
	return f.taskSender.Send(output)
}

func (f GenericFilter) process(row common.Row) *common.Row {
	for _, condition := range f.Conditions {
		passes, err := condition.Passes(row)
		if err != nil {
			log.Errorf("Error processing condition: %v", err)
			return nil
		}
		if !passes {
			f.Logf("Row %+v failed condition: %+v", row, condition)
			return nil
		}
	}
	res := &common.Row{
		Strings:  make(map[string]string),
		Numerics: make(map[string]uint),
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
		err := mapf.Transform(&row, res)
		if err != nil {
			log.Errorf("Error during map transformation: %v", err)
			return nil
		}
	}
	return res
}

func (f GenericFilter) Logf(format string, args ...any) {
	log.Debugf(format, args...)
}

func (f GenericFilter) String() string {
	return fmt.Sprintf("GenericFilter{Conditions: %v}", f.Conditions)
}

func (f *GenericFilter) Connect(middlewareConnection middleware.MiddlewareCola[common.Row]) ([]chan middleware.Envelope[common.Row], error) {
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
				if err.Error() == "read channel was closed" || err.Error() == "close channel was closed" {
					log.Infof("Channel closed: %v", f.Name())
					break
				}
				log.Errorf("Error reading from middleware: %v", err)
				continue
			}
			if !ok {
				log.Infof("Channel closed desde generic2: %v", f.Name())
				break
			}
			inputChannel <- envelope
		}
		close(inputChannel)
	}()
	channels := []chan middleware.Envelope[common.Row]{inputChannel}

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
