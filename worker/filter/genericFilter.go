package filter

import (
	"fmt"
	"reflect"
	"strings"

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
	FilterName        string
	InputName         string
	Conditions        []Condition
	KeptStringFields  []string
	KeptNumericFields []string
	KeptFloatFields   []string
	KeptArrayFields   []string
	Maps              []Map
	TaskReceiver      middleware.Receiver[common.Row]
	TaskSender        middleware.Sender[common.Row]
	Subscribers       []string
}

func (f *GenericFilter) Name() string {
	return f.FilterName
}

func (f *GenericFilter) Input() string {
	return f.InputName
}

func (f GenericFilter) ProcessAndSend(row common.Row) error {
	output := f.process(row)
	if output == nil {
		return nil
	}
	return f.TaskSender.Send(output)
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
	f.TaskReceiver, err = middlewareConnection.ConsumeFrom(f.Input(), f.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue for task %s", f.Name())
	}
	f.TaskSender, err = middlewareConnection.WriteTo(f.Name(), f.Subscribers)
	if err != nil {
		return nil, fmt.Errorf("failed to create write queue for task %s", f.Name())
	}

	//lint:ignore S1019 Ignorar reflect.Select en este archivo
	inputChannel := make(chan middleware.Envelope[common.Row], 0)
	go func() {
		for {
			envelope, ok, err := f.TaskReceiver.Next(nil)
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
	if err := f.TaskReceiver.Close(); err != nil {
		return fmt.Errorf("failed to close task receiver: %w", err)
	}
	if err := f.TaskSender.Close(); err != nil {
		return fmt.Errorf("failed to close task sender: %w", err)
	}
	log.Infof("Closed task %s", f.Name())
	return nil
}

func (f GenericFilter) Run() error {
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
		if !ok {
			log.Infof("Channel closed from task: %s", f.Name())
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
