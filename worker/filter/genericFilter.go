package filter

import (
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
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
	input             task.Task
	Conditions        []Condition
	KeptStringFields  []string
	KeptNumericFields []string
	KeptFloatFields   []string
	KeptArrayFields   []string
	Maps              []Map
}

func (f *GenericFilter) Name() string {
	return f.name
}

func (f *GenericFilter) Input() string {
	return f.input.Name()
}

func (f GenericFilter) Process(row common.Row) *common.Row {
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
		mapf.Transform(&row, res)
	}
	return res
}

func (f GenericFilter) Logf(format string, args ...any) {
	log.Infof(format, args...)
}

func (f GenericFilter) String() string {
	return fmt.Sprintf("GenericFilter{Conditions: %v}", f.Conditions)
}
