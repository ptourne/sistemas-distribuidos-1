package map_reducer

import (
	"fmt"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/middleware"
)

// MapReducer is a struct that represents a map-reduce operation.
//
// I is the type of the input data.
// A is the type of the accumulator.
// R is the type of the final result.
type MapReducer[I, A, R any] struct {
	batchSize             uint
	input                 middleware.Receiver[I]
	partialResultSender   middleware.Sender[A]
	partialResultReceiver middleware.Receiver[A]
	mapReduce             MapReduce[I, A, R]
	output                middleware.Sender[R]
	inputClosed           bool
}

// batchSize is the number of top groups you reduce at once
func NewMapReducer[I, A, R any](name string, input string, batchSize uint, mapReducer MapReduce[I, A, R]) (*MapReducer[I, A, R], error) {
	if batchSize < 2 {
		return nil, fmt.Errorf("batchSize must be at least two")
	}
	connIn, err := middleware.NewRabbitmq[I]()
	if err != nil {
		return nil, err
	}
	inputCh, err := connIn.ConsumeFrom(input, name)
	if err != nil {
		return nil, err
	}
	connOut, err := middleware.NewRabbitmq[R]()
	if err != nil {
		return nil, err
	}
	output, err := connOut.WriteTo(name)
	if err != nil {
		return nil, err
	}

	accName := accName(name)
	connAcc, err := middleware.NewRabbitmq[A]()
	if err != nil {
		return nil, err
	}
	accIn, err := connAcc.ConsumeFrom(accName, name)
	if err != nil {
		return nil, err
	}
	accOut, err := connAcc.WriteTo(accName)
	if err != nil {
		return nil, err
	}

	return &MapReducer[I, A, R]{
		batchSize:             batchSize,
		input:                 inputCh,
		partialResultSender:   accOut,
		partialResultReceiver: accIn,
		mapReduce:             mapReducer,
		output:                output,
	}, nil
}

func accName(name string) string {
	return name + "_acc"
}

type MapReduce[T, A, R any] interface {
	Map(T) A
	Reduce([]A) A
	Output(A) R
}

func (mr *MapReducer[I, A, R]) Run() error {
	for {
		accBatch := mr.readBatch()
		var inputChan <-chan asyncRes[*I]
		if mr.inputClosed {
			inputChan = nil // Ni idea qué pasa esperando a leer de un canal nulo
		} else {
			inputChan = mr.readInput()
		}
		select {
		case acc := <-accBatch:
			if acc.err != nil {
				return fmt.Errorf("error reading partial result: %w", acc.err)
			}

			if len(acc.res) == 0 {
				break
			}
			reduced := mr.mapReduce.Reduce(acc.res)
			err := mr.partialResultSender.Send(&reduced)
			if err != nil {
				return fmt.Errorf("error sending partial result: %w", err)
			}
		case input := <-inputChan:
			if input.err != nil {
				return fmt.Errorf("error reading input: %w", input.err)
			}

			if input.res == nil {
				break // Should abandon reading from input
			}
			acc := mr.mapReduce.Map(*input.res)
			err := mr.partialResultSender.Send(&acc)
			if err != nil {
				return fmt.Errorf("error sending partial result: %w", err)
			}
		}
	}
	return nil
}

type asyncRes[T any] struct {
	res T
	err error
}

func (mr *MapReducer[I, A, R]) readBatch() <-chan asyncRes[[]A] {
	res := make(chan asyncRes[[]A])
	timer := time.NewTimer(time.Millisecond * 200)
	task := func() {
		batch := make([]A, 0, mr.batchSize)
		var lastAck func(bool) error
		for range mr.batchSize {
			a, ok, err := mr.partialResultReceiver.Next(timer)
			if err != nil {
				if err.Error() == "timeout reached while waiting for message" {
					break
				}
				res <- asyncRes[[]A]{batch, fmt.Errorf("error reading partial result: %w", err)}
			}
			if !ok {
				mr.inputClosed = true
				break
			}
			batch = append(batch, a.Msg())
			lastAck = a.Ack
		}
		if len(batch) < 2 {
			res <- asyncRes[[]A]{batch[:0], nil}
			return // Devolvemos el elemento que tomamos para que lo utilice otro worker
		}
		err := lastAck(true)
		res <- asyncRes[[]A]{batch, err}
	}
	go task()
	return res
}

func (mr *MapReducer[I, A, R]) readInput() <-chan asyncRes[*I] {
	res := make(chan asyncRes[*I])
	timer := time.NewTimer(time.Millisecond * 200)
	task := func() {
		a, ok, err := mr.input.Next(timer)
		if err != nil {
			if err.Error() == "timeout reached while waiting for message" {
				res <- asyncRes[*I]{nil, err}
				return
			}
			res <- asyncRes[*I]{nil, fmt.Errorf("error reading partial result: %w", err)}
		}
		if !ok {
			mr.inputClosed = true
			res <- asyncRes[*I]{nil, nil}
		}

		msg := a.Msg()
		res <- asyncRes[*I]{&msg, a.Ack(false)}
	}
	go task()
	return res
}
