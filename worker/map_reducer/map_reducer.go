package map_reducer

import (
	"fmt"
	"math/rand/v2"
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

const INITIAL_TIMEOUT_DURATION = 100
const WATING_JITTER = 50
const GIVE_UP_READING_AT = 10

func ExponentialBackoffDuration(originalDuration uint) uint {
	return originalDuration + INITIAL_TIMEOUT_DURATION + rand.UintN(WATING_JITTER)
}

func (mr *MapReducer[I, A, R]) Run() error {
	timeoutDuration := ExponentialBackoffDuration(0)

	accBatch := mr.readBatch(timeoutDuration)
	inputChan := mr.readInput()
	consumedBatch := false
	consumedInput := false
	for {
		if consumedBatch {
			accBatch = mr.readBatch(timeoutDuration)
			consumedBatch = false
		}
		if mr.inputClosed {
			break
		}
		if consumedInput {
			inputChan = mr.readInput()
			consumedInput = false
		}

		select {
		case acc := <-accBatch:
			consumedBatch = true
			if acc.err != nil {
				return fmt.Errorf("error reading partial result: %w", acc.err)
			}
			if len(acc.res) == 0 {
				timeoutDuration = ExponentialBackoffDuration(timeoutDuration)
				break
			}
			timeoutDuration = ExponentialBackoffDuration(0)
			reduced := mr.mapReduce.Reduce(acc.res)
			err := mr.partialResultSender.Send(&reduced)
			if err != nil {
				return fmt.Errorf("error sending partial result: %w", err)
			}
		case input := <-inputChan:
			consumedInput = true
			if input.err != nil {
				return fmt.Errorf("error reading input: %w", input.err)
			}

			if input.res == nil {
				mr.inputClosed = true
				break // Should abandon reading from input
			}
			acc := mr.mapReduce.Map(*input.res)
			err := mr.partialResultSender.Send(&acc)
			if err != nil {
				return fmt.Errorf("error sending partial result: %w", err)
			}
		}
	}

	producerCount, err := mr.partialResultReceiver.CountProducers()
	if err != nil {
		return fmt.Errorf("failed to get producer count")
	}
	shouldRetire := false
	for {
		if shouldRetire {
			return nil
		}
		if producerCount == 1 {
			break
		}
		acc := <-accBatch
		if acc.err != nil {
			return fmt.Errorf("error reading partial result: %w", acc.err)
		}
		if len(acc.res) < int(mr.batchSize) {
			timeoutDuration = ExponentialBackoffDuration(timeoutDuration)
			if len(acc.res) == 0 {
				shouldRetire = true
			} else {
				producerCount, err = mr.partialResultReceiver.CountProducers()
				if err != nil {
					return fmt.Errorf("failed to get producer count")
				}
			}
			break
		}
		timeoutDuration = ExponentialBackoffDuration(0)
		reduced := mr.mapReduce.Reduce(acc.res)
		err := mr.partialResultSender.Send(&reduced)
		if err != nil {
			return fmt.Errorf("error sending partial result: %w", err)
		}

		accBatch = mr.readBatch(timeoutDuration)
	}

	timer := time.NewTimer(time.Millisecond * time.Duration(INITIAL_TIMEOUT_DURATION))
	lastBatch := make([]A, 0, mr.batchSize)
	var lastMsg *middleware.Envelope[A] = nil
	for range mr.batchSize {
		a, ok, err := mr.partialResultReceiver.Next(timer)
		if err != nil {
			if err.Error() == "timeout reached while waiting for message" {
				break
			}
			return fmt.Errorf("error reading partial result: %w", err)
		}
		if !ok {
			break
		}
		lastBatch = append(lastBatch, a.Msg())
		lastMsg = &a
	}
	lastRes := mr.mapReduce.Reduce(lastBatch)
	output := mr.mapReduce.Output(lastRes)
	mr.output.Send(&output)
	cerr := mr.output.Close()
	err = (*lastMsg).Ack(true)
	if cerr != nil {
		return fmt.Errorf("error closing output: %w", cerr)
	}
	if err != nil {
		return fmt.Errorf("error acknowledging message: %w", err)
	}
	return nil
}

type asyncRes[T any] struct {
	res T
	err error
}

func (mr *MapReducer[I, A, R]) readBatch(milliseconds uint) <-chan asyncRes[[]A] {
	res := make(chan asyncRes[[]A])
	timer := time.NewTimer(time.Millisecond * time.Duration(milliseconds))
	task := func() {
		batch := make([]A, 0, mr.batchSize)
		var lastMsg *middleware.Envelope[A] = nil
		for range mr.batchSize {
			a, ok, err := mr.partialResultReceiver.Next(timer)
			if err != nil {
				if err.Error() == "timeout reached while waiting for message" {
					break
				}
				res <- asyncRes[[]A]{batch, fmt.Errorf("error reading partial result: %w", err)}
			}
			if !ok {
				break
			}
			batch = append(batch, a.Msg())
			lastMsg = &a
		}
		if len(batch) < 2 {
			res <- asyncRes[[]A]{batch, nil}
			if lastMsg != nil {
				(*lastMsg).Nack(true)
			}
			return // Devolvemos el elemento que tomamos para que lo utilice otro worker
		}
		err := (*lastMsg).Ack(true)
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
			res <- asyncRes[*I]{nil, nil}
		}

		msg := a.Msg()
		res <- asyncRes[*I]{&msg, a.Ack(false)}
	}
	go task()
	return res
}
