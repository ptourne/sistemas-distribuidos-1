package map_reducer

import (
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Debug)

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

const INITIAL_TIMEOUT_DURATION = 1000
const WATING_JITTER = 50
const GIVE_UP_READING_AT = 10

func ExponentialBackoffDuration(originalDuration uint) uint {
	return originalDuration + INITIAL_TIMEOUT_DURATION + rand.UintN(WATING_JITTER)
}

func (mr *MapReducer[I, A, R]) Run() error {
	log.Debugf("Starting map-reduce operation")
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

		log.Debugf("Waiting for batch or input")
		select {
		case acc := <-accBatch:
			log.Debugf("Received partial result")
			consumedBatch = true
			if acc.err != nil {
				return fmt.Errorf("error reading partial result: %w", acc.err)
			}
			if len(acc.res) == 0 {
				log.Debugf("Partial result is empty")
				timeoutDuration = ExponentialBackoffDuration(timeoutDuration)
				break
			}
			log.Debugf("Partial result: %v", acc.res)
			timeoutDuration = ExponentialBackoffDuration(0)
			reduced := mr.mapReduce.Reduce(acc.res)
			log.Debugf("Reduced partial result: %v", reduced)
			err := mr.partialResultSender.Send(&reduced)
			if err != nil {
				return fmt.Errorf("error sending partial result: %w", err)
			}
			log.Debugf("Sent reduced partial result")
		case input := <-inputChan:
			log.Debugf("Received input")
			consumedInput = true
			if input.err != nil {
				return fmt.Errorf("error reading input: %w", input.err)
			}

			if input.res == nil {
				mr.inputClosed = true
				log.Debugf("Input is closed")
				break // Should abandon reading from input
			}
			log.Debugf("Mapping row: %v", input.res)
			acc := mr.mapReduce.Map(*input.res)
			err := mr.partialResultSender.Send(&acc)
			if err != nil {
				return fmt.Errorf("error sending partial result: %w", err)
			}
			log.Debugf("Sent mapped partial result: %v", acc)
		}
	}

	log.Debugf("Finished processing input")
	producerCount, err := mr.partialResultReceiver.CountProducers()
	if err != nil {
		return fmt.Errorf("failed to get producer count")
	}
	shouldRetire := false
	for {
		if shouldRetire {
			log.Debugf("Retiring")
			return nil
		}
		if producerCount == 1 {
			log.Debugf("I am the last producer, computing final result")
			break
		}
		log.Debugf("Waiting for partial result")
		acc := <-accBatch
		if acc.err != nil {
			return fmt.Errorf("error reading partial result: %w", acc.err)
		}
		log.Debugf("Received partial result")
		if len(acc.res) < int(mr.batchSize) {
			timeoutDuration = ExponentialBackoffDuration(timeoutDuration)
			if len(acc.res) == 0 {
				log.Debugf("No partial result received")
				shouldRetire = true
			} else {
				log.Debugf("Partial result is smaller than batch size: %d < %d", len(acc.res), mr.batchSize)
				producerCount, err = mr.partialResultReceiver.CountProducers()
				log.Debugf("Producer count: %d", producerCount)
				if err != nil {
					return fmt.Errorf("failed to get producer count")
				}
			}
			break
		}
		timeoutDuration = ExponentialBackoffDuration(0)
		log.Debugf("Partial result received: %v", acc.res)
		reduced := mr.mapReduce.Reduce(acc.res)
		log.Debugf("Reduced partial result: %v", reduced)
		err := mr.partialResultSender.Send(&reduced)
		if err != nil {
			return fmt.Errorf("error sending partial result: %w", err)
		}

		accBatch = mr.readBatch(timeoutDuration)
	}

	timer := time.NewTimer(time.Millisecond * time.Duration(INITIAL_TIMEOUT_DURATION))
	lastBatch := make([]A, 0, mr.batchSize)
	var lastMsg *middleware.Envelope[A] = nil
	log.Debugf("Waiting for final partial result")
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
	log.Debugf("Received final partial result: %v", lastBatch)
	lastRes := mr.mapReduce.Reduce(lastBatch)
	log.Debugf("Reduced final partial result: %v", lastRes)
	output := mr.mapReduce.Output(lastRes)
	log.Debugf("Output final result: %v", output)
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
	timer := time.NewTimer(time.Second * 200)
	task := func() {
		a, ok, err := mr.input.Next(timer)
		if err != nil {
			if err.Error() == "timeout reached while waiting for message" {
				res <- asyncRes[*I]{nil, err}
				return
			}
			res <- asyncRes[*I]{nil, fmt.Errorf("error reading partial result: %w", err)}
			return
		}
		if !ok {
			res <- asyncRes[*I]{nil, nil}
			return
		}

		msg := a.Msg()
		res <- asyncRes[*I]{&msg, a.Ack(false)}
	}
	go task()
	return res
}
