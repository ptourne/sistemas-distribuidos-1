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
func NewMapReducer[I, A, R any](name string, input string, batchSize uint, mapReducer MapReduce[I, A, R], subscribers []string) (*MapReducer[I, A, R], error) {
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
	output, err := connOut.WriteTo(name, subscribers)
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
	err = accIn.Qos(1, 0)
	if err != nil {
		return nil, err
	}
	accOut, err := connAcc.WriteTo(accName, []string{})
	if err != nil {
		return nil, err
	}
	log.Debugf("Creating map-reduce operation with name: %s, input: %s, batchSize: %d\ninput: %+v\noutput: %v\naccIn: %+v\naccOut: %+v",
		name,
		input,
		batchSize,
		inputCh,
		output,
		accIn,
		accOut,
	)

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
	Map(T) []A
	Reduce([]A) A
	Output(A) []R
}

const INITIAL_TIMEOUT_DURATION = 3000
const WATING_JITTER = 100

func ExponentialBackoffDuration(step uint) (nextStep uint, duration uint) {
	if step == 0 {
		return step + 1, 0
	}
	return step + 1, rand.UintN((step + 1) * WATING_JITTER)
}

func (mr *MapReducer[I, A, R]) Run() error {
	defer mr.partialResultSender.Close()
	defer mr.output.Close()
	inputChan := mr.readInput()
	accBatch := mr.reduceBattchess()
	var err error
	select {
	case err = <-inputChan:
	case err = <-accBatch:
	}
	if err != nil {
		return err
	}
	err = <-accBatch
	return err
}

func (mr *MapReducer[I, A, R]) reduceBattchess() <-chan error {
	res := make(chan error)
	task := func() {
		log.Debugf("Starting map-reduce operation")
		timeoutStep, backoff := ExponentialBackoffDuration(0)
		log.Debugf("reduceBattchess(%d)", backoff)
		var err error
		var lastMsg *middleware.Envelope[A]
		nack := func() error {
			if lastMsg != nil {
				nerr := (*lastMsg).Nack(true)
				lastMsg = nil
				return nerr
			}
			return nil
		}
		ack := func() error {
			if lastMsg != nil {
				aerr := (*lastMsg).Ack(true)
				lastMsg = nil
				return aerr
			}
			return nil
		}
		defer func() {
			nack()
			res <- err
		}()
		var batch []A
		for !mr.inputClosed {
			nack()
			<-time.NewTimer(time.Millisecond * time.Duration(backoff)).C
			batch, lastMsg, err = mr.readBatch()
			if err != nil {
				if err.Error() == "timeout reached while waiting for message" {
					err = nil
					timeoutStep, backoff = ExponentialBackoffDuration(timeoutStep)
					continue
				} else {
					err = fmt.Errorf("error reading partial result: %w", err)
					return
				}
			}
			log.Debugf("Batch read completed: len = %v", len(batch))
			err = ack()
			timeoutStep, backoff = ExponentialBackoffDuration(0)
			log.Infof("Merging %+v", batch)
			reduced := mr.mapReduce.Reduce(batch)
			log.Debugf("Reduced partial result: %v", reduced)
			err = mr.partialResultSender.Send(&reduced)
			if err != nil {
				err = fmt.Errorf("error sending partial result: %w", err)
				return
			}
			log.Debugf("Sent reduced partial result")
		}
		var producerCount int
		countProducers := func() (int, error) {
			if WORKER_ID != "1" {
				return 2, nil
			}
			log.Infof("Calling count producers from worker '%s'", WORKER_ID)
			return mr.partialResultReceiver.CountProducers()
		}
		producerCount, err = countProducers()
		if err != nil {
			err = fmt.Errorf("failed to get producer count")
			return
		}
		for {
			nack()
			<-time.NewTimer(time.Millisecond * time.Duration(backoff)).C
			batch, lastMsg, err = mr.readBatch()
			if err != nil {
				if err.Error() == "timeout reached while waiting for message" {
					err = nil
				} else {
					err = fmt.Errorf("error reading partial result: %w", err)
					return
				}
			}
			if len(batch) == 0 {
				log.Debugf("WorkerID: %s", WORKER_ID)
				if WORKER_ID != "1" {
					log.Debugf("Retiring")
					return
				}
				log.Debugf("Must no retire")
				continue
			}
			if len(batch) < int(mr.batchSize) {
				producerCount, err = countProducers()
				log.Infof("cant producers %v", producerCount)
				if err != nil {
					err = fmt.Errorf("failed to get producer count")
					return
				}
				if producerCount > 1 {
					timeoutStep, backoff = ExponentialBackoffDuration(timeoutStep)
					continue
				}
			}

			if producerCount == 1 && len(batch) == 1 {
				log.Debugf("Last batch processed")
				log.Infof("Merging %+v", batch)
				reduced := mr.mapReduce.Reduce(batch)
				log.Debugf("Reduced partial result: %v", reduced)
				outputs := mr.mapReduce.Output(reduced)
				for _, output := range outputs {
					err = mr.output.Send(&output)
					if err != nil {
						err = fmt.Errorf("error sending output: %w", err)
						return
					}
				}
				log.Debugf("Sent output")
				return
			}
			log.Debugf("Batch read completed: len = %v", len(batch))
			err = ack()
			timeoutStep, backoff = ExponentialBackoffDuration(0)
			log.Infof("Merging %+v", batch)
			reduced := mr.mapReduce.Reduce(batch)
			log.Debugf("Reduced partial result: %v", reduced)
			err = mr.partialResultSender.Send(&reduced)
			if err != nil {
				err = fmt.Errorf("error sending partial result: %w", err)
				return
			}
			log.Debugf("Sent reduced partial result")
		}
	}
	go task()
	return res
}

func (mr *MapReducer[I, A, R]) readBatch() (batch []A, lastMsg *middleware.Envelope[A], err error) {
	timer := time.NewTimer(time.Millisecond * time.Duration(INITIAL_TIMEOUT_DURATION))
	batch = make([]A, 0, mr.batchSize)
	log.Debugf("Initial batch: %v", batch)
	for range mr.batchSize {
		log.Debugf("Batch state: %v", batch)
		a, ok, err := mr.partialResultReceiver.Next(timer)
		// log.Debugf("Received message envelope: %v", a)
		log.Debugf("Received message len ok: %v, err: %s", ok, err)
		if err != nil {
			if err.Error() == "timeout reached while waiting for message" {
				return batch, lastMsg, err
			}
			return nil, nil, err
		}
		if !ok {
			log.Debugf("No more messages available")
			break
		}
		batch = append(batch, a.Msg())
		lastMsg = &a
	}
	return batch, lastMsg, nil
}

func (mr *MapReducer[I, A, R]) readInput() <-chan error {
	res := make(chan error)
	task := func() {
		var err error
		defer func() {
			mr.inputClosed = true
			log.Debugf("Input closed")
			res <- err
		}()
		for {
			var envelope middleware.Envelope[I]
			var ok bool
			envelope, ok, err = mr.input.Next(nil)
			if err != nil {
				return
			}
			if !ok {
				return
			}

			msg := envelope.Msg()
			log.Debugf("Received input")
			log.Debugf("Mapping row: %v", msg)
			acc := mr.mapReduce.Map(msg)
			for _, a := range acc {
				err = mr.partialResultSender.Send(&a)
				if err != nil {
					err = fmt.Errorf("error sending partial result: %w", err)
					return
				}
			}
			log.Debugf("Sent mapped partial result: %v", acc)
			envelope.Ack(false)
		}
	}
	go task()
	return res
}
