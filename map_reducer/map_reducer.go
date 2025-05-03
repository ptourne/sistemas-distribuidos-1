package map_reducer

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Debug)
var WORKER_COUNT_STR = os.Getenv("WORKER_COUNT")

// MapReducer is a struct that represents a map-reduce operation.
//
// I is the type of the input data.
// A is the type of the accumulator.
// R is the type of the final result.
type MapReducer[I codec.Serializable[I], A codec.Serializable[A], R codec.Serializable[R]] struct {
	MapReduce             MapReduce[I, A, R]
	BatchSize             uint
	Input                 middleware.Receiver[I]
	PartialResultSender   middleware.Sender[A]
	PartialResultReceiver middleware.Receiver[A]
	FinalReduceSender     middleware.Sender[A]
	FinalReduceReceiver   middleware.Receiver[A]
	Output                middleware.Sender[R]
	RoutingKeys           []string
	ClientBatches         map[string]ClientBatch[A]
	timeoutStep           uint
	backoff               uint
}

type ClientBatch[A codec.Serializable[A]] struct {
	batch    []A
	closed   bool
}

// batchSize is the number of top groups you reduce at once
func NewMapReducer[I codec.Serializable[I], A codec.Serializable[A], R codec.Serializable[R]](name string, input string, batchSize uint, mapReducer MapReduce[I, A, R], subscribers []string, routingKeys []string) (*MapReducer[I, A, R], error) {
	WORKER_COUNT, err := strconv.Atoi(WORKER_COUNT_STR)
	if err != nil {
		return nil, fmt.Errorf("failed to parse WORKER_COUNT: %w", err)
	}
	var t string = "direct"
	subscribersMap := make(map[string][]string)
	for _, subscriber := range subscribers {
		subscribersMap[subscriber] = []string{""}
	}

	if len(routingKeys) == 0 {
		log.Infof("Using FANOUT")
		routingKeys = []string{""}
		t = "fanout"
	}

	if batchSize < 2 {
		return nil, fmt.Errorf("batchSize must be at least two")
	}
	connector, err := rabbitmq.Connector()
	if err != nil {
		return nil, fmt.Errorf("failed to create connector: %w", err)
	}
	connIn := rabbitmq.NewMiddleware[I](connector)

	inputCh, err := connIn.ConsumeFromRK(input, name, t, routingKeys[0], WORKER_COUNT-1, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create input channel: %w", err)
	}
	connOut := rabbitmq.NewMiddleware[R](connector)

	output, err := connOut.WriteToRK(name, subscribersMap, t)
	if err != nil {
		return nil, fmt.Errorf("failed to create output channel: %w", err)
	}

	partialResultName := partialResultName(name)
	connPartialResult := rabbitmq.NewMiddleware[A](connector)
	partialResultIn, err := connPartialResult.ConsumeFrom(partialResultName, name, WORKER_COUNT-1, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create accumulator input channel: %w", err)
	}
	partialResultOut, err := connPartialResult.WriteTo(partialResultName, []string{})
	if err != nil {
		return nil, fmt.Errorf("failed to create accumulator output channel: %w", err)
	}

	finalReuceName := finalReuceName(name)
	connFinalReduce := rabbitmq.NewMiddleware[A](connector)
	var finalReduceIn *middleware.Receiver[A] = nil
	if WORKER_ID == "1" {
		// Only leader gets to consume from the final reduce queue
		finalReduceIn, err := connFinalReduce.ConsumeFrom(finalReuceName, name, WORKER_COUNT-1, 1)
		if err != nil {
			return nil, fmt.Errorf("failed to create accumulator input channel: %w", err)
		}
	}
	finalReduceOut, err := connFinalReduce.WriteTo(finalReuceName, []string{})
	if err != nil {
		return nil, fmt.Errorf("failed to create accumulator output channel: %w", err)
	}

	return &MapReducer[I, A, R]{
		MapReduce:             mapReducer,
		BatchSize:             batchSize,
		Input:                 inputCh,
		PartialResultSender:   partialResultOut,
		PartialResultReceiver: partialResultIn,
		FinalReduceSender:     finalReduceOut,
		FinalReduceReceiver:   finalReduceIn,
		Output:                output,
		ClientBatches:         make(map[string][]A),
		timeoutStep:           0,
		backoff:               0,
	}, nil
}

func partialResultName(name string) string {
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
	defer mr.PartialResultSender.Close()
	defer mr.Output.Close()
	inputChan := mr.readInput()
	accBatch := mr.reduceBattchess()
	finalReduce := mr.finalReduce()
	var err error
	for range 3 {

	select {
	case err = <-inputChan:
		if err != nil {
			return fmt.Errorf("error reading input: %w", err)
		}
	case err = <-accBatch:
		if err != nil {
			return fmt.Errorf("error reducing batch: %w", err)
		}
	case err = <-finalReduce:
		if err != nil {
			return fmt.Errorf("error final reducing: %w", err)
		}
	}
	}

	return err
}

func (mr *MapReducer[I, A, R]) reduceBattchess() <-chan error {
	res := make(chan error)
	task := func() {
		log.Infof("Starting map-reduce operation")
		mr.resetBackoff()
		log.Debugf("reduceBattchess(%d)", mr.backoff)
		var err error
		defer func() {
			res <- err
			close(res)
		}()

		nextCh := mr.asyncNext()
		for {
			select {
			case res := <-nextCh:
				if res.err != nil {
					if res.err.Error() == "timeout reached while waiting for message" {
						mr.backoffIncrease()
						nextCh = mr.asyncNext()
						break
					}
					err = res.err
					return
				}
				if !res.ok {
					log.Fatalf("Channel closed?")
					panic("Channel closed?")
					break
				}
				mr.resetBackoff()
				switch res.a.Type() {
				case middmiddleware.TypeRow:
					mr.ProcessAccumulator(res.a.Cid(), res.a.Msg())
				default:
					// TODO: handle close
					panic("TODO")
				}
				res.a.Ack(true)
			}
		}
	}
	go task()
	return res
}

func (mr *MapReducer[I, A, R]) resetBackoff() {
	mr.timeoutStep, mr.backoff = ExponentialBackoffDuration(0)
}

func (mr *MapReducer[I, A, R]) backoffIncrease() {
	mr.timeoutStep, mr.backoff = ExponentialBackoffDuration(mr.timeoutStep)
}


func (mr *MapReducer[I, A, R]) ProcessAccumulator(cid string, msg A) error {
	clientBatch, ok:= mr.ClientBatches[cid]
	if !ok {
		batch = make([]A, 0)
		mr.ClientBatches[cid] = ClientBatch[A]{batch}
	}
	batch = append(batch, msg)
	if len(batch) < mr.BatchSize {
		return nil
	}
	reduced := mr.MapReduce.Reduce(batch)
	err = mr.PartialResultSender.Send(reduced)
	if err != nil {
		err = fmt.Errorf("error sending partial result: %w", err)
		return
	}


			log.Infof("Entering solo mode")
			batchLen1Count := 0
			for {
				nack()
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
					continue
				}

				if producerCount == 1 && len(batch) == 1 {
					batchLen1Count++
					if batchLen1Count > 5 {
						log.Debugf("Last batch processed")
						reduced := mr.MapReduce.Reduce(batch)
						log.Debugf("Reduced partial result: %v", reduced)
						outputs := mr.MapReduce.Output(reduced)
						for _, output := range outputs {
							err = mr.Output.Send(output)
							if err != nil {
								err = fmt.Errorf("error sending output: %w", err)
								return
							}
						}
						log.Debugf("Sent output")
						err = ack()
						return
					}
				}
				log.Debugf("Batch read completed: len = %v", len(batch))
				err = ack()
				timeoutStep, backoff = ExponentialBackoffDuration(0)
				reduced := mr.MapReduce.Reduce(batch)
				log.Infof("Reduced partial result: %v", reduced)
				err = mr.PartialResultSender.Send(reduced)
				if err != nil {
					err = fmt.Errorf("error sending partial result: %w", err)
					return
				}
}

type asyncNextResult[A any] struct {
	a   middleware.Envelope[A]
	ok  bool
	err error
}

func (mr *MapReducer[I, A, R]) asyncNext() chan asyncNextResult[A] {
	res := make(chan asyncNextResult[A])
	go func() {
		<-time.NewTimer(time.Millisecond * time.Duration(backoff)).C
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(INITIAL_TIMEOUT_DURATION))
		a, ok, err := mr.PartialResultReceiver.Next(ctx)
		cancel()
		res <- asyncNextResult[A]{a, ok, err}
	}()
	return res
}

func (mr *MapReducer[I, A, R]) readInput() <-chan error {
	res := make(chan error)
	task := func() {
		var err error
		defer func() {
			res <- err
		}()
		for {
			var envelope middleware.Envelope[I]
			var ok bool
			envelope, ok, err = mr.Input.Next(nil)
			if err != nil {
				return
			}
			if !ok {
				log.Debugf("No more messages available")
				return
			}

			msg := envelope.Msg()
			log.Debugf("Received input")
			// log.Debugf("Mapping row: %v", msg)
			acc := mr.MapReduce.Map(msg)
			for _, a := range acc {
				err = mr.PartialResultSender.Send(a, envelope.Cid(), middleware.QueryRow)
				if err != nil {
					err = fmt.Errorf("error sending partial result: %w", err)
					return
				}
			}
			envelope.Ack(false)
		}
	}
	go task()
	return res
}
