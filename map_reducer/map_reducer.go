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
	MapReduce                 MapReduce[I, A, R]
	BatchSize                 uint
	Input                     middleware.Receiver[I]
	PartialResultSender       middleware.Sender[A]
	PartialResultReceiver     middleware.Receiver[A]
	FinalReduceSender         middleware.Sender[A]
	FinalReduceReceiver       *middleware.Receiver[A] // It will be null for all but the leader
	Output                    middleware.Sender[R]
	RoutingKeys               []string
	PartReduceBatchesForPart  map[string]*ClientBatch[A]
	PartReduceBatchesForFinal map[string]*ClientBatch[A]
	FinalReduceBatches        map[string]*ClientBatch[A]
	timeoutStep               uint
	backoff                   uint
}

type ClientBatch[A codec.Serializable[A]] struct {
	batch []A
}

func (c *ClientBatch[A]) flush() []A {
	copy := c.batch
	c.batch = make([]A, 0)
	return copy
}

func (c ClientBatch[A]) len() uint {
	return uint(len(c.batch))
}

func (c *ClientBatch[A]) append(msg A) {
	c.batch = append(c.batch, msg)
}

// batchSize is the number of top groups you reduce at once
func NewMapReducer[I codec.Serializable[I], A codec.Serializable[A], R codec.Serializable[R]](
	connector *rabbitmq.RabbitMQConnector,
	name string,
	input string,
	batchSize uint,
	mapReducer MapReduce[I, A, R],
	subscribers []string,
	routingKeys []string,
) (*MapReducer[I, A, R], error) {
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

	finalReuceName := finalReduceName(name)
	connFinalReduce := rabbitmq.NewMiddleware[A](connector)
	var finalReduceInP *middleware.Receiver[A] = nil
	if WORKER_ID == "1" {
		// Only leader gets to consume from the final reduce queue
		finalReduceIn, err := connFinalReduce.ConsumeFrom(finalReuceName, name, WORKER_COUNT-1, 1)
		if err != nil {
			return nil, fmt.Errorf("failed to create accumulator input channel: %w", err)
		}
		finalReduceInP = &finalReduceIn
	}
	finalReduceOut, err := connFinalReduce.WriteTo(finalReuceName, []string{})
	if err != nil {
		return nil, fmt.Errorf("failed to create accumulator output channel: %w", err)
	}

	return &MapReducer[I, A, R]{
		MapReduce:                 mapReducer,
		BatchSize:                 batchSize,
		Input:                     inputCh,
		PartialResultSender:       partialResultOut,
		PartialResultReceiver:     partialResultIn,
		FinalReduceSender:         finalReduceOut,
		FinalReduceReceiver:       finalReduceInP,
		Output:                    output,
		PartReduceBatchesForPart:  make(map[string]*ClientBatch[A]),
		PartReduceBatchesForFinal: make(map[string]*ClientBatch[A]),
		timeoutStep:               0,
		backoff:                   0,
	}, nil
}

func partialResultName(name string) string {
	return name + "_acc"
}

func finalReduceName(name string) string {
	return name + "_finacc"
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
			acc := mr.MapReduce.Map(msg)
			for _, a := range acc {
				err = mr.PartialResultSender.Send(a, envelope.Cid())
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

		for {
			<-time.NewTimer(time.Millisecond * time.Duration(mr.backoff)).C
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(INITIAL_TIMEOUT_DURATION))
			e, ok, err := mr.PartialResultReceiver.Next(ctx)
			cancel()
			if err != nil {
				if err.Error() == "timeout reached while waiting for message" {
					mr.backoffIncrease()
					break
				}
				return
			}
			if !ok {
				log.Fatalf("Channel closed?")
				panic("Channel closed?")
			}
			mr.resetBackoff()
			switch e.Type() {
			case middleware.Normal:
				err = mr.ProcessAccumulator(e.Cid(), e.Msg())
				if err != nil {
					e.Nack(true)
					return
				}
				e.Ack(true)
			case middleware.EOF:
				err = mr.Prune(e.Cid())
				if err != nil {
					e.Nack(true)
					return
				}
				err = mr.PartialResultSender.SendEOF(e.Cid())
				if err != nil {
					e.Nack(true)
					return
				}
				e.Ack(true)
			case middleware.Prune:
				err = mr.Prune(e.Cid())
				if err != nil {
					e.Nack(true)
					return
				}
				e.Ack(true)
			}
		}
	}
	go task()
	return res
}

func (mr *MapReducer[I, A, R]) ProcessAccumulator(cid string, msg A) error {
	clientBatch, ok := mr.PartReduceBatchesForPart[cid]
	if !ok {
		batch := make([]A, 0)
		clientBatch = &ClientBatch[A]{batch}
		mr.PartReduceBatchesForPart[cid] = clientBatch
	}
	clientBatch.append(msg)
	if clientBatch.len() < mr.BatchSize {
		return nil
	}
	reduced := mr.MapReduce.Reduce(clientBatch.flush())
	err := mr.PartialResultSender.Send(reduced, cid)
	if err != nil {
		return fmt.Errorf("error sending partial result: %w", err)
	}
	return nil
}

func (mr *MapReducer[I, A, R]) Prune(cid string) error {
	clientBatch, ok := mr.PartReduceBatchesForPart[cid]
	if ok {
		if len(clientBatch.batch) > 0 {
			reduced := mr.MapReduce.Reduce(clientBatch.flush())
			err := mr.PartialResultSender.Send(reduced, cid)
			if err != nil {
				return fmt.Errorf("error sending partial result: %w", err)
			}
		}
		delete(mr.PartReduceBatchesForPart, cid)
		mr.PartReduceBatchesForFinal[cid] = &ClientBatch[A]{make([]A, 0)}
		return nil
	}
	clientBatch, ok = mr.PartReduceBatchesForFinal[cid]
	if !ok {
		return nil
	}
	if len(clientBatch.batch) > 0 {
		reduced := mr.MapReduce.Reduce(clientBatch.flush())
		err := mr.FinalReduceSender.Send(reduced, cid)
		if err != nil {
			return fmt.Errorf("error sending partial result: %w", err)
		}
	}
	return nil
}

func (mr *MapReducer[I, A, R]) finalReduce() chan error {
	res := make(chan error)
	task := func() {
		var err error
		defer func() {
			res <- err
			close(res)
		}()
		if mr.FinalReduceReceiver == nil {
			log.Debugf("Worker %s is not the master, skipping final reduce", WORKER_ID)
			return
		}
		FinalReduceReceiver := *mr.FinalReduceReceiver
		log.Infof("Starting map-reduce operation")
		mr.resetBackoff()
		log.Debugf("reduceBattchess(%d)", mr.backoff)

		for {
			<-time.NewTimer(time.Millisecond * time.Duration(mr.backoff)).C
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(INITIAL_TIMEOUT_DURATION))
			e, ok, err := FinalReduceReceiver.Next(ctx)
			cancel()
			if err != nil {
				if err.Error() == "timeout reached while waiting for message" {
					mr.backoffIncrease()
					continue
				}
				return
			}
			if !ok {
				log.Fatalf("Channel closed?")
				panic("Channel closed?")
			}
			mr.resetBackoff()
			switch e.Type() {
			case middleware.Normal:
				clientBatch, ok := mr.FinalReduceBatches[e.Cid()]
				if !ok {
					batch := make([]A, 0)
					clientBatch = &ClientBatch[A]{batch}
					mr.FinalReduceBatches[e.Cid()] = clientBatch
				}
				clientBatch.append(e.Msg())
				e.Ack(true)
			case middleware.EOF:
				clientBatch, ok := mr.FinalReduceBatches[e.Cid()]
				if !ok {
					log.Errorf("Finnal Reduce batch not found on EOF")
					break
				}
				if len(clientBatch.batch) == 0 {
					log.Errorf("Final Reduce batch is empty on EOF")
					break
				}
				reduced := mr.MapReduce.Reduce(clientBatch.flush())
				err = mr.PartialResultSender.Send(reduced, e.Cid())
				if err != nil {
					e.Nack(true)
					err = fmt.Errorf("error sending partial result: %w", err)
					return
				}

				delete(mr.FinalReduceBatches, e.Cid())
				err = mr.PartialResultSender.SendEOF(e.Cid())
				if err != nil {
					e.Nack(true)
					log.Fatalf("error sending EOF after sending partial result: %s", err)
					panic("Resending EOF not implemented")
					return
				}
				e.Ack(true)
			case middleware.Prune:
				log.Fatalf("Prune not expected on leader")
				panic("Prune not expected on leader")
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
