package map_reducer

import (
	"fmt"
	"math/rand/v2"
	"os"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Debug)

// MapReducer is a struct that represents a map-reduce operation.
//
// I is the type of the input data.
// A is the type of the accumulator.
// R is the type of the final result.
type MapReducer[I codec.Serializable[I], A codec.Serializable[A], R codec.Serializable[R]] struct {
	BatchSize             uint
	Input                 middleware.Receiver[I]
	PartialResultSender   middleware.Sender[A]
	PartialResultReceiver middleware.Receiver[A]
	MapReduce             MapReduce[I, A, R]
	Output                middleware.Sender[R]
	InputClosed           bool
	RoutingKeys           []string
}

// batchSize is the number of top groups you reduce at once
func NewMapReducer[I codec.Serializable[I], A codec.Serializable[A], R codec.Serializable[R]](name string, input string, batchSize uint, mapReducer MapReduce[I, A, R], subscribers []string, routingKeys []string) (*MapReducer[I, A, R], error) {
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
		return nil, err
	}
	connIn := rabbitmq.NewMiddleware[I](connector)

	inputCh, err := connIn.ConsumeFromRK(input, name, t, routingKeys[0])
	if err != nil {
		return nil, err
	}
	connOut := rabbitmq.NewMiddleware[R](connector)

	output, err := connOut.WriteToRK(name, subscribersMap, t)
	if err != nil {
		return nil, err
	}

	accName := accName(name)
	connAcc := rabbitmq.NewMiddleware[A](connector)

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
		BatchSize:             batchSize,
		Input:                 inputCh,
		PartialResultSender:   accOut,
		PartialResultReceiver: accIn,
		MapReduce:             mapReducer,
		Output:                output,
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
	defer mr.PartialResultSender.Close()
	defer mr.Output.Close()
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
		log.Infof("Starting map-reduce operation")
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
		for !mr.InputClosed {
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
			reduced := mr.MapReduce.Reduce(batch)
			// log.Debugf("Reduced partial result: %v", reduced)
			err = mr.PartialResultSender.Send(reduced)
			if err != nil {
				err = fmt.Errorf("error sending partial result: %w", err)
				return
			}
			// log.Debugf("Sent reduced partial result")
		}
		var producerCount int
		var lastProducerCount time.Time = time.Now()
		producerCount, err = mr.PartialResultReceiver.CountProducers()
		if err != nil {
			err = fmt.Errorf("failed to get producer count")
			return
		}
		countProducers := func() (int, error) {
			if time.Since(lastProducerCount) > time.Second {
				lastProducerCount = time.Now()
				// log.Deb("Calling count producers from worker '%s'", WORKER_ID)
				return mr.PartialResultReceiver.CountProducers()
			}
			return producerCount, nil
		}
		defer nack()
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
			if len(batch) < int(mr.BatchSize) {
				var condi = os.Getenv("WORKER_CONDI")
				if WORKER_ID != "1" && condi == "" {
					log.Infof("Retiring")
					return
				}
				producerCount, err = countProducers()
				if err != nil {
					err = fmt.Errorf("failed to get producer count")
					return
				}
			}
			if len(batch) == 0 {
				continue
			}

			log.Debugf("Batch read completed: len = %v", len(batch))
			err = ack()
			timeoutStep, backoff = ExponentialBackoffDuration(0)
			reduced := mr.MapReduce.Reduce(batch)
			// log.Debugf("Reduced partial result: %v", reduced)
			err = mr.PartialResultSender.Send(reduced)
			if err != nil {
				err = fmt.Errorf("error sending partial result: %w", err)
				return
			}
			// log.Debugf("Sent reduced partial result")

			if producerCount == 1 && len(batch) == 1 {
				break
			}
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
			// log.Debugf("Sent reduced partial result")
		}
	}
	go task()
	return res
}

func (mr *MapReducer[I, A, R]) readBatch() (batch []A, lastMsg *middleware.Envelope[A], err error) {
	timer := time.NewTimer(time.Millisecond * time.Duration(INITIAL_TIMEOUT_DURATION))
	batch = make([]A, 0, mr.BatchSize)
	log.Debugf("Initial batch: %v", batch)
	for range mr.BatchSize {
		log.Debugf("Batch state: %v", batch)
		a, ok, err := mr.PartialResultReceiver.Next(timer)
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
			mr.InputClosed = true
			log.Infof("Input closed")
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
				err = mr.PartialResultSender.Send(a)
				if err != nil {
					err = fmt.Errorf("error sending partial result: %w", err)
					return
				}
			}
			// log.Debugf("Sent mapped partial result: %v", acc)
			envelope.Ack(false)
		}
	}
	go task()
	return res
}
