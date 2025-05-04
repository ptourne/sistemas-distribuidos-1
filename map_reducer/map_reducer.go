package map_reducer

import (
	"context"
	"fmt"
	"math/rand/v2"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

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
	CidPrunnedTwice           map[string]bool
	timeoutStep               uint
	timeoutStepFinal          uint
	backoff                   uint
	backoffFinal              uint
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

var log *logger.ConsoleLogger

// batchSize is the number of top groups you reduce at once
func NewMapReducer[I codec.Serializable[I], A codec.Serializable[A], R codec.Serializable[R]](
	connector *rabbitmq.RabbitMQConnector,
	name string,
	input string,
	batchSize uint,
	mapReducer MapReduce[I, A, R],
	subscribers []string,
	routingKeys []string,
	id string,
	count uint,
) (*MapReducer[I, A, R], error) {
	log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", id), logger.Debug)

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

	inputCh, err := connIn.ConsumeFromRK(input, name, t, routingKeys[0], int(count)-1, 1)
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
	partialResultIn, err := connPartialResult.ConsumeFrom(partialResultName, name, int(count)-1, 1)
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
	if id == "1" {
		// Only leader gets to consume from the final reduce queue
		finalReduceIn, err := connFinalReduce.ConsumeFrom(finalReuceName, name, int(count)-1, 1)
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
		FinalReduceBatches:        make(map[string]*ClientBatch[A]),
		CidPrunnedTwice:           make(map[string]bool),
		timeoutStep:               0,
		timeoutStepFinal:          0,
		backoff:                   0,
		backoffFinal:              0,
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

func (mr *MapReducer[I, A, R]) Run(ctx context.Context) error {
	defer mr.Input.Close()
	defer mr.PartialResultReceiver.Close()
	defer mr.PartialResultSender.Close()
	if mr.FinalReduceReceiver != nil {
		defer (*mr.FinalReduceReceiver).Close()
	}
	defer mr.FinalReduceSender.Close()
	defer mr.Output.Close()
	inputChan := mr.readInput(ctx)
	accBatch := mr.reduceBattchess(ctx)
	finalReduce := mr.finalReduce(ctx)
	var err error
	for range 3 {
		select {
		case err = <-inputChan:
			log.Infof("input : Completed")
			if err != nil {
				log.Errorf("error reading input: %s", err)
				return fmt.Errorf("error reading input: %w", err)
			}
		case err = <-accBatch:
			log.Infof("reduc : Completed")
			if err != nil {
				log.Errorf("error reducing batch: %s", err)
				return fmt.Errorf("error reducing batch: %w", err)
			}
		case err = <-finalReduce:
			log.Infof("final : Completed")
			if err != nil {
				log.Errorf("error final reducing: %s", err)
				return fmt.Errorf("error final reducing: %w", err)
			}
		}
	}
	log.Infof("MapReducer : Run | Completed")
	return err
}

func (mr *MapReducer[I, A, R]) readInput(ctx context.Context) <-chan error {
	res := make(chan error)
	task := func() {
		var err error
		defer func() {
			res <- err
			close(res)
		}()
		log.Infof("input : Starting maper")
		for {
			var envelope middleware.Envelope[I]
			envelope, err = mr.Input.Next(ctx)
			if err != nil {
				if (err.Error() == middleware.TimeoutErr{}.Error()) {
					err = nil
					log.Infof("input : Received termination signal")
				}
				return
			}
			switch envelope.Type() {
			case middleware.Normal:
				msg := envelope.Msg()
				log.Debugf("input : %s | Received input", envelope.Cid())
				acc := mr.MapReduce.Map(msg)
				for _, a := range acc {
					err = mr.PartialResultSender.Send(a, envelope.Cid())
					if err != nil {
						envelope.Nack(true)
						err = fmt.Errorf("error sending partial result: %w", err)
						return
					}
					log.Debugf("input : %s | Sent partial result", envelope.Cid())
				}
				envelope.Ack(false)
			case middleware.EOF:
				log.Debugf("input : %s | Received EOF", envelope.Cid())
				err = mr.PartialResultSender.SendEOF(envelope.Cid())
				if err != nil {
					envelope.Nack(true)
					err = fmt.Errorf("error sending final result: %w", err)
					return
				}
				log.Debugf("input : %s | Sent EOF", envelope.Cid())
				envelope.Ack(true)
			case middleware.Prune:
				log.Infof("input : %s | Received prune: nothing to do", envelope.Cid())
				envelope.Ack(true)
			}
		}
	}
	go task()
	return res
}

func (mr *MapReducer[I, A, R]) reduceBattchess(ctx context.Context) <-chan error {
	res := make(chan error)
	task := func() {
		log.Infof("reduc : Starting partial reducer")
		mr.resetBackoff()
		log.Debugf("reduc : backoff %d", mr.backoff)
		var err error
		defer func() {
			log.Debugf("reduc : Shutdown")
			res <- err
			close(res)
		}()

		for {
			var e middleware.Envelope[A]
			e, err = mr.PartialResultReceiver.Next(ctx)
			if err != nil {
				if (err.Error() == middleware.TimeoutErr{}.Error()) {
					err = nil
					log.Infof("reduc : Received termination signal")
				}
				return
			}
			mr.resetBackoff()
			switch e.Type() {
			case middleware.Normal:
				err = mr.ReduceAndSend(e.Cid(), e.Msg())
				if err != nil {
					log.Errorf("reduc : %s | ReduceAndSend failed: %s", e.Cid(), err)
					e.Nack(true)
					return
				}
				e.Ack(true)
			case middleware.EOF:
				log.Debugf("reduc : %s | Received EOF", e.Cid())
				if mr.CidPrunnedTwice[e.Cid()] {
					log.Debugf("reduc : %s | Sending EOF to FinalReduceSender", e.Cid())
					err = mr.FinalReduceSender.SendEOF(e.Cid())
					delete(mr.CidPrunnedTwice, e.Cid())
				} else {
					log.Debugf("reduc : %s | Sending EOF to PartialResultSender", e.Cid())
					err = mr.PartialResultSender.SendEOF(e.Cid())
				}
				if err != nil {
					log.Errorf("reduc : %s | SendEOF failed: %s", e.Cid(), err)
					e.Nack(true)
					return
				}
				e.Ack(true)
			case middleware.Prune:
				err = mr.Prune(e.Cid())
				if err != nil {
					log.Errorf("reduc : %s | Prune failed: %s", e.Cid(), err)
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

func (mr *MapReducer[I, A, R]) ReduceAndSend(cid string, msg A) error {
	log.Infof("reduc : %s | ReduceAndSend ", cid)
	clientBatch, ok := mr.PartReduceBatchesForPart[cid]
	if ok {
		log.Infof("reduc : %s | ReduceAndSend partial", cid)
		clientBatch.append(msg)
		if clientBatch.len() < mr.BatchSize {
			log.Infof("reduc : %s | ReduceAndSend | Batch size not reached", cid)
			return nil
		}
		log.Infof("reduc : %s | ReduceAndSend | Batch size reached", cid)
		reduced := mr.MapReduce.Reduce(clientBatch.flush())
		err := mr.PartialResultSender.Send(reduced, cid)
		if err != nil {
			return fmt.Errorf("error sending partial result: %w", err)
		}
		log.Infof("reduc : %s | ReduceAndSend | Partial result sent ", cid)
		return nil
	}
	clientBatch, ok = mr.PartReduceBatchesForFinal[cid]
	if ok {
		log.Infof("reduc : %s | ReduceAndSend final", cid)
		clientBatch.append(msg)
		if clientBatch.len() < mr.BatchSize {
			log.Infof("reduc : %s | ReduceAndSend | Batch size not reached", cid)
			return nil
		}
		log.Infof("reduc : %s | ReduceAndSend | Batch size reached", cid)
		reduced := mr.MapReduce.Reduce(clientBatch.flush())
		err := mr.FinalReduceSender.Send(reduced, cid)
		if err != nil {
			return fmt.Errorf("error sending final result: %w", err)
		}
		log.Infof("reduc : %s | ReduceAndSend | Final result sent ", cid)
		return nil
	}
	log.Infof("reduc : %s | ReduceAndSend | Creating new batch", cid)
	batch := []A{msg}
	clientBatch = &ClientBatch[A]{batch}
	mr.PartReduceBatchesForPart[cid] = clientBatch
	return nil
}

func (mr *MapReducer[I, A, R]) Prune(cid string) error {
	log.Infof("reduc : %s | Pruning partial reducer", cid)
	clientBatch, ok := mr.PartReduceBatchesForPart[cid]
	if ok {
		log.Infof("reduc : %s | First prune", cid)
		if len(clientBatch.batch) > 0 {
			log.Infof("reduc : %s | First prune | Sending partial result to Final", cid)
			reduced := mr.MapReduce.Reduce(clientBatch.flush())
			err := mr.FinalReduceSender.Send(reduced, cid)
			if err != nil {
				return fmt.Errorf("error sending partial result: %w", err)
			}
		} else {
			log.Infof("reduc : %s | First prune | No partial result to send", cid)
		}
		delete(mr.PartReduceBatchesForPart, cid)
		mr.PartReduceBatchesForFinal[cid] = &ClientBatch[A]{make([]A, 0)}
		return nil
	}
	clientBatch, ok = mr.PartReduceBatchesForFinal[cid]
	if ok {
		log.Infof("reduc : %s | Second prune", cid)
		if len(clientBatch.batch) > 0 {
			reduced := mr.MapReduce.Reduce(clientBatch.flush())
			err := mr.FinalReduceSender.Send(reduced, cid)
			if err != nil {
				return fmt.Errorf("error sending partial result: %w", err)
			}
			delete(mr.PartReduceBatchesForFinal, cid)
		}
		mr.CidPrunnedTwice[cid] = true
		return nil
	}
	log.Infof("reduc : %s | First prune empty", cid)
	mr.PartReduceBatchesForFinal[cid] = &ClientBatch[A]{make([]A, 0)}
	return nil
}

func (mr *MapReducer[I, A, R]) finalReduce(ctx context.Context) chan error {
	res := make(chan error)
	task := func() {
		var err error
		defer func() {
			res <- err
			close(res)
		}()
		if mr.FinalReduceReceiver == nil {
			log.Debugf("Final : Worker is not the master, skipping final reduce")
			return
		}
		FinalReduceReceiver := *mr.FinalReduceReceiver
		log.Infof("Final : Starting final reducer")
		mr.resetBackoffFinal()
		log.Debugf("Final : backoffFinal %d", mr.backoffFinal)

		for {
			// <-time.NewTimer(time.Millisecond * time.Duration(mr.backoffFinal)).C
			// ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*time.Duration(INITIAL_TIMEOUT_DURATION))
			e, err := FinalReduceReceiver.Next(ctx)
			// cancel()
			if err != nil {
				if (err.Error() == middleware.TimeoutErr{}.Error()) {
					err = nil
					log.Infof("Final : Received termination signal")
				}
				return
			}
			switch e.Type() {
			case middleware.Normal:
				log.Infof("Final : %s | Saving final reduce batch", e.Cid())
				clientBatch, ok := mr.FinalReduceBatches[e.Cid()]
				if !ok {
					batch := []A{e.Msg()}
					clientBatch = &ClientBatch[A]{batch}
					mr.FinalReduceBatches[e.Cid()] = clientBatch
				} else {
					clientBatch.append(e.Msg())
				}
				e.Ack(true)
			case middleware.EOF:
				log.Infof("Final : %s | Sending EOF after sending partial result", e.Cid())
				err = mr.Output.SendEOF(e.Cid())
				if err != nil {
					e.Nack(true)
					log.Fatalf("Final : error sending EOF after sending partial result: %s", err)
					panic("Resending EOF not implemented")
				}
			case middleware.Prune:
				log.Infof("Final : %s | Pruning final reduce batch", e.Cid())
				clientBatch, ok := mr.FinalReduceBatches[e.Cid()]
				if !ok {
					log.Errorf("Final : %s | Final Reduce batch not found on Prune", e.Cid())
					break
				}
				if len(clientBatch.batch) == 0 {
					log.Errorf("Final : %s | Final Reduce batch is empty on Prune", e.Cid())
					break
				}
				log.Debugf("Final : %s | clientBatch before: %v", e.Cid(), clientBatch)
				reduced := mr.MapReduce.Reduce(clientBatch.flush())
				output := mr.MapReduce.Output(reduced)
				for _, o := range output {
					err = mr.Output.Send(o, e.Cid())
				}
				if err != nil {
					e.Nack(true)
					err = fmt.Errorf("error sending partial result: %w", err)
					return
				}

				delete(mr.FinalReduceBatches, e.Cid())
				e.Ack(true)
			}
		}
	}
	go task()
	return res
}

func (mr *MapReducer[I, A, R]) resetBackoff() {
	// mr.timeoutStep, mr.backoff = ExponentialBackoffDuration(0)
	mr.timeoutStep = 0
	mr.backoff = 0
}

func (mr *MapReducer[I, A, R]) backoffIncrease() {
	// mr.timeoutStep, mr.backoff = ExponentialBackoffDuration(mr.timeoutStep)
	mr.timeoutStep = 1
	mr.backoff = 500
}
func (mr *MapReducer[I, A, R]) resetBackoffFinal() {
	// mr.timeoutStepFinal, mr.backoffFinal = ExponentialBackoffDuration(0)
	mr.timeoutStepFinal = 0
	mr.backoffFinal = 0
}

func (mr *MapReducer[I, A, R]) backoffIncreaseFinal() {
	// mr.timeoutStepFinal, mr.backoffFinal = ExponentialBackoffDuration(mr.timeoutStepFinal)
	mr.timeoutStepFinal = 1
	mr.backoffFinal = 500
}
