package map_reducer

import (
	"context"
	"fmt"
	"reflect"

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
	log                       *logger.ConsoleLogger
	MapReduce                 MapReduce[I, A, R]
	BatchSize                 uint
	Input                     middleware.Receiver[I]
	PartialResultSender       middleware.Sender[A]
	PartialResultReceiver     middleware.Receiver[A]
	FinalReduceSender         middleware.Sender[A]
	FinalReduceReceiver       map[string]middleware.Receiver[A] // It will be null for all but the leader
	Output                    middleware.Sender[R]
	RoutingKey                string
	PartReduceBatchesForPart  map[string]*ClientBatch[A]
	PartReduceBatchesForFinal map[string]*ClientBatch[A]
	FinalReduceBatches        map[string]*ClientBatch[A]
	CidPrunnedTwice           map[string]bool
	timeoutStep               uint
	timeoutStepFinal          uint
	shardCount                uint
	pruneCounter              map[string]uint
	eofCounter                map[string]uint
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
	routingKey string,
	routingKeys []string,
	id string,
	count uint,
	shardCount uint,
) (*MapReducer[I, A, R], error) {
	log := logger.NewConsoleLogger(fmt.Sprintf("worker_%s", id), logger.Info)

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
	const prefetch = 30

	if batchSize < 2 {
		return nil, fmt.Errorf("batchSize must be at least two")
	}

	middlewareLogger := logger.NewConsoleLogger(fmt.Sprintf("middleware_%s", id), logger.Info)
	connIn := rabbitmq.NewMiddleware[I](connector, middlewareLogger)

	inputCh, err := connIn.ConsumeFromRK(input, name, t, routingKey, count, prefetch)
	if err != nil {
		return nil, fmt.Errorf("failed to create input channel: %w", err)
	}
	connOut := rabbitmq.NewMiddleware[R](connector, middlewareLogger)

	output, err := connOut.WriteTo(name, subscribers)
	if err != nil {
		return nil, fmt.Errorf("failed to create output channel: %w", err)
	}

	partialResultName := partialResultName(input, name, routingKey)
	connPartialResult := rabbitmq.NewMiddleware[A](connector, middlewareLogger)
	partialResultIn, err := connPartialResult.ConsumeFrom(partialResultName, name, count, prefetch)
	if err != nil {
		return nil, fmt.Errorf("failed to create accumulator input channel: %w", err)
	}
	partialResultOut, err := connPartialResult.WriteTo(partialResultName, []string{})
	if err != nil {
		return nil, fmt.Errorf("failed to create accumulator output channel: %w", err)
	}

	finalReuceName := finalReduceName(input, name)
	connFinalReduce := rabbitmq.NewMiddleware[A](connector, middlewareLogger)
	var finalReduceInMap = make(map[string]middleware.Receiver[A])
	if id == "1" {
		for _, rk := range routingKeys {
			// Only leader gets to consume from the final reduce queue
			finalReduceIn, err := connFinalReduce.ConsumeFromRK(finalReuceName, name, "direct", rk, 1, prefetch)
			if err != nil {
				return nil, fmt.Errorf("failed to create accumulator input channel: %w", err)
			}
			finalReduceInMap[rk] = finalReduceIn
		}
	}
	finalReduceOut, err := connFinalReduce.WriteToRK(finalReuceName, map[string][]string{}, "direct")
	if err != nil {
		return nil, fmt.Errorf("failed to create accumulator output channel: %w", err)
	}

	return &MapReducer[I, A, R]{
		log:                       log,
		MapReduce:                 mapReducer,
		BatchSize:                 batchSize,
		Input:                     inputCh,
		PartialResultSender:       partialResultOut,
		PartialResultReceiver:     partialResultIn,
		FinalReduceSender:         finalReduceOut,
		FinalReduceReceiver:       finalReduceInMap,
		Output:                    output,
		PartReduceBatchesForPart:  make(map[string]*ClientBatch[A]),
		PartReduceBatchesForFinal: make(map[string]*ClientBatch[A]),
		FinalReduceBatches:        make(map[string]*ClientBatch[A]),
		CidPrunnedTwice:           make(map[string]bool),
		timeoutStep:               0,
		timeoutStepFinal:          0,
		shardCount:                shardCount,
		RoutingKey:                routingKey,
		pruneCounter:              make(map[string]uint),
		eofCounter:                make(map[string]uint),
	}, nil
}

func partialResultName(input string, name string, routingKey string) string {
	return fmt.Sprintf("%s->%s_%s:partial_accumulators", input, name, routingKey)
}

func finalReduceName(input string, name string) string {
	return fmt.Sprintf("%s->%s:final_reduce", input, name)
}

type MapReduce[T, A, R any] interface {
	Map(T) []A
	Reduce([]A) A
	Output(A) []R
}

const INITIAL_TIMEOUT_DURATION = 3000
const WATING_JITTER = 100

func (mr *MapReducer[I, A, R]) Run(ctx context.Context) error {
	defer mr.Input.Close()
	defer mr.PartialResultReceiver.Close()
	defer mr.PartialResultSender.Close()
	defer func() {
		for _, receiver := range mr.FinalReduceReceiver {
			receiver.Close()
		}
	}()
	defer mr.FinalReduceSender.Close()
	defer mr.Output.Close()
	inputChan := mr.readInput(ctx)
	accBatch := mr.reduceBattchess(ctx)
	finalReduce := mr.finalReduce(ctx)
	var err error
	for range 3 {
		select {
		case err = <-inputChan:
			mr.log.Infof("input : Completed")
			if err != nil {
				mr.log.Errorf("error reading input: %s", err)
				return fmt.Errorf("error reading input: %w", err)
			}
			inputChan = nil
		case err = <-accBatch:
			mr.log.Infof("reduc : Completed")
			if err != nil {
				mr.log.Errorf("error reducing batch: %s", err)
				return fmt.Errorf("error reducing batch: %w", err)
			}
			accBatch = nil
		case err = <-finalReduce:
			mr.log.Infof("final : Completed")
			if err != nil {
				mr.log.Errorf("error final reducing: %s", err)
				return fmt.Errorf("error final reducing: %w", err)
			}
			finalReduce = nil
		}
	}
	mr.log.Infof("MapReducer : Run | Completed")
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
		mr.log.Infof("input : Starting maper")
		for {
			var envelope middleware.Envelope[I]
			envelope, err = mr.Input.Next(ctx)
			if err != nil {
				if (err.Error() == middleware.TimeoutErr{}.Error()) {
					err = nil
					mr.log.Infof("input : Received termination signal")
				}
				return
			}
			switch envelope.Type() {
			case middleware.Normal:
				msg := envelope.Msg()
				mr.log.Debugf("input : %s | Received input", envelope.Cid())
				acc := mr.MapReduce.Map(msg)
				for _, a := range acc {
					err = mr.PartialResultSender.Send(a, envelope.Cid())
					if err != nil {
						envelope.Nack(true)
						err = fmt.Errorf("error sending partial result: %w", err)
						return
					}
					mr.log.Debugf("input : %s | Sent partial result %+v", envelope.Cid(), a)
				}
				envelope.Ack(false)
			case middleware.EOF:
				mr.log.Debugf("input : %s | Received EOF", envelope.Cid())
				err = mr.PartialResultSender.SendEOF(envelope.Cid())
				if err != nil {
					envelope.Nack(true)
					err = fmt.Errorf("error sending final result: %w", err)
					return
				}
				mr.log.Debugf("input : %s | Sent EOF", envelope.Cid())
				envelope.Ack(true)
			case middleware.Prune:
				mr.log.Infof("input : %s | Received prune: prunning output", envelope.Cid())
				err := mr.PartialResultSender.Prune(envelope.Cid())
				if err != nil {
					mr.log.Errorf("input : %s | Prune failed: %s", envelope.Cid(), err)
					envelope.Nack(true)
				}
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
		mr.log.Infof("reduc : Starting partial reducer")
		var err error
		defer func() {
			mr.log.Debugf("reduc : Shutdown")
			res <- err
			close(res)
		}()

		for {
			var e middleware.Envelope[A]
			e, err = mr.PartialResultReceiver.Next(ctx)
			if err != nil {
				if (err.Error() == middleware.TimeoutErr{}.Error()) {
					err = nil
					mr.log.Infof("reduc : Received termination signal")
				}
				mr.log.Debugf("reduc : err in next: %s", err)
				return
			}
			switch e.Type() {
			case middleware.Normal:
				err = mr.ReduceAndSend(e.Cid(), e.Msg())
				if err != nil {
					mr.log.Errorf("reduc : %s | ReduceAndSend failed: %s", e.Cid(), err)
					e.Nack(true)
					return
				}
				e.Ack(true)
			case middleware.EOF:
				mr.log.Debugf("reduc : %s | Received EOF", e.Cid())
				if mr.CidPrunnedTwice[e.Cid()] {
					mr.log.Debugf("reduc : %s | Sending EOF to FinalReduceSender", e.Cid())
					err = mr.FinalReduceSender.SendEOFRK(mr.RoutingKey, e.Cid())
					delete(mr.CidPrunnedTwice, e.Cid())
				} else {
					mr.log.Debugf("reduc : %s | Sending EOF to PartialResultSender", e.Cid())
					err = mr.PartialResultSender.SendEOF(e.Cid())
				}
				if err != nil {
					mr.log.Errorf("reduc : %s | SendEOF failed: %s", e.Cid(), err)
					e.Nack(true)
					return
				}
				e.Ack(true)
			case middleware.Prune:
				mr.log.Debugf("reduc: %s | Received Prune", e.Cid())

				err = mr.Prune(e.Cid())
				if err != nil {
					mr.log.Errorf("reduc : %s | Prune failed: %s", e.Cid(), err)
					e.Nack(true)
					return
				}
				err := mr.PartialResultSender.Prune(e.Cid())
				if err != nil {
					mr.log.Errorf("input : %s | Prune failed: %s", e.Cid(), err)
					e.Nack(true)
				}
				err = mr.FinalReduceSender.Prune(e.Cid())
				if err != nil {
					mr.log.Errorf("input : %s | Prune failed: %s", e.Cid(), err)
					e.Nack(true)
				}
				e.Ack(true)
			}
		}
	}
	go task()
	return res
}

func (mr *MapReducer[I, A, R]) ReduceAndSend(cid string, msg A) error {
	// mr.log.Infof("reduc : %s | ReduceAndSend ", cid)
	clientBatch, ok := mr.PartReduceBatchesForPart[cid]
	if ok {
		// mr.log.Infof("reduc : %s | ReduceAndSend partial", cid)
		clientBatch.append(msg)
		if clientBatch.len() < mr.BatchSize {
			// mr.log.Infof("reduc : %s | ReduceAndSend | Batch size not reached", cid)
			return nil
		}
		// mr.log.Infof("reduc : %s | ReduceAndSend | Batch size reached", cid)
		reduced := mr.MapReduce.Reduce(clientBatch.flush())
		err := mr.PartialResultSender.Send(reduced, cid)
		if err != nil {
			return fmt.Errorf("error sending partial result: %w", err)
		}
		// mr.log.Infof("reduc : %s | ReduceAndSend | Partial result sent ", cid)
		return nil
	}
	clientBatch, ok = mr.PartReduceBatchesForFinal[cid]
	if ok {
		// mr.log.Infof("reduc : %s | ReduceAndSend final", cid)
		clientBatch.append(msg)
		if clientBatch.len() < mr.BatchSize {
			// mr.log.Infof("reduc : %s | ReduceAndSend | Batch size not reached", cid)
			return nil
		}
		// mr.log.Infof("reduc : %s | ReduceAndSend | Batch size reached", cid)
		reduced := mr.MapReduce.Reduce(clientBatch.flush())
		err := mr.FinalReduceSender.SendRK(reduced, mr.RoutingKey, cid)
		if err != nil {
			return fmt.Errorf("error sending final result: %w", err)
		}
		// mr.log.Infof("reduc : %s | ReduceAndSend | Final result sent ", cid)
		return nil
	}
	// mr.log.Infof("reduc : %s | ReduceAndSend | Creating new batch", cid)
	batch := []A{msg}
	clientBatch = &ClientBatch[A]{batch}
	mr.PartReduceBatchesForPart[cid] = clientBatch
	return nil
}

func (mr *MapReducer[I, A, R]) Prune(cid string) error {
	// mr.log.Infof("reduc : %s | Pruning partial reducer", cid)
	clientBatch, ok := mr.PartReduceBatchesForPart[cid]
	if ok {
		mr.log.Infof("reduc : %s | First prune", cid)
		if len(clientBatch.batch) > 0 {
			mr.log.Infof("reduc : %s | First prune | Sending partial result to Final", cid)
			reduced := mr.MapReduce.Reduce(clientBatch.flush())
			err := mr.FinalReduceSender.SendRK(reduced, mr.RoutingKey, cid)
			if err != nil {
				return fmt.Errorf("error sending partial result: %w", err)
			}
		} else {
			// mr.log.Infof("reduc : %s | First prune | No partial result to send", cid)
		}
		delete(mr.PartReduceBatchesForPart, cid)
		mr.PartReduceBatchesForFinal[cid] = &ClientBatch[A]{make([]A, 0)}
		return nil
	}
	clientBatch, ok = mr.PartReduceBatchesForFinal[cid]
	if ok {
		mr.log.Infof("reduc : %s | Second prune", cid)
		if len(clientBatch.batch) > 0 {
			reduced := mr.MapReduce.Reduce(clientBatch.flush())
			err := mr.FinalReduceSender.SendRK(reduced, mr.RoutingKey, cid)
			if err != nil {
				return fmt.Errorf("error sending partial result: %w", err)
			}
			delete(mr.PartReduceBatchesForFinal, cid)
		}
		mr.CidPrunnedTwice[cid] = true
		return nil
	}
	mr.log.Infof("reduc : %s | First prune empty", cid)
	mr.PartReduceBatchesForFinal[cid] = &ClientBatch[A]{make([]A, 0)}
	return nil
}

type NextAsyncRes[T codec.Serializable[T]] struct {
	received middleware.Envelope[T]
	err      error
}

func (mr *MapReducer[I, A, R]) NewIterator(ctx context.Context) *Iterator[A] {
	readCtx, cancelNexts := context.WithCancel(ctx)
	cases := make([]reflect.SelectCase, len(mr.FinalReduceReceiver))
	j := 0
	for _, receiver := range mr.FinalReduceReceiver {
		handle := make(chan NextAsyncRes[A], 0)
		cases[j] = reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(handle),
		}
		go func() {
			for {
				received, err := receiver.Next(readCtx)
				handle <- NextAsyncRes[A]{received, err}
			}
		}()
		j++
	}
	return &Iterator[A]{
		cases,
		cancelNexts,
	}
}

type Iterator[T codec.Serializable[T]] struct {
	cases       []reflect.SelectCase
	cancelNexts context.CancelFunc
}

func (it *Iterator[T]) Next(ctx context.Context) (int, middleware.Envelope[T], error) {
	i, value, ok := reflect.Select(it.cases)
	if !ok {
		panic("channel closed")
	}
	asyncRes := value.Interface().(NextAsyncRes[T])
	return i, asyncRes.received, asyncRes.err
}

func (mr *MapReducer[I, A, R]) finalReduce(ctx context.Context) chan error {
	res := make(chan error)
	task := func() {
		var err error
		defer func() {
			res <- err
			close(res)
		}()
		if len(mr.FinalReduceReceiver) == 0 {
			mr.log.Debugf("Final : Worker is not the master, skipping final reduce")
			return
		}
		// FinalReduceReceiver := *mr.FinalReduceReceiver
		iterator := mr.NewIterator(ctx)
		defer iterator.cancelNexts()

		mr.log.Infof("Final : Starting final reducer")

		for {
			_, e, err := iterator.Next(ctx)
			if err != nil {
				if (err.Error() == middleware.TimeoutErr{}.Error()) {
					err = nil
					mr.log.Infof("Final : Received termination signal")
				}
				return
			}
			switch e.Type() {
			case middleware.Normal:
				mr.log.Infof("Final : %s | Saving final reduce batch", e.Cid())
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
				mr.log.Infof("Final : %s | Received EOF", e.Cid())
				count, ok := mr.eofCounter[e.Cid()]
				if !ok {
					mr.log.Infof("Final : %s | Received first EOF", e.Cid())
					count = mr.shardCount
				}
				count--
				if count > 0 {
					mr.log.Infof("Final : %s | Received EOF, but not the last one", e.Cid())
					mr.eofCounter[e.Cid()] = count
					e.Ack(true)
					continue
				}
				mr.log.Infof("Final : %s | Received last EOF", e.Cid())
				delete(mr.eofCounter, e.Cid())
				mr.log.Infof("Final : %s | Sending EOF after sending partial result", e.Cid())
				err = mr.Output.SendEOF(e.Cid())
				if err != nil {
					e.Nack(true)
					mr.log.Fatalf("Final : error sending EOF after sending partial result: %s", err)
					panic("Resending EOF not implemented")
				}
				e.Ack(true)
			case middleware.Prune:
				mr.log.Infof("Final : %s | Received PRUNE", e.Cid())
				count, ok := mr.pruneCounter[e.Cid()]
				if !ok {
					mr.log.Infof("Final : %s | Received first PRUNE", e.Cid())
					count = mr.shardCount
				}
				count--
				if count > 0 {
					mr.log.Infof("Final : %s | Received PRUNE, but not the last one", e.Cid())
					mr.pruneCounter[e.Cid()] = count
					e.Ack(true)
					continue
				}
				mr.log.Infof("Final : %s | Received last PRUNE", e.Cid())
				delete(mr.pruneCounter, e.Cid())
				mr.log.Infof("Final : %s | Pruning final reduce batch", e.Cid())
				clientBatch, ok := mr.FinalReduceBatches[e.Cid()]
				if !ok {
					mr.log.Infof("Final : %s | Final Reduce batch not found on Prune", e.Cid())
					e.Ack(true)
					break
				}
				if len(clientBatch.batch) == 0 {
					mr.log.Infof("Final : %s | Final Reduce batch is empty on Prune", e.Cid())
					e.Ack(true)
					break
				}
				mr.log.Debugf("Final : %s | clientBatch before: %v", e.Cid(), clientBatch)
				reduced := mr.MapReduce.Reduce(clientBatch.flush())
				output := mr.MapReduce.Output(reduced)
				for _, o := range output {
					mr.log.Infof("Final : %s | Sending partial result to output: %v", e.Cid(), o)
					err = mr.Output.Send(o, e.Cid())
				}
				if err != nil {
					e.Nack(true)
					err = fmt.Errorf("error sending partial result: %w", err)
					return
				}
				err := mr.Output.Prune(e.Cid())
				if err != nil {
					mr.log.Errorf("input : %s | Prune failed: %s", e.Cid(), err)
					e.Nack(true)
				}

				delete(mr.FinalReduceBatches, e.Cid())
				e.Ack(true)
			}
		}
	}
	go task()
	return res
}
