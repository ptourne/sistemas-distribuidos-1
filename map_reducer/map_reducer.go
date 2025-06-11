package map_reducer

import (
	"context"
	"fmt"
	"reflect"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq/transaction_log"
)

// MapReducer is a struct that represents a map-reduce operation.
//
// I is the type of the input data.
// A is the type of the accumulator.
// R is the type of the final result.
type MapReducer[I codec.Serializable[I], A codec.Serializable[A], R codec.Serializable[R]] struct {
	log             *logger.ConsoleLogger
	MapReduce       MapReduce[I, A, R]
	BatchSize       uint
	RoutingKey      string
	connFinalReduce middleware.Connection[A]
	partialReducer  *PartialReducer[I, A, R]
	finalReducer    *FinalReducer[I, A, R]
}

type FinalReducer[I codec.Serializable[I], A codec.Serializable[A], R codec.Serializable[R]] struct {
	FinalReduceBatches  map[string]*A
	connOut             middleware.Connection[R]
	FinalReduceReceiver map[string]middleware.Receiver[A] // It will be null for all but the leader
	Output              middleware.Sender[R]
	transactionLog      transaction_log.TransactionLog
}

// batchSize is the number of top groups you reduce at once
func NewMapReducer[I codec.Serializable[I], A codec.Serializable[A], R codec.Serializable[R]](
	connector *rabbitmq.RabbitMQConnector,
	name string,
	input string,
	batchSize uint,
	mapReducer MapReduce[I, A, R],
	subscribers []string,
	id string,
	workersCount uint,
	shardCountOutput uint,
	// dirPath string,
) (*MapReducer[I, A, R], error) {
	log := logger.NewConsoleLogger(fmt.Sprintf("worker_mp_%s", id), logger.Debug)

	prefetch := 500
	prefetchIn := 1000

	routingKey := id

	if batchSize < 2 {
		return nil, fmt.Errorf("batchSize must be at least two")
	}

	middlewareLogger := logger.NewConsoleLogger(fmt.Sprintf("middleware_mp_%s", id), logger.Debug)
	connIn := rabbitmq.NewMiddleware[I](connector, middlewareLogger)

	inputCh, err := connIn.ConsumeFrom(input, name, routingKey, prefetchIn, workersCount)
	if err != nil {
		return nil, fmt.Errorf("failed to create input channel: %w", err)
	}
	connOut := rabbitmq.NewMiddleware[R](connector, middlewareLogger)

	output, err := connOut.WriteTo(name, subscribers, id, shardCountOutput)
	if err != nil {
		return nil, fmt.Errorf("failed to create output channel: %w", err)
	}

	finalReuceName := finalReduceName(input, name)
	connFinalReduce := rabbitmq.NewMiddleware[A](connector, middlewareLogger)
	var finalReduceInMap = make(map[string]middleware.Receiver[A])
	if id == "0" {
		for i := range workersCount {
			rk := fmt.Sprintf("%d", i)
			// Only leader gets to consume from the final reduce queue
			finalReduceIn, err := connFinalReduce.ConsumeFrom(finalReuceName, name, rk, prefetch, workersCount)
			if err != nil {
				return nil, fmt.Errorf("failed to create accumulator input channel: %w", err)
			}
			finalReduceInMap[rk] = finalReduceIn
		}
	}
	finalReduceOut, err := connFinalReduce.WriteTo(finalReuceName, []string{}, id, workersCount)
	if err != nil {
		return nil, fmt.Errorf("failed to create accumulator output channel: %w", err)
	}

	mr := &MapReducer[I, A, R]{
		log:             log,
		MapReduce:       mapReducer,
		BatchSize:       batchSize,
		RoutingKey:      routingKey,
		connFinalReduce: connFinalReduce,
		partialReducer: &PartialReducer[I, A, R]{
			connIn:            connIn,
			PartReduceBatches: make(map[string]*A),
			Input:             inputCh,
			FinalReduceSender: finalReduceOut,
			transactionLog:    nil,
		},
		finalReducer: &FinalReducer[I, A, R]{
			FinalReduceBatches:  make(map[string]*A),
			connOut:             connOut,
			Output:              output,
			FinalReduceReceiver: finalReduceInMap,
			transactionLog:      nil,
		},
	}

	// mr.partialReducer.transactionLog, err = transaction_log.NewTransactionLogFromDir(dirPath, mr.partialReducer)
	// mr.finalReducer.transactionLog, err = transaction_log.NewTransactionLogFromDir(dirPath, mr.finalReducer)

	return mr, nil
}

func finalReduceName(input string, name string) string {
	return fmt.Sprintf("%s->%s:final_reduce", input, name)
}

type MapReduce[T, A, R any] interface {
	Map(T) []A
	Reduce([]A) A
	Output(A) []R
}

func (mr *MapReducer[I, A, R]) Run(ctx context.Context) error {
	defer mr.Close()
	inputChan := mr.readInput(ctx)
	finalReduce := mr.finalReduce(ctx)
	var err error
	for inputChan != nil || finalReduce != nil {
		select {
		case err = <-inputChan:
			mr.log.Infof("input : Completed")
			if err != nil {
				if err != context.Canceled {
					mr.log.Errorf("error reading input: %s", err)
					return fmt.Errorf("error reading input: %w", err)
				}
			}
			inputChan = nil
		case err = <-finalReduce:
			mr.log.Infof("final : Completed")
			if err != nil {
				if err != context.Canceled {
					mr.log.Errorf("error final reducing: %s", err)
					return fmt.Errorf("error final reducing: %w", err)
				}
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
			select {
			case <-ctx.Done():
				err = ctx.Err()
				mr.log.Infof("input : Received SIGTERM. Shutting down gracefully...")
				return
			default:
				var envelope middleware.Envelope[I]
				envelope, err = mr.partialReducer.Input.Next(ctx)
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
						mr.ReduceAndSend(envelope.Cid(), a)
					}

					envelope.Ack(false)
				case middleware.EOF:
					mr.log.Debugf("input : %s | Received EOF", envelope.Cid())
					err = mr.partialReducer.FinalReduceSender.SendEOF(envelope.Cid())
					envelope.Ack(false)
				case middleware.Prune:
					mr.log.Debugf("input : %s | Received Prune", envelope.Cid())
					clientBatch, ok := mr.partialReducer.PartReduceBatches[envelope.Cid()]
					if ok {
						err := mr.partialReducer.FinalReduceSender.Send(*clientBatch, envelope.Cid(), 0) // TODO id!!
						if err != nil {
							err = fmt.Errorf("error sending partial result: %w", err)
							return
						}
					}
					err = mr.partialReducer.FinalReduceSender.Prune(envelope.Cid())
					if err != nil {
						err = fmt.Errorf("input : %s | Prune failed: %s", envelope.Cid(), err)
						envelope.Nack(false)
						return
					}
					envelope.Ack(false)
				}
			}
		}
	}
	go task()
	return res
}

func (mr *MapReducer[I, A, R]) ReduceAndSend(cid string, msg A) error {
	mr.log.Infof("reduc : %s | ReduceAndSend ", cid)
	clientBatch, ok := mr.partialReducer.PartReduceBatches[cid]
	if ok {
		mr.log.Infof("reduc : %s | ReduceAndSend partial", cid)
		reduc := []A{*clientBatch, msg}
		reduced := mr.MapReduce.Reduce(reduc)
		mr.partialReducer.PartReduceBatches[cid] = &reduced
		return nil
	}

	mr.log.Infof("reduc : %s | ReduceAndSend | Creating new batch", cid)
	mr.partialReducer.PartReduceBatches[cid] = &msg
	return nil
}

type NextAsyncRes[T codec.Serializable[T]] struct {
	received middleware.Envelope[T]
	err      error
}

func (mr *MapReducer[I, A, R]) NewIterator(ctx context.Context) *Iterator[A] {
	readCtx, cancelNexts := context.WithCancel(ctx)
	cases := make([]reflect.SelectCase, len(mr.finalReducer.FinalReduceReceiver))
	j := 0
	for _, receiver := range mr.finalReducer.FinalReduceReceiver {
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
		if len(mr.finalReducer.FinalReduceReceiver) == 0 {
			mr.log.Debugf("Final : Worker is not the master, skipping final reduce")
			return
		}
		iterator := mr.NewIterator(ctx)
		defer iterator.cancelNexts()

		mr.log.Infof("Final : Starting final reducer")

		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				mr.log.Infof("final : Received SIGTERM. Shutting down gracefully...")
				return
			default:
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
					clientBatch, ok := mr.finalReducer.FinalReduceBatches[e.Cid()]
					if !ok {
						acc := e.Msg()
						mr.finalReducer.FinalReduceBatches[e.Cid()] = &acc
					} else {
						reduc := []A{*clientBatch, e.Msg()}
						reduced := mr.MapReduce.Reduce(reduc)
						mr.finalReducer.FinalReduceBatches[e.Cid()] = &reduced
					}
					e.Ack(false)
				case middleware.EOF:
					mr.log.Infof("Final EOF: %s | Pruning final reduce batch", e.Cid())
					clientBatch, ok := mr.finalReducer.FinalReduceBatches[e.Cid()]
					if !ok {
						mr.log.Infof("Final : %s | Final Reduce batch not found on Prune", e.Cid())
						e.Ack(false)
						break
					}
					if clientBatch == nil {
						mr.log.Infof("Final : %s | Final Reduce batch is empty on Prune", e.Cid())
						e.Ack(false)
						break
					}
					mr.log.Debugf("Final : %s | clientBatch before: %v", e.Cid(), clientBatch)
					output := mr.MapReduce.Output(*clientBatch)
					for i, o := range output {
						mr.log.Infof("Final : %s | Sending partial result to output: %v", e.Cid(), o)
						err = mr.finalReducer.Output.Send(o, e.Cid(), uint64(i))
					}
					if err != nil {
						e.Nack(false)
						err = fmt.Errorf("error sending partial result: %w", err)
						_ = err
						return
					}
					err := mr.finalReducer.Output.Prune(e.Cid())
					if err != nil {
						mr.log.Errorf("input : %s | Prune failed: %s", e.Cid(), err)
						e.Nack(false)
					}

					delete(mr.finalReducer.FinalReduceBatches, e.Cid())

					mr.log.Infof("Final : %s | Sending EOF after sending partial result", e.Cid())
					err = mr.finalReducer.Output.SendEOF(e.Cid())
					if err != nil {
						mr.log.Errorf("Final : %s | SendEOF failed: %s", e.Cid(), err)
						e.Nack(false)
						return
					}
					e.Ack(false)
				case middleware.Prune:
					mr.log.Infof("Final : %s | Received PRUNE", e.Cid())
					e.Ack(false)
				}
			}
		}
	}
	go task()
	return res
}

func (mr *MapReducer[I, A, R]) Close() {
	if err := mr.partialReducer.Input.Close(); err != nil {
		mr.log.Errorf("Error closing input channel: %s", err)
	}

	for _, receiver := range mr.finalReducer.FinalReduceReceiver {
		if err := receiver.Close(); err != nil {
			mr.log.Errorf("Error closing final reduce receiver: %s", err)
		}
	}

	if err := mr.partialReducer.FinalReduceSender.Close(); err != nil {
		mr.log.Errorf("Error closing final reduce channel (sender): %s", err)
	}

	if err := mr.finalReducer.Output.Close(); err != nil {
		mr.log.Errorf("Error closing output channel: %s", err)
	}

	if err := mr.partialReducer.connIn.Close(); err != nil {
		mr.log.Errorf("Error closing input connection: %s", err)
	}

	if err := mr.finalReducer.connOut.Close(); err != nil {
		mr.log.Errorf("Error closing output connection: %s", err)
	}

	if err := mr.connFinalReduce.Close(); err != nil {
		mr.log.Errorf("Error closing final reduce connection: %s", err)
	}
}
