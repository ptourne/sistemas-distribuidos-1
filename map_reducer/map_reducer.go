package map_reducer

import (
	"context"
	"fmt"
	"path"

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
	BatchSize       uint
	RoutingKey      string
	connFinalReduce middleware.Connection[A]
	partialReducer  *PartialReducer[I, A, R]
	finalReducer    *FinalReducer[I, A, R]
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
	dirPath string,
	maxLogSize uint64,
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
		BatchSize:       batchSize,
		RoutingKey:      routingKey,
		connFinalReduce: connFinalReduce,
		partialReducer: &PartialReducer[I, A, R]{
			log:                              log,
			MapReduce:                        mapReducer,
			connIn:                           connIn,
			ReduceBatches:                    make(map[uint64]*A),
			Receiver:                         inputCh,
			Sender:                           finalReduceOut,
			transactionLog:                   nil,
			msgsSinceLastDump:                0,
			maxLogSize:                       maxLogSize,
			lastNormalMsgIdsByCidAndSenderId: make(map[uint64]map[uint64]uint64),
		},
		finalReducer: &FinalReducer[I, A, R]{
			log:                              log,
			MapReduce:                        mapReducer,
			ReduceBatches:                    make(map[uint64]*A),
			connOut:                          connOut,
			Sender:                           output,
			Receiver:                         finalReduceInMap,
			transactionLog:                   nil,
			msgsSinceLastDump:                0,
			maxLogSize:                       maxLogSize,
			lastNormalMsgIdsByCidAndSenderId: make(map[uint64]map[uint64]uint64),
		},
	}

	partialReducerPath := path.Join(dirPath, "partial_reducer")
	mr.partialReducer.transactionLog, err = transaction_log.NewTransactionLogFromDir(partialReducerPath, mr.partialReducer)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction log for partial reducer: %w", err)
	}
	err = mr.partialReducer.Conclude()
	if err != nil {
		return nil, fmt.Errorf("failed to conclude partial reducer: %w", err)
	}
	finalReducerPath := path.Join(dirPath, "final_reducer")
	mr.finalReducer.transactionLog, err = transaction_log.NewTransactionLogFromDir(finalReducerPath, mr.finalReducer)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction log for final reducer: %w", err)
	}
	err = mr.partialReducer.Conclude()
	if err != nil {
		return nil, fmt.Errorf("failed to conclude final reducer: %w", err)
	}
	mr.partialReducer.log.Infof("MapReducer : NewMapReducer | Created with input: %s, name: %s, batchSize: %d", input, name, batchSize)

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
	inputChan := mr.partialReducer.Run(ctx)
	finalReduce := mr.finalReducer.Run(ctx)
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

func (mr *MapReducer[I, A, R]) Close() {
	if err := mr.partialReducer.Receiver.Close(); err != nil {
		mr.log.Errorf("Error closing input channel: %s", err)
	}

	for _, receiver := range mr.finalReducer.Receiver {
		if err := receiver.Close(); err != nil {
			mr.log.Errorf("Error closing final reduce receiver: %s", err)
		}
	}

	if err := mr.partialReducer.Sender.Close(); err != nil {
		mr.log.Errorf("Error closing final reduce channel (sender): %s", err)
	}

	if err := mr.finalReducer.Sender.Close(); err != nil {
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
