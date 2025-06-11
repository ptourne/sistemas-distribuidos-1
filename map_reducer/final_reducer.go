package map_reducer

import (
	"context"
	"fmt"
	"reflect"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq/transaction_log"
)

type FinalReducer[I codec.Serializable[I], A codec.Serializable[A], R codec.Serializable[R]] struct {
	log            *logger.ConsoleLogger
	MapReduce      MapReduce[I, A, R]
	ReduceBatches  map[uint64]*A
	connOut        middleware.Connection[R]
	Receiver       map[string]middleware.Receiver[A] // It will be null for all but the leader
	Sender         middleware.Sender[R]
	transactionLog transaction_log.TransactionLog
}

type NextAsyncRes[T codec.Serializable[T]] struct {
	received middleware.Envelope[T]
	err      error
}

func (fr *FinalReducer[I, A, R]) NewIterator(ctx context.Context) *Iterator[A] {
	readCtx, cancelNexts := context.WithCancel(ctx)
	cases := make([]reflect.SelectCase, len(fr.Receiver))
	j := 0
	for _, receiver := range fr.Receiver {
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

func (fr *FinalReducer[I, A, R]) Run(ctx context.Context) chan error {
	res := make(chan error)
	task := func() {
		var err error
		defer func() {
			res <- err
			close(res)
		}()
		if len(fr.Receiver) == 0 {
			fr.log.Debugf("Final : Worker is not the master, skipping final reduce")
			return
		}
		iterator := fr.NewIterator(ctx)
		defer iterator.cancelNexts()

		fr.log.Infof("Final : Starting final reducer")

		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				fr.log.Infof("final : Received SIGTERM. Shutting down gracefully...")
				return
			default:
				_, e, err := iterator.Next(ctx)
				if err != nil {
					if (err.Error() == middleware.TimeoutErr{}.Error()) {
						err = nil
						fr.log.Infof("Final : Received termination signal")
					}
					return
				}
				switch e.Type() {
				case middleware.Normal:
					fr.log.Infof("Final : %d | Saving final reduce batch", e.Cid())
					clientBatch, ok := fr.ReduceBatches[e.Cid()]
					if !ok {
						acc := e.Msg()
						fr.ReduceBatches[e.Cid()] = &acc
					} else {
						reduc := []A{*clientBatch, e.Msg()}
						reduced := fr.MapReduce.Reduce(reduc)
						fr.ReduceBatches[e.Cid()] = &reduced
					}
					e.Ack(false)
				case middleware.EOF:
					fr.log.Infof("Final EOF: %d | Pruning final reduce batch", e.Cid())
					clientBatch, ok := fr.ReduceBatches[e.Cid()]
					if !ok {
						fr.log.Infof("Final : %d | Final Reduce batch not found on Prune", e.Cid())
						e.Ack(false)
						break
					}
					if clientBatch == nil {
						fr.log.Infof("Final : %d | Final Reduce batch is empty on Prune", e.Cid())
						e.Ack(false)
						break
					}
					fr.log.Debugf("Final : %d | clientBatch before: %v", e.Cid(), clientBatch)
					output := fr.MapReduce.Output(*clientBatch)
					for i, o := range output {
						fr.log.Infof("Final : %d | Sending partial result to output: %v", e.Cid(), o)
						err = fr.Sender.Send(o, e.Cid(), uint64(i))
					}
					if err != nil {
						e.Nack(false)
						err = fmt.Errorf("error sending partial result: %w", err)
						_ = err
						return
					}
					err := fr.Sender.Prune(e.Cid())
					if err != nil {
						fr.log.Errorf("input : %d | Prune failed: %v", e.Cid(), err)
						e.Nack(false)
					}

					delete(fr.ReduceBatches, e.Cid())

					fr.log.Infof("Final : %d | Sending EOF after sending partial result", e.Cid())
					err = fr.Sender.SendEOF(e.Cid())
					if err != nil {
						fr.log.Errorf("Final : %d | SendEOF failed: %s", e.Cid(), err)
						e.Nack(false)
						return
					}
					e.Ack(false)
				case middleware.Prune:
					fr.log.Infof("Final : %d | Received PRUNE", e.Cid())
					e.Ack(false)
				}
			}
		}
	}
	go task()
	return res
}
