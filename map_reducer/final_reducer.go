package map_reducer

import (
	"bytes"
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
	pendingPrune   *uint64
	pendingEOF     *uint64
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

func (pr *FinalReducer[I, A, R]) Received(cid, id uint64, data []byte) error {
	var nul I
	msg, err := nul.Decode(data)
	if err != nil {
		pr.log.Errorf("input : %d | Received error decoding message: %s", cid, err)
		return fmt.Errorf("error decoding message: %w", err)
	}
	pr.log.Debugf("input : %d | Received message: %v", cid, msg)
	return pr.reduce(cid, msg)
}

func (pr *FinalReducer[I, A, R]) ReceivedPrune(cid uint64) error {
	pr.pendingPrune = &cid
	return nil
}

func (pr *FinalReducer[I, A, R]) ReceivedEOF(cid uint64) error {
	pr.pendingEOF = &cid
	return nil
}

func (pr *FinalReducer[I, A, R]) Acknowledged() error {
	pr.pendingPrune = nil
	pr.pendingEOF = nil
	return nil
}

func (pr *FinalReducer[I, A, R]) Conclude() error {
	pr.log.Debugf("input : Conclude | Finalizing FinalReducer")
	if pr.pendingPrune != nil {
		err := pr.processPrune(*pr.pendingPrune)
		if err != nil {
			return fmt.Errorf("error processing pending prune: %w", err)
		}
		pr.pendingPrune = nil
	}
	if pr.pendingEOF != nil {
		err := pr.Sender.SendEOF(*pr.pendingEOF)
		if err != nil {
			return fmt.Errorf("error sending pending EOF: %w", err)
		}
		pr.pendingEOF = nil
	}
	return nil
}

func (pr *FinalReducer[I, A, R]) FromCheckpoint(data []byte) error {
	pr.log.Debugf("input : FromCheckpoint | Data: %s", data)
	if len(data) == 0 {
		pr.log.Debugf("input : FromCheckpoint | No data to restore")
		return nil
	}

	pr.ReduceBatches = make(map[uint64]*A)
	reader := bytes.NewReader(data)
	for {
		key, err := codec.Uint64Decode(reader)
		if err != nil {
			if err.Error() == "EOF" {
				pr.log.Debugf("input : FromCheckpoint | Reached end of data")
				break
			}
			pr.log.Errorf("input : FromCheckpoint | Error decoding key: %s", err)
			return fmt.Errorf("error decoding key: %w", err)
		}
		valueDataLen, err := codec.Uint64Decode(reader)
		if err != nil {
			pr.log.Errorf("input : FromCheckpoint | Error decoding value data len: %s", err)
			return fmt.Errorf("error decoding key: %w", err)
		}
		valueData, err := codec.DoRead(valueDataLen, reader)
		if err != nil {
			pr.log.Errorf("input : FromCheckpoint | Error reading value data: %s", err)
			return fmt.Errorf("error reading value data: %w", err)
		}
		var nul A
		value, err := nul.Decode(valueData)
		if err != nil {
			pr.log.Errorf("input : FromCheckpoint | Error decoding value: %s", err)
			return fmt.Errorf("error decoding value: %w", err)
		}
		pr.ReduceBatches[key] = &value
	}
	pr.log.Debugf("input : FromCheckpoint | ReduceBatches restored: %v", pr.ReduceBatches)
	return nil
}

func (pr *FinalReducer[I, A, R]) Dump() []byte {
	pr.log.Debugf("input : Dump | Dumping ReduceBatches")
	if len(pr.ReduceBatches) == 0 {
		pr.log.Debugf("input : Dump | No data to dump")
		return nil
	}

	var buf bytes.Buffer
	for key, value := range pr.ReduceBatches {
		keyData, err := codec.Uint64Encode(key)
		if err != nil {
			pr.log.Errorf("input : Dump | Error encoding key: %s", err)
			continue
		}
		valueData, err := (*value).Encode()
		if err != nil {
			pr.log.Errorf("input : Dump | Error encoding value: %s", err)
			continue
		}
		valueDataLen, err := codec.Uint64Encode(uint64(len(valueData)))
		if err != nil {
			pr.log.Errorf("input : Dump | Error encoding value data length: %s", err)
			continue
		}
		buf.Write(keyData)
		buf.Write(valueDataLen)
		buf.Write(valueData)
	}
	return buf.Bytes()
}
