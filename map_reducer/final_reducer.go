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
					fr.reduce(e.Cid(), e.Msg())
					e.Ack(false)
				case middleware.EOF:
					err = fr.processEof(e.Cid())
					if err != nil {
						fr.log.Errorf("Final : %d | Error processing EOF: %s", e.Cid(), err)
						e.Nack(false)
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

func (fr *FinalReducer[I, A, R]) processEof(cid uint64) error {
	fr.log.Infof("Final EOF: %d | Pruning final reduce batch", cid)
	clientBatch, ok := fr.ReduceBatches[cid]
	if !ok {
		fr.log.Infof("Final : %d | Final Reduce batch not found on Prune", cid)
		return nil
	}
	if clientBatch == nil {
		fr.log.Infof("Final : %d | Final Reduce batch is empty on Prune", cid)
		return nil
	}
	fr.log.Debugf("Final : %d | clientBatch before: %v", cid, clientBatch)
	output := fr.MapReduce.Output(*clientBatch)
	for i, o := range output {
		fr.log.Infof("Final : %d | Sending partial result to output: %v", cid, o)
		err := fr.Sender.Send(o, cid, uint64(i))
		if err != nil {
			fr.log.Errorf("Final : %d | Error sending partial result: %s", cid, err)
			return fmt.Errorf("error sending partial result: %w", err)
		}
	}
	err := fr.Sender.Prune(cid)
	if err != nil {
		fr.log.Errorf("input : %d | Prune failed: %v", cid, err)
		return fmt.Errorf("error pruning cid %d: %w", cid, err)
	}

	delete(fr.ReduceBatches, cid)

	fr.log.Infof("Final : %d | Sending EOF after sending partial result", cid)
	err = fr.Sender.SendEOF(cid)
	if err != nil {
		fr.log.Errorf("Final : %d | SendEOF failed: %s", cid, err)
		return fmt.Errorf("error sending EOF: %w", err)
	}
	return nil
}

func (fr *FinalReducer[I, A, R]) reduce(cid uint64, msg A) {
	clientBatch, ok := fr.ReduceBatches[cid]
	if !ok {
		acc := msg
		fr.ReduceBatches[cid] = &acc
	} else {
		reduc := []A{*clientBatch, msg}
		reduced := fr.MapReduce.Reduce(reduc)
		fr.ReduceBatches[cid] = &reduced
	}
}

func (fr *FinalReducer[I, A, R]) Received(cid, id uint64, data []byte) error {
	var nul A
	msg, err := nul.Decode(data)
	if err != nil {
		fr.log.Errorf("input : %d | Received error decoding message: %s", cid, err)
		return fmt.Errorf("error decoding message: %w", err)
	}
	fr.log.Debugf("input : %d | Received message: %v", cid, msg)
	fr.reduce(cid, msg)
	return nil
}

func (fr *FinalReducer[I, A, R]) ReceivedPrune(cid uint64) error {
	fr.pendingPrune = &cid
	return nil
}

func (fr *FinalReducer[I, A, R]) ReceivedEOF(cid uint64) error {
	fr.pendingEOF = &cid
	return nil
}

func (fr *FinalReducer[I, A, R]) Acknowledged() error {
	fr.pendingPrune = nil
	fr.pendingEOF = nil
	return nil
}

func (fr *FinalReducer[I, A, R]) Conclude() error {
	fr.log.Debugf("input : Conclude | Finalizing FinalReducer")
	if fr.pendingPrune != nil {
		fr.pendingPrune = nil
	}
	if fr.pendingEOF != nil {
		err := fr.processEof(*fr.pendingEOF)
		if err != nil {
			return fmt.Errorf("error processing pending EOF: %w", err)
		}
		fr.pendingEOF = nil
	}
	return nil
}

func (fr *FinalReducer[I, A, R]) FromCheckpoint(data []byte) error {
	fr.log.Debugf("input : FromCheckpoint | Data: %s", data)
	if len(data) == 0 {
		fr.log.Debugf("input : FromCheckpoint | No data to restore")
		return nil
	}

	fr.ReduceBatches = make(map[uint64]*A)
	reader := bytes.NewReader(data)
	for {
		key, err := codec.Uint64Decode(reader)
		if err != nil {
			if err.Error() == "EOF" {
				fr.log.Debugf("input : FromCheckpoint | Reached end of data")
				break
			}
			fr.log.Errorf("input : FromCheckpoint | Error decoding key: %s", err)
			return fmt.Errorf("error decoding key: %w", err)
		}
		valueDataLen, err := codec.Uint64Decode(reader)
		if err != nil {
			fr.log.Errorf("input : FromCheckpoint | Error decoding value data len: %s", err)
			return fmt.Errorf("error decoding key: %w", err)
		}
		valueData, err := codec.DoRead(valueDataLen, reader)
		if err != nil {
			fr.log.Errorf("input : FromCheckpoint | Error reading value data: %s", err)
			return fmt.Errorf("error reading value data: %w", err)
		}
		var nul A
		value, err := nul.Decode(valueData)
		if err != nil {
			fr.log.Errorf("input : FromCheckpoint | Error decoding value: %s", err)
			return fmt.Errorf("error decoding value: %w", err)
		}
		fr.ReduceBatches[key] = &value
	}
	fr.log.Debugf("input : FromCheckpoint | ReduceBatches restored: %v", fr.ReduceBatches)
	return nil
}

func (fr *FinalReducer[I, A, R]) Dump() []byte {
	fr.log.Debugf("input : Dump | Dumping ReduceBatches")
	if len(fr.ReduceBatches) == 0 {
		fr.log.Debugf("input : Dump | No data to dump")
		return nil
	}

	var buf bytes.Buffer
	for key, value := range fr.ReduceBatches {
		keyData, err := codec.Uint64Encode(key)
		if err != nil {
			fr.log.Errorf("input : Dump | Error encoding key: %s", err)
			continue
		}
		valueData, err := (*value).Encode()
		if err != nil {
			fr.log.Errorf("input : Dump | Error encoding value: %s", err)
			continue
		}
		valueDataLen, err := codec.Uint64Encode(uint64(len(valueData)))
		if err != nil {
			fr.log.Errorf("input : Dump | Error encoding value data length: %s", err)
			continue
		}
		buf.Write(keyData)
		buf.Write(valueDataLen)
		buf.Write(valueData)
	}
	return buf.Bytes()
}
