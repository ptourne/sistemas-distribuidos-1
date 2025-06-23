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
	log               *logger.ConsoleLogger
	MapReduce         MapReduce[I, A, R]
	ReduceBatches     map[uint64]*A
	connOut           middleware.Connection[R]
	Receiver          map[string]middleware.Receiver[A] // It will be null for all but the leader
	Sender            middleware.Sender[R]
	transactionLog    transaction_log.TransactionLog
	pendingPrune      *uint64
	pendingEOF        *uint64
	msgsSinceLastDump uint64
	maxLogSize        uint64
}

type NextAsyncRes[T codec.Serializable[T]] struct {
	received middleware.Envelope[T]
	err      error
}

func (r *FinalReducer[I, A, R]) NewIterator(ctx context.Context, log *logger.ConsoleLogger) *Iterator[A] {
	readCtx, cancelNexts := context.WithCancel(ctx)
	cases := make([]reflect.SelectCase, len(r.Receiver))
	j := 0
	lastNormalMsgIdsByReducerAndCid := make(map[int]map[uint64]uint64)
	for _, receiver := range r.Receiver {
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
		lastNormalMsgIdsByReducerAndCid[j] = make(map[uint64]uint64)
		j++
	}
	return &Iterator[A]{
		cases,
		cancelNexts,
		lastNormalMsgIdsByReducerAndCid,
		log,
	}
}

type Iterator[T codec.Serializable[T]] struct {
	cases                           []reflect.SelectCase
	cancelNexts                     context.CancelFunc
	lastNormalMsgIdsByReducerAndCid map[int]map[uint64]uint64 // Reducer id -> Cid -> Last transaction id
	log                             *logger.ConsoleLogger
}

func (it *Iterator[T]) Next(ctx context.Context) (reducerId int, envelope middleware.Envelope[T], err error) {
	i, value, ok := reflect.Select(it.cases)
	if !ok {
		panic("channel closed")
	}
	asyncRes := value.Interface().(NextAsyncRes[T])
	return i, asyncRes.received, asyncRes.err
}

func (it *Iterator[T]) NextFiltered(ctx context.Context) (reducerId int, e middleware.Envelope[T], err error) {
	for {
		reducerId, e, err = it.Next(ctx)
		if err != nil {
			if e == nil {
				it.log.Errorf("Final : %d | Envelope nil: %s", reducerId, err)
				return reducerId, nil, fmt.Errorf("envelope nil: %w", err)
			}
			cid := e.Cid()
			if e.Type() == middleware.Normal {
				lastMsgId, ok := it.lastNormalMsgIdsByReducerAndCid[reducerId][cid]
				id := e.Id()
				isDuplicate := ok && lastMsgId >= id
				if isDuplicate {
					it.log.Debugf("Final : %d | Duplicate message received, ignoring", cid)
					e.Ack(false)
					continue
				}
				it.lastNormalMsgIdsByReducerAndCid[reducerId][cid] = id
			} else if e.Type() == middleware.EOF {
				delete(it.lastNormalMsgIdsByReducerAndCid[reducerId], cid)
			}
		}
		return reducerId, e, err
	}

}

func (r *FinalReducer[I, A, R]) Run(ctx context.Context) chan error {
	res := make(chan error)
	task := func() {
		var err error
		defer func() {
			res <- err
			close(res)
		}()
		if len(r.Receiver) == 0 {
			r.log.Debugf("Final : Worker is not the master, skipping final reduce")
			return
		}
		iterator := r.NewIterator(ctx, r.log)
		defer iterator.cancelNexts()

		r.log.Infof("Final : Starting final reducer")

		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				r.log.Infof("final : Received SIGTERM. Shutting down gracefully...")
				return
			default:
				_, e, err := iterator.Next(ctx) // TODO should use NextFiltered
				if err != nil {
					if (err.Error() == middleware.TimeoutErr{}.Error()) {
						err = nil
						r.log.Infof("Final : Received termination signal")
					}
					return
				}
				switch e.Type() {
				case middleware.Normal:
					r.log.Infof("Final : %d | Saving final reduce batch", e.Cid())
					r.reduce(e.Cid(), e.Msg())
					var msgBody []byte
					msgBody, err = e.Msg().Encode()
					if err != nil {
						err = fmt.Errorf("final : %d | Error encoding message: %w", e.Cid(), err)
						r.log.Errorf("Final : %d | Error encoding message: %s", e.Cid(), err)
						e.Nack(false)
						return
					}
					err = r.transactionLog.Received(e.Cid(), e.Id(), msgBody)
					if err != nil {
						err = fmt.Errorf("final : %d | Error logging transaction: %w", e.Cid(), err)
						r.log.Errorf("Final : %d | Error logging transaction: %s", e.Cid(), err)
						e.Nack(false)
						return
					}
					e.Ack(false)
					err = r.transactionLog.Acknowledged()
					if err != nil {
						err = fmt.Errorf("final : %d | Error acknowledging transaction: %w", e.Cid(), err)
						r.log.Errorf("Final : %d | Error acknowledging transaction: %s", e.Cid(), err)
						e.Nack(false)
						return
					}
				case middleware.EOF:
					err = r.processEof(e.Cid())
					if err != nil {
						err = fmt.Errorf("final : %d | Error processing EOF: %w", e.Cid(), err)
						r.log.Errorf("Final : %d | Error processing EOF: %s", e.Cid(), err)
						e.Nack(false)
						return
					}
					r.log.Infof("Final : %d | Received EOF", e.Cid())

					e.Ack(false)
					err = r.transactionLog.Acknowledged()
					if err != nil {
						err = fmt.Errorf("final : %d | Error acknowledging transaction: %w", e.Cid(), err)
						r.log.Errorf("Final : %d | Error acknowledging transaction: %s", e.Cid(), err)
						return
					}
				case middleware.Prune:
					r.log.Infof("Final : %d | Received PRUNE", e.Cid())
					err = r.transactionLog.ReceivedPrune(e.Cid())
					if err != nil {
						err = fmt.Errorf("final : %d | Error logging prune transaction: %w", e.Cid(), err)
						r.log.Errorf("Final : %d | Error logging prune transaction: %s", e.Cid(), err)
						return
					}
					e.Ack(false)
					err = r.transactionLog.Acknowledged()
					if err != nil {
						err = fmt.Errorf("final : %d | Error acknowledging transaction: %w", e.Cid(), err)
						r.log.Errorf("Final : %d | Error acknowledging transaction: %s", e.Cid(), err)
						return
					}
				}
				r.msgsSinceLastDump++
				if r.msgsSinceLastDump >= r.maxLogSize || e.Type() == middleware.EOF {
					err = r.DumpAndFlush()
					if err != nil {
						r.log.Errorf("Final : Error dumping transaction log: %s", err)
						err = fmt.Errorf("error dumping transaction log: %w", err)
						return
					}
				}
			}
		}
	}
	go task()
	return res
}

func (r *FinalReducer[I, A, R]) DumpAndFlush() error {
	data := r.Dump()
	err := r.transactionLog.Dump(data)
	if err != nil {
		r.log.Errorf("Final : Error dumping transaction log: %s", err)
		return fmt.Errorf("error dumping transaction log: %w", err)
	}
	r.log.Infof("Final : Transaction log dumped successfully")
	r.msgsSinceLastDump = 0
	return nil
}

func (r *FinalReducer[I, A, R]) processEof(cid uint64) error {
	r.log.Infof("Final EOF: %d | Pruning final reduce batch", cid)
	clientBatch, ok := r.ReduceBatches[cid]
	if !ok {
		r.log.Infof("Final : %d | Final Reduce batch not found on Prune", cid)
		return nil
	}
	if clientBatch == nil {
		r.log.Infof("Final : %d | Final Reduce batch is empty on Prune", cid)
		return nil
	}
	r.log.Debugf("Final : %d | clientBatch before: %v", cid, clientBatch)
	output := r.MapReduce.Output(*clientBatch)
	for i, o := range output {
		r.log.Infof("Final : %d | Sending partial result to output: %v", cid, o)
		err := r.Sender.Send(o, cid, uint64(i))
		if err != nil {
			r.log.Errorf("Final : %d | Error sending partial result: %s", cid, err)
			return fmt.Errorf("error sending partial result: %w", err)
		}
	}
	err := r.Sender.Prune(cid)
	if err != nil {
		r.log.Errorf("input : %d | Prune failed: %v", cid, err)
		return fmt.Errorf("error pruning cid %d: %w", cid, err)
	}

	delete(r.ReduceBatches, cid)

	r.log.Infof("Final : %d | Sending EOF after sending partial result", cid)
	err = r.Sender.SendEOF(cid)
	if err != nil {
		r.log.Errorf("Final : %d | SendEOF failed: %s", cid, err)
		return fmt.Errorf("error sending EOF: %w", err)
	}
	return nil
}

func (r *FinalReducer[I, A, R]) reduce(cid uint64, msg A) {
	clientBatch, ok := r.ReduceBatches[cid]
	if !ok {
		acc := msg
		r.ReduceBatches[cid] = &acc
	} else {
		reduc := []A{*clientBatch, msg}
		reduced := r.MapReduce.Reduce(reduc)
		r.ReduceBatches[cid] = &reduced
	}
}

func (r *FinalReducer[I, A, R]) Received(cid, id uint64, data []byte) error {
	var nul A
	msg, err := nul.Decode(data)
	if err != nil {
		r.log.Errorf("input : %d | Received error decoding message: %s", cid, err)
		return fmt.Errorf("error decoding message: %w", err)
	}
	r.log.Debugf("input : %d | Received message: %v", cid, msg)
	r.reduce(cid, msg)
	return nil
}

func (r *FinalReducer[I, A, R]) ReceivedPrune(cid uint64) error {
	r.pendingPrune = &cid
	return nil
}

func (r *FinalReducer[I, A, R]) ReceivedEOF(cid uint64) error {
	r.pendingEOF = &cid
	return nil
}

func (r *FinalReducer[I, A, R]) Acknowledged() error {
	r.pendingPrune = nil
	r.pendingEOF = nil
	return nil
}

func (r *FinalReducer[I, A, R]) Conclude() error {
	r.log.Debugf("input : Conclude | Finalizing FinalReducer")
	if r.pendingPrune != nil {
		r.pendingPrune = nil
	}
	if r.pendingEOF != nil {
		err := r.processEof(*r.pendingEOF)
		if err != nil {
			return fmt.Errorf("error processing pending EOF: %w", err)
		}
		r.pendingEOF = nil
	}
	return nil
}

func (r *FinalReducer[I, A, R]) FromCheckpoint(data []byte) error {
	r.log.Debugf("input : FromCheckpoint | Data: %s", data)
	if len(data) == 0 {
		r.log.Debugf("input : FromCheckpoint | No data to restore")
		return nil
	}

	r.ReduceBatches = make(map[uint64]*A)
	reader := bytes.NewReader(data)
	for {
		key, err := codec.Uint64Decode(reader)
		if err != nil {
			if err.Error() == "EOF" {
				r.log.Debugf("input : FromCheckpoint | Reached end of data")
				break
			}
			r.log.Errorf("input : FromCheckpoint | Error decoding key: %s", err)
			return fmt.Errorf("error decoding key: %w", err)
		}
		valueDataLen, err := codec.Uint64Decode(reader)
		if err != nil {
			r.log.Errorf("input : FromCheckpoint | Error decoding value data len: %s", err)
			return fmt.Errorf("error decoding value len: %w", err)
		}
		valueData, err := codec.DoRead(valueDataLen, reader)
		if err != nil {
			r.log.Errorf("input : FromCheckpoint | Error reading value data: %s", err)
			return fmt.Errorf("error reading value data: %w", err)
		}
		var nul A
		value, err := nul.Decode(valueData)
		if err != nil {
			r.log.Errorf("input : FromCheckpoint | Error decoding value: %s", err)
			return fmt.Errorf("error decoding value: %w", err)
		}
		r.ReduceBatches[key] = &value
	}
	r.log.Debugf("input : FromCheckpoint | ReduceBatches restored: %v", r.ReduceBatches)
	return nil
}

func (r *FinalReducer[I, A, R]) Dump() []byte {
	r.log.Debugf("input : Dump | Dumping ReduceBatches")
	if len(r.ReduceBatches) == 0 {
		r.log.Debugf("input : Dump | No data to dump")
		return nil
	}

	var buf bytes.Buffer
	for key, value := range r.ReduceBatches {
		keyData, err := codec.Uint64Encode(key)
		if err != nil {
			r.log.Errorf("input : Dump | Error encoding key: %s", err)
			continue
		}
		valueData, err := (*value).Encode()
		if err != nil {
			r.log.Errorf("input : Dump | Error encoding value: %s", err)
			continue
		}
		valueDataLen, err := codec.Uint64Encode(uint64(len(valueData)))
		if err != nil {
			r.log.Errorf("input : Dump | Error encoding value data length: %s", err)
			continue
		}
		buf.Write(keyData)
		buf.Write(valueDataLen)
		buf.Write(valueData)
	}
	return buf.Bytes()
}
