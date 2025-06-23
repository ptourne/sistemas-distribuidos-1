package map_reducer

import (
	"bytes"
	"context"
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq/transaction_log"
)

type PartialReducer[I codec.Serializable[I], A codec.Serializable[A], R codec.Serializable[R]] struct {
	log                              *logger.ConsoleLogger
	MapReduce                        MapReduce[I, A, R]
	Receiver                         middleware.Receiver[I]
	ReduceBatches                    map[uint64]*A
	Sender                           middleware.Sender[A]
	connIn                           middleware.Connection[I]
	transactionLog                   transaction_log.TransactionLog
	pendingPrune                     *uint64
	pendingEOF                       *uint64
	msgsSinceLastDump                uint64
	maxLogSize                       uint64
	lastNormalMsgIdsByCidAndSenderId map[uint64]map[uint64]uint64 // Cid -> SenderId -> Last transaction id
}

func (r *PartialReducer[I, A, R]) Run(ctx context.Context) <-chan error {
	res := make(chan error)
	task := func() {
		var err error
		defer func() {
			res <- err
			close(res)
		}()
		r.log.Infof("input : Starting maper")
		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				r.log.Infof("input : Received SIGTERM. Shutting down gracefully...")
				return
			default:
				var e middleware.Envelope[I]
				e, err = r.Receiver.Next(ctx)
				if err != nil {
					if (err.Error() == middleware.TimeoutErr{}.Error()) {
						err = nil
						r.log.Infof("input : Received termination signal")
					}
					return
				}
				cid := e.Cid()
				if e.Type() == middleware.Normal {
					if r.isDuplicate(cid, e) {
						r.log.Debugf("input : %d | Duplicate message received:\nid: %d\nMsg:\n%+v\n, ignoring", cid, e.Id(), e.Msg())
						e.Ack(false)
						continue
					}
				} else if e.Type() == middleware.EOF {
					delete(r.lastNormalMsgIdsByCidAndSenderId, cid)
				}
				switch e.Type() {
				case middleware.Normal:
					msg := e.Msg()
					err = r.reduce(e.Cid(), msg)
					if err != nil {
						err = fmt.Errorf("input : %d | Error processing message: %w", e.Cid(), err)
						e.Nack(false)
						return
					}
					var msgBody []byte
					msgBody, err = msg.Encode()
					if err != nil {
						err = fmt.Errorf("input : %d | Error encoding message: %w", e.Cid(), err)
						e.Nack(false)
						return
					}
					err = r.transactionLog.Received(e.Cid(), e.Id(), msgBody)
					if err != nil {
						err = fmt.Errorf("input : %d | Error logging transaction: %w", e.Cid(), err)
						e.Nack(false)
						return
					}
					e.Ack(false)
					err = r.transactionLog.Acknowledged()
					if err != nil {
						err = fmt.Errorf("input : %d | Error acknowledging transaction: %w", e.Cid(), err)
						r.log.Errorf("Input : %d | Error acknowledging transaction: %s", e.Cid(), err)
						return
					}
				case middleware.EOF:
					r.log.Debugf("input : %d | Received EOF", e.Cid())
					err = r.Sender.SendEOF(e.Cid())
					if err != nil {
						err = fmt.Errorf("input : %d | Error sending EOF: %w", e.Cid(), err)
						e.Nack(false)
						return
					}
					err = r.transactionLog.ReceivedEOF(e.Cid())
					if err != nil {
						err = fmt.Errorf("input : %d | Error logging EOF transaction: %w", e.Cid(), err)
						e.Nack(false)
						return
					}
					e.Ack(false)
					err = r.transactionLog.Acknowledged()
					if err != nil {
						err = fmt.Errorf("input : %d | Error acknowledging transaction: %w", e.Cid(), err)
						r.log.Errorf("Input : %d | Error acknowledging transaction: %s", e.Cid(), err)
						return
					}
				case middleware.Prune:
					r.log.Debugf("input : %d | Received Prune", e.Cid())
					err = r.processPrune(e.Cid())
					if err != nil {
						err = fmt.Errorf("input : %d | Prune failed: %s", e.Cid(), err)
						e.Nack(false)
						return
					}
					err = r.transactionLog.ReceivedPrune(e.Cid())
					if err != nil {
						err = fmt.Errorf("input : %d | Error logging prune transaction: %w", e.Cid(), err)
						e.Nack(false)
						return
					}
					e.Ack(false)
					err = r.transactionLog.Acknowledged()
					if err != nil {
						err = fmt.Errorf("input : %d | Error acknowledging transaction: %w", e.Cid(), err)
						r.log.Errorf("Input : %d | Error acknowledging transaction: %s", e.Cid(), err)
						return
					}
				}
				r.msgsSinceLastDump++
				r.log.Debugf("input : %d | Processed message, total messages since last dump: %d. Max log size is %d", e.Cid(), r.msgsSinceLastDump, r.maxLogSize)
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

func (r *PartialReducer[I, A, R]) isDuplicate(cid uint64, e middleware.Envelope[I]) bool {
	lastNormalMsgIdsBySenderId, ok := r.lastNormalMsgIdsByCidAndSenderId[cid]
	if !ok {
		r.lastNormalMsgIdsByCidAndSenderId[cid] = make(map[uint64]uint64)
		r.lastNormalMsgIdsByCidAndSenderId[cid][e.SenderId()] = e.Id()
		return false
	}
	lastMsgId, ok := lastNormalMsgIdsBySenderId[e.SenderId()]

	if ok && lastMsgId >= e.Id() {
		return true
	}
	r.lastNormalMsgIdsByCidAndSenderId[cid][e.SenderId()] = e.Id()
	return false
}

func (r *PartialReducer[I, A, R]) DumpAndFlush() error {
	r.log.Debugf("input : DumpAndFlush | Dumping transaction log")
	data := r.Dump()
	err := r.transactionLog.Dump(data)
	if err != nil {
		r.log.Errorf("Input : Error dumping transaction log: %s", err)
		return fmt.Errorf("error dumping transaction log: %w", err)
	}
	r.log.Infof("Input : Transaction log dumped successfully")
	r.msgsSinceLastDump = 0
	return nil
}

func (r *PartialReducer[I, A, R]) processPrune(cid uint64) error {
	clientBatch, ok := r.ReduceBatches[cid]
	if ok {
		err := r.Sender.Send(*clientBatch, cid, 1)
		if err != nil {
			return fmt.Errorf("error sending partial result: %w", err)
		}
		delete(r.ReduceBatches, cid)
	}
	return nil
}

func (r *PartialReducer[I, A, R]) reduce(cid uint64, msg I) error {
	r.log.Debugf("input : %d | Received input", cid)
	acc := r.MapReduce.Map(msg)
	for _, a := range acc {
		err := r.reduceAndStore(cid, a)
		if err != nil {
			r.log.Errorf("input : %d | Error reducing and sending: %v", cid, err)
			return fmt.Errorf("error reducing and sending: %w", err)
		}
	}
	return nil
}

func (r *PartialReducer[I, A, R]) reduceAndStore(cid uint64, msg A) error {
	r.log.Infof("reduc : %d | reduceAndStore ", cid)
	clientBatch, ok := r.ReduceBatches[cid]
	if ok {
		r.log.Infof("reduc : %d | reduceAndStore partial", cid)
		reduc := []A{*clientBatch, msg}
		reduced := r.MapReduce.Reduce(reduc)
		r.ReduceBatches[cid] = &reduced
		return nil
	}

	r.log.Infof("reduc : %d | reduceAndStore | Creating new batch", cid)
	r.ReduceBatches[cid] = &msg
	return nil
}

func (r *PartialReducer[I, A, R]) Received(cid, id uint64, data []byte) error {
	var nul I
	msg, err := nul.Decode(data)
	if err != nil {
		r.log.Errorf("input : %d | Received error decoding message: %s", cid, err)
		return fmt.Errorf("error decoding message: %w", err)
	}
	r.log.Debugf("input : %d | Received message: %v", cid, msg)
	return r.reduce(cid, msg)
}

func (r *PartialReducer[I, A, R]) ReceivedPrune(cid uint64) error {
	r.pendingPrune = &cid
	return nil
}

func (r *PartialReducer[I, A, R]) ReceivedEOF(cid uint64) error {
	r.pendingEOF = &cid
	return nil
}

func (r *PartialReducer[I, A, R]) Acknowledged() error {
	r.pendingPrune = nil
	r.pendingEOF = nil
	return nil
}

func (r *PartialReducer[I, A, R]) Conclude() error {
	r.log.Debugf("input : Conclude | Finalizing PartialReducer")
	if r.pendingPrune != nil {
		err := r.processPrune(*r.pendingPrune)
		if err != nil {
			return fmt.Errorf("error processing pending prune: %w", err)
		}
		r.pendingPrune = nil
	}
	if r.pendingEOF != nil {
		err := r.Sender.SendEOF(*r.pendingEOF)
		if err != nil {
			return fmt.Errorf("error sending pending EOF: %w", err)
		}
		r.pendingEOF = nil
	}
	return nil
}

func (r *PartialReducer[I, A, R]) FromCheckpoint(data []byte) error {
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
			return fmt.Errorf("error decoding key: %w", err)
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

func (r *PartialReducer[I, A, R]) Dump() []byte {
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
