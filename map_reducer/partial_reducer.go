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
	log            *logger.ConsoleLogger
	MapReduce      MapReduce[I, A, R]
	Receiver       middleware.Receiver[I]
	ReduceBatches  map[uint64]*A
	Sender         middleware.Sender[A]
	connIn         middleware.Connection[I]
	transactionLog transaction_log.TransactionLog
	pendingPrune   *uint64
	pendingEOF     *uint64
}

func (pr *PartialReducer[I, A, R]) Run(ctx context.Context) <-chan error {
	res := make(chan error)
	task := func() {
		var err error
		defer func() {
			res <- err
			close(res)
		}()
		pr.log.Infof("input : Starting maper")
		for {
			select {
			case <-ctx.Done():
				err = ctx.Err()
				pr.log.Infof("input : Received SIGTERM. Shutting down gracefully...")
				return
			default:
				var envelope middleware.Envelope[I]
				envelope, err = pr.Receiver.Next(ctx)
				if err != nil {
					if (err.Error() == middleware.TimeoutErr{}.Error()) {
						err = nil
						pr.log.Infof("input : Received termination signal")
					}
					return
				}
				switch envelope.Type() {
				case middleware.Normal:
					msg := envelope.Msg()
					err := pr.reduce(envelope.Cid(), msg)
					if err != nil {
						err = fmt.Errorf("input : %d | Error processing message: %w", envelope.Cid(), err)
						envelope.Nack(false)
					}
					envelope.Ack(false)
				case middleware.EOF:
					pr.log.Debugf("input : %d | Received EOF", envelope.Cid())
					err = pr.Sender.SendEOF(envelope.Cid())
					envelope.Ack(false)
				case middleware.Prune:
					pr.log.Debugf("input : %d | Received Prune", envelope.Cid())
					err = pr.processPrune(envelope.Cid())
					if err != nil {
						err = fmt.Errorf("input : %d | Prune failed: %s", envelope.Cid(), err)
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

func (pr *PartialReducer[I, A, R]) processPrune(cid uint64) error {
	clientBatch, ok := pr.ReduceBatches[cid]
	if ok {
		err := pr.Sender.Send(*clientBatch, cid, 0)
		if err != nil {
			err = fmt.Errorf("error sending partial result: %w", err)
			return nil
		}
	}
	return pr.Sender.Prune(cid)
}

func (pr *PartialReducer[I, A, R]) reduce(cid uint64, msg I) error {
	pr.log.Debugf("input : %d | Received input", cid)
	acc := pr.MapReduce.Map(msg)
	for _, a := range acc {
		err := pr.reduceAndStore(cid, a)
		if err != nil {
			pr.log.Errorf("input : %d | Error reducing and sending: %v", cid, err)
			return fmt.Errorf("error reducing and sending: %w", err)
		}
	}
	return nil
}

func (pr *PartialReducer[I, A, R]) reduceAndStore(cid uint64, msg A) error {
	pr.log.Infof("reduc : %d | reduceAndStore ", cid)
	clientBatch, ok := pr.ReduceBatches[cid]
	if ok {
		pr.log.Infof("reduc : %d | reduceAndStore partial", cid)
		reduc := []A{*clientBatch, msg}
		reduced := pr.MapReduce.Reduce(reduc)
		pr.ReduceBatches[cid] = &reduced
		return nil
	}

	pr.log.Infof("reduc : %d | reduceAndStore | Creating new batch", cid)
	pr.ReduceBatches[cid] = &msg
	return nil
}

func (pr *PartialReducer[I, A, R]) Received(cid, id uint64, data []byte) error {
	var nul I
	msg, err := nul.Decode(data)
	if err != nil {
		pr.log.Errorf("input : %d | Received error decoding message: %s", cid, err)
		return fmt.Errorf("error decoding message: %w", err)
	}
	pr.log.Debugf("input : %d | Received message: %v", cid, msg)
	return pr.reduce(cid, msg)
}

func (pr *PartialReducer[I, A, R]) ReceivedPrune(cid uint64) error {
	pr.pendingPrune = &cid
	return nil
}

func (pr *PartialReducer[I, A, R]) ReceivedEOF(cid uint64) error {
	pr.pendingEOF = &cid
	return nil
}

func (pr *PartialReducer[I, A, R]) Acknowledged() error {
	pr.pendingPrune = nil
	pr.pendingEOF = nil
	return nil
}

func (pr *PartialReducer[I, A, R]) Conclude() error {
	pr.log.Debugf("input : Conclude | Finalizing PartialReducer")
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

func (pr *PartialReducer[I, A, R]) FromCheckpoint(data []byte) error {
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

func (pr *PartialReducer[I, A, R]) Dump() []byte {
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
