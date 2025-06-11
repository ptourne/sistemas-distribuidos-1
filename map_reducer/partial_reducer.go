package map_reducer

import (
	"context"
	"fmt"
	"strings"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq/transaction_log"
)

type PartialReducer[I codec.Serializable[I], A codec.Serializable[A], R codec.Serializable[R]] struct {
	log            *logger.ConsoleLogger
	MapReduce      MapReduce[I, A, R]
	Receiver       middleware.Receiver[I]
	ReduceBatches  map[string]*A
	Sender         middleware.Sender[A]
	connIn         middleware.Connection[I]
	transactionLog transaction_log.TransactionLog
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
						err = fmt.Errorf("input : %s | Error processing message: %w", envelope.Cid(), err)
						envelope.Nack(false)
					}

					envelope.Ack(false)
				case middleware.EOF:
					pr.log.Debugf("input : %s | Received EOF", envelope.Cid())
					err = pr.Sender.SendEOF(envelope.Cid())
					envelope.Ack(false)
				case middleware.Prune:
					pr.log.Debugf("input : %s | Received Prune", envelope.Cid())
					clientBatch, ok := pr.ReduceBatches[envelope.Cid()]
					if ok {
						err := pr.Sender.Send(*clientBatch, envelope.Cid(), 0) // TODO id!!
						if err != nil {
							err = fmt.Errorf("error sending partial result: %w", err)
							return
						}
					}
					err = pr.Sender.Prune(envelope.Cid())
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

func (pr *PartialReducer[I, A, R]) reduce(cid uint64, msg I) error {
	pr.log.Debugf("input : %s | Received input", cid)
	acc := pr.MapReduce.Map(msg)
	for _, a := range acc {
		err := pr.reduceAndStore(cid, a)
		if err != nil {
			pr.log.Errorf("input : %s | Error reducing and sending: %s", cid, err)
			return fmt.Errorf("error reducing and sending: %w", err)
		}
	}
	return nil
}

func (pr *PartialReducer[I, A, R]) reduceAndStore(cid string, msg A) error {
	pr.log.Infof("reduc : %s | reduceAndStore ", cid)
	clientBatch, ok := pr.ReduceBatches[cid]
	if ok {
		pr.log.Infof("reduc : %s | reduceAndStore partial", cid)
		reduc := []A{*clientBatch, msg}
		reduced := pr.MapReduce.Reduce(reduc)
		pr.ReduceBatches[cid] = &reduced
		return nil
	}

	pr.log.Infof("reduc : %s | reduceAndStore | Creating new batch", cid)
	pr.ReduceBatches[cid] = &msg
	return nil
}

func (pr *PartialReducer[I, A, R]) Received(cid, id uint64, data []byte) error {
	var nul I
	msg, err := nul.Decode(data)
	if err != nil {
		pr.log.Errorf("input : %s | Received error decoding message: %s", cid, err)
		return fmt.Errorf("error decoding message: %w", err)
	}
	pr.log.Debugf("input : %s | Received message: %v", cid, msg)
	return pr.reduce(cid, msg)
}

func (pr *PartialReducer[I, A, R]) ReceivedEOF(cid uint64) error {

}

func (pr *PartialReducer[I, A, R]) Acknowledged() error {

}

func (pr *PartialReducer[I, A, R]) FromCheckpoint(data []byte) error {
	pr.log.Debugf("input : FromCheckpoint | Data: %s", data)
	if len(data) == 0 {
		pr.log.Debugf("input : FromCheckpoint | No data to restore")
		return nil
	}

	pr.ReduceBatches = make(map[string]*A)
	entries := string(data)
	for {
		parts := strings.SplitN(entries, ":", 2)
		if len(parts) != 2 {
			pr.log.Errorf("input : FromCheckpoint | Invalid entry format: %s", entries)
			return fmt.Errorf("invalid entry format: %s", entries)
		}
		key := parts[0]
		var nul A
		parts2 := strings.SplitN(parts[1], ";", 2)
		value, err := nul.Decode([]byte(parts2[0]))
		if err != nil {
			pr.log.Errorf("input : FromCheckpoint | Error decoding value: %s", err)
			return fmt.Errorf("error decoding value: %w", err)
		}
		pr.ReduceBatches[key] = &value
		entries = parts2[1]
	}
	pr.log.Debugf("input : FromCheckpoint | ReduceBatches restored: %v", pr.ReduceBatches)
	return nil
}

func (pr *PartialReducer[I, A, R]) Dump() []byte {
	buf := []byte{}
	for key, value := range pr.ReduceBatches {
		data, err := (*value).Encode()
		if err != nil {
			pr.log.Errorf("input : %s | Error encoding value: %s", key, err)
			panic(fmt.Sprintf("error encoding value: %s", err))
		}
		buf = append(buf, []byte(fmt.Sprintf("%s:%s;", key, data))...)
	}
	pr.log.Debugf("input : Dumping ReduceBatches: %s", buf)
	return buf
}
