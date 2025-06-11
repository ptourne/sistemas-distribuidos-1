package map_reducer

import (
	"context"
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq/transaction_log"
)

// Received(cid, id uint64, data []byte) error
// ReceivedEOF(cid uint64) error
// Acknowledged() error
// FromCheckpoint(data []byte) error
// Dump() []byte

type PartialReducer[I codec.Serializable[I], A codec.Serializable[A], R codec.Serializable[R]] struct {
	log               *logger.ConsoleLogger
	MapReduce         MapReduce[I, A, R]
	Input             middleware.Receiver[I]
	PartReduceBatches map[string]*A
	FinalReduceSender middleware.Sender[A]
	connIn            middleware.Connection[I]
	transactionLog    transaction_log.TransactionLog
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
				envelope, err = pr.Input.Next(ctx)
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
					pr.log.Debugf("input : %s | Received input", envelope.Cid())
					acc := pr.MapReduce.Map(msg)
					for _, a := range acc {
						pr.ReduceAndSend(envelope.Cid(), a)
					}

					envelope.Ack(false)
				case middleware.EOF:
					pr.log.Debugf("input : %s | Received EOF", envelope.Cid())
					err = pr.FinalReduceSender.SendEOF(envelope.Cid())
					envelope.Ack(false)
				case middleware.Prune:
					pr.log.Debugf("input : %s | Received Prune", envelope.Cid())
					clientBatch, ok := pr.PartReduceBatches[envelope.Cid()]
					if ok {
						err := pr.FinalReduceSender.Send(*clientBatch, envelope.Cid(), 0) // TODO id!!
						if err != nil {
							err = fmt.Errorf("error sending partial result: %w", err)
							return
						}
					}
					err = pr.FinalReduceSender.Prune(envelope.Cid())
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

func (pr *PartialReducer[I, A, R]) ReduceAndSend(cid string, msg A) error {
	pr.log.Infof("reduc : %s | ReduceAndSend ", cid)
	clientBatch, ok := pr.PartReduceBatches[cid]
	if ok {
		pr.log.Infof("reduc : %s | ReduceAndSend partial", cid)
		reduc := []A{*clientBatch, msg}
		reduced := pr.MapReduce.Reduce(reduc)
		pr.PartReduceBatches[cid] = &reduced
		return nil
	}

	pr.log.Infof("reduc : %s | ReduceAndSend | Creating new batch", cid)
	pr.PartReduceBatches[cid] = &msg
	return nil
}
