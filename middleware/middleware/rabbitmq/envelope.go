package rabbitmq

import (
	"context"
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log2 = logger.NewConsoleLogger("envelope", logger.Info)

func newNormalEnvelope[T codec.Serializable[T]](cid string, msgbody T, tag *amqp.Delivery, id uint64) middleware.Envelope[T] {
	return &EnvelopeRabbitmq[T]{
		msg: msgbody,
		tag: tag,
		cid: cid,
		id:  id,
	}
}

type EnvelopeRabbitmq[T codec.Serializable[T]] struct {
	msg T
	tag *amqp.Delivery
	cid string
	id  uint64
}

func (r *EnvelopeRabbitmq[T]) Msg() T {
	return r.msg
}

func (r *EnvelopeRabbitmq[T]) Cid() string {
	return r.cid
}

func (r *EnvelopeRabbitmq[T]) Type() middleware.TypeMsg {
	return middleware.Normal
}

func (r *EnvelopeRabbitmq[T]) Id() uint64 {
	return r.id
}

func (r *EnvelopeRabbitmq[T]) Ack(multiple bool) error {
	if r.tag == nil {
		return fmt.Errorf("tag is not initialized or already acked")
	}
	err := r.tag.Ack(false)
	if err != nil {
		return fmt.Errorf("failed to ack message: %v", err)
	}
	r.tag = nil
	return nil
}

func (r *EnvelopeRabbitmq[T]) Nack(multiple bool) error {
	if r.tag == nil {
		return fmt.Errorf("tag is not initialized or already acked")
	}
	err := r.tag.Nack(false, true)
	if err != nil {
		return fmt.Errorf("failed to ack message: %v", err)
	}
	r.tag = nil
	return nil
}

type eofEnvelopeRabbitmq[T codec.Serializable[T]] struct {
	cid           string
	finishDoneIds map[string]*amqp.Delivery
	msgEOF        *amqp.Delivery
}

func newEOFEnvelope[T codec.Serializable[T]](
	cid string,
	msgEof *amqp.Delivery,
	finishDoneIds map[string]*amqp.Delivery,
) middleware.Envelope[T] {
	return &eofEnvelopeRabbitmq[T]{
		cid:           cid,
		finishDoneIds: finishDoneIds,
		msgEOF:        msgEof,
	}
}

func NewEOFEnvelope[T codec.Serializable[T]](
	cid string,
	msgEof *amqp.Delivery,
	finishDoneIds map[string]*amqp.Delivery,
) middleware.Envelope[T] {
	return newEOFEnvelope[T](cid, msgEof, finishDoneIds)
}

func (r *eofEnvelopeRabbitmq[T]) Msg() T {
	var nul T
	return nul
}

func (r *eofEnvelopeRabbitmq[T]) Cid() string {
	return r.cid
}

func (r *eofEnvelopeRabbitmq[T]) Type() middleware.TypeMsg {
	return middleware.EOF
}

func (r *eofEnvelopeRabbitmq[T]) Id() uint64 {
	return 0 // EOF TIENE QUE TENER SU PROPPIO ID?
}

func (r *eofEnvelopeRabbitmq[T]) Ack(multiple bool) error {
	log2.Infof("Acking EOF envelope for cid: %s", r.cid)
	if r.msgEOF == nil {
		return fmt.Errorf("msgEOF is not initialized or already acked")
	}
	if err := r.msgEOF.Ack(false); err != nil {
		return fmt.Errorf("failed to ack EOF message: %v", err)
	}
	r.msgEOF = nil

	// Acknowledge all finish done IDs
	for id, tag := range r.finishDoneIds {
		if id == "" {
			return fmt.Errorf("finish done ID is empty for cid: %s", r.cid)
		}
		log2.Infof("Acking finish done ID: %v for cid: %s", id, r.cid)
		if err := tag.Ack(false); err != nil {
			return fmt.Errorf("failed to ack finish done ID %s: %v", id, err)
		}
		delete(r.finishDoneIds, id)
	}

	return nil
}

func (r *eofEnvelopeRabbitmq[T]) Nack(multiple bool) error {
	return fmt.Errorf("nack not implemented")
}

func newPrune2Envelope[T codec.Serializable[T]](
	cid string,
	closeSender SenderChannel[*CloseNotification],
	idWorker string,
	msgEofNotLider *amqp.Delivery,
) middleware.Envelope[T] {
	log2.Debugf("Creating prune2 envelope for cid: %s, idWorker: %s", cid, idWorker)
	return &prune2EnvelopeRabbitmq[T]{
		closeSender:    closeSender,
		cid:            cid,
		idWorker:       idWorker,
		msgEofNotLider: msgEofNotLider,
	}
}

type prune2EnvelopeRabbitmq[T codec.Serializable[T]] struct {
	cid            string
	closeSender    SenderChannel[*CloseNotification]
	idWorker       string
	msgEofNotLider *amqp.Delivery
}

func (r *prune2EnvelopeRabbitmq[T]) Msg() T {
	var nul T
	return nul
}

func (r *prune2EnvelopeRabbitmq[T]) Cid() string {
	return r.cid
}

func (r *prune2EnvelopeRabbitmq[T]) Type() middleware.TypeMsg {
	return middleware.Prune
}

func (r *prune2EnvelopeRabbitmq[T]) Id() uint64 {
	return 0 //TODO TIENE QUE TENER UN ID PROPIO?
}

func (r *prune2EnvelopeRabbitmq[T]) Ack(multiple bool) error {
	log2.Infof("Acking prune2 envelope for cid: %s, sending finishdone with idW: %s", r.cid, r.idWorker)
	if r.closeSender.ch == nil {
		return fmt.Errorf("cannot Ack: closeSender channel is nil")
	}
	if err := r.closeSender.Publish(context.Background(), &CloseNotification{closeNotificationFinishCidDone, r.idWorker}, r.cid, uint64(0)); err != nil { //TODO: revisar id si puede ignorarse
		return fmt.Errorf("failed to ack message in close notification: %v", err)
	} else {
		err := r.closeSender.Prune(r.cid)
		if err != nil {
			return fmt.Errorf("failed to send message in close notification: %v", err)
		}
	}
	if r.msgEofNotLider != nil {
		if err := r.msgEofNotLider.Ack(false); err != nil {
			return fmt.Errorf("failed to ack EOF message: %v", err)
		}
	}
	return nil
}

func (r *prune2EnvelopeRabbitmq[T]) Nack(multiple bool) error {
	return fmt.Errorf("nack not implemented")
}
