package rabbitmq

import (
	"context"
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

func newNormalEnvelope[T codec.Serializable[T]](cid string, msgbody T, tag *amqp.Delivery) middleware.Envelope[T] {
	return &EnvelopeRabbitmq[T]{
		msg: msgbody,
		tag: tag,
		cid: cid,
	}
}

type EnvelopeRabbitmq[T codec.Serializable[T]] struct {
	msg T
	tag *amqp.Delivery
	cid string
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

func (r *EnvelopeRabbitmq[T]) Ack(multiple bool) error {
	if r.tag == nil {
		return fmt.Errorf("tag is not initialized or already acked")
	}
	err := r.tag.Ack(multiple)
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
	err := r.tag.Nack(multiple, true)
	if err != nil {
		return fmt.Errorf("failed to ack message: %v", err)
	}
	r.tag = nil
	return nil
}

func newEOFEnvelope[T codec.Serializable[T]](
	cid string,
) middleware.Envelope[T] {
	return &eofEnvelopeRabbitmq[T]{
		cid: cid,
	}
}

type eofEnvelopeRabbitmq[T codec.Serializable[T]] struct {
	cid string
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

func (r *eofEnvelopeRabbitmq[T]) Ack(multiple bool) error {
	return nil
}

func (r *eofEnvelopeRabbitmq[T]) Nack(multiple bool) error {
	return fmt.Errorf("nack not implemented")
}

func newPrune2Envelope[T codec.Serializable[T]](
	cid string,
	closeSender SenderChannel[*CloseNotification],
) middleware.Envelope[T] {
	return &prune2EnvelopeRabbitmq[T]{
		closeSender: closeSender,
		cid:         cid,
	}
}

type prune2EnvelopeRabbitmq[T codec.Serializable[T]] struct {
	cid         string
	closeSender SenderChannel[*CloseNotification]
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

func (r *prune2EnvelopeRabbitmq[T]) Ack(multiple bool) error {
	if err := r.closeSender.Publish(context.Background(), &CloseNotification{closeNotificationFinishCidDone}, r.cid); err != nil {
		return fmt.Errorf("failed to ack message in close notification: %v", err)
	}
	return nil
}

func (r *prune2EnvelopeRabbitmq[T]) Nack(multiple bool) error {
	return fmt.Errorf("nack not implemented")
}
