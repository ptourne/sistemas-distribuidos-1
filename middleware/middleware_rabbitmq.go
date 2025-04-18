package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MiddlewareRabbitmq[T any] struct {
	Conn *amqp.Connection
}

type ReceiverRabbitmq[T any] struct {
	inputMsgs           *<-chan amqp.Delivery
	inputCh             *amqp.Channel
	closeMsg            *<-chan amqp.Delivery
	closeCh             *amqp.Channel
	notifedClosed       bool
	asumeNoInFlightMsgs bool
	depleteTimmer       *time.Timer
	conn                *amqp.Connection
	isClosed            atomic.Bool
}

type EnvelopeRabbitmq[T any] struct {
	msg T
	tag *amqp.Delivery
}

type SenderRabbitmq[T any] struct {
	exchangeName string
	outputCh     *amqp.Channel
	closeCh      *amqp.Channel
	conn         *amqp.Connection
	isBlocked    atomic.Bool
	isClosed     atomic.Bool
}

var log = logger.NewConsoleLogger("middleware rabbitmq", logger.Debug)

func NewRabbitmq[T any]() (MiddlewareCola[T], error) {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	for range 5 {
		if err == nil {
			break
		}
		log.Errorf("Failed to connect to RabbitMQ: %v", err)
		time.Sleep(5 * time.Second)
		log.Infof("Retrying connection...")
		conn, err = amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	}
	if err != nil {
		return nil, err
	}

	middleware := MiddlewareRabbitmq[T]{
		Conn: conn,
	}
	return &middleware, nil
}

func (m *MiddlewareRabbitmq[T]) Close() error {
	log.Infof("CLOSING MIDDLEWARE")
	if m.Conn != nil {
		m.Conn.Close()
		m.Conn = nil
	}
	return nil
}

func (r *ReceiverRabbitmq[T]) Close() error {
	log.Infof("CLOSING RECEIVER")
	if r.inputCh != nil {
		r.inputCh.Close()
		r.inputCh = nil
	}
	if r.closeCh != nil {
		r.closeCh.Close()
		r.closeCh = nil
	}
	if r.depleteTimmer != nil {
		r.depleteTimmer.Stop()
		r.depleteTimmer = nil
	}
	return nil
}

func (s *SenderRabbitmq[T]) Close() error {
	log.Infof("CLOSING SENDER")
	if s.outputCh != nil {
		s.outputCh.Close()
		s.outputCh = nil
	}
	if s.closeCh != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := s.closeCh.PublishWithContext(ctx,
			closeExchangeName(s.exchangeName), // exchange
			"",                                // routing key
			false,                             // mandatory
			false,                             // immediate
			amqp.Publishing{
				ContentType: "text/plain",
				Body:        []byte{},
			})
		if err != nil {
			return fmt.Errorf("failed to publish a message: %v in chan %s", err, closeExchangeName(s.exchangeName))
		}
		log.Infof("CLOSEDDD message in chan %s", closeExchangeName(s.exchangeName))
		s.closeCh.Close()
		s.closeCh = nil
	}
	return nil
}

func (m *MiddlewareRabbitmq[T]) SuscribeTo(sourceName string) (Receiver[T], error) {
	if sourceName == "" {
		return nil, fmt.Errorf("readExchangeName is empty, should be a valid name")
	}
	return m.createReadQueue(sourceName, "")
}

func (m *MiddlewareRabbitmq[T]) ConsumeFrom(sourceName string, groupName string) (Receiver[T], error) {
	if sourceName == "" {
		return nil, fmt.Errorf("readExchangeName is empty, should be a valid name")
	}
	if groupName == "" {
		return nil, fmt.Errorf("groupQueueName is empty, should be a valid name")
	}
	return m.createReadQueue(sourceName, groupName)
}

func (m *MiddlewareRabbitmq[T]) createReadQueue(readExchangeName string, queueName string) (Receiver[T], error) {
	inputQueue, ch, err := m.createQueue(readExchangeName, queueName)
	if err != nil {
		return nil, err
	}
	msgs, err := ch.Consume(
		inputQueue.Name, // queue
		"",              // consumer
		false,           // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)

	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer %v", err)
	}

	closeQueue, closeCh, err := m.createQueue(closeExchangeName(readExchangeName), "")
	if err != nil {
		return nil, err
	}
	closeMsg, err := ch.Consume(
		closeQueue.Name, // queue
		"",              // consumer
		true,            // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)

	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer %v", err)
	}
	receiver := &ReceiverRabbitmq[T]{
		inputMsgs: &msgs,
		inputCh:   ch,
		closeMsg:  &closeMsg,
		closeCh:   closeCh,
		conn:      m.Conn,
		isClosed:  atomic.Bool{},
	}
	receiver.isClosed.Store(false)
	return receiver, nil
}

func (m *MiddlewareRabbitmq[T]) WriteTo(outputName string) (Sender[T], error) {
	outputCh, err := m.Conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %v", err)
	}
	err = outputCh.ExchangeDeclare(
		outputName, // name
		"fanout",   // type
		true,       // durable
		false,      // auto-deleted
		false,      // internal
		false,      // no-wait
		nil,        // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err)
	}

	closeCh, err := m.Conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %v", err)
	}
	err = closeCh.ExchangeDeclare(
		closeExchangeName(outputName), // name
		"fanout",                      // type
		true,                          // durable
		false,                         // auto-deleted
		false,                         // internal
		false,                         // no-wait
		nil,                           // arguments
	)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err)
	}
	sender := &SenderRabbitmq[T]{
		exchangeName: outputName,
		outputCh:     outputCh,
		closeCh:      closeCh,
		conn:         m.Conn,
		isBlocked:    atomic.Bool{},
		isClosed:     atomic.Bool{},
	}
	sender.isBlocked.Store(false)
	sender.isClosed.Store(false)
	return sender, nil
}

func (r *ReceiverRabbitmq[T]) Next(timeout *time.Timer) (Envelope[T], bool, error) {
	if r.inputMsgs == nil {
		return nil, false, fmt.Errorf("read channel is not initialized")
	}

	if r.asumeNoInFlightMsgs {
		return r.nextIfNoInFlightMsgs()
	}

	if r.notifedClosed {
		return r.nextIfNotifedClosed(timeout)
	}

	if timeout == nil {
		select {
		case msg, ok := <-*r.inputMsgs:
			if !ok {
				return nil, false, fmt.Errorf("read channel was closed")
			}
			return processMsg[T](msg)

		case _, ok := <-*r.closeMsg:
			if !ok {
				return nil, false, fmt.Errorf("close channel was closed")
			}
			r.notifedClosed = true
			return r.nextIfNotifedClosed(timeout)
		}
	}

	select {
	case msg, ok := <-*r.inputMsgs:
		if !ok {
			return nil, false, fmt.Errorf("read channel was closed")
		}
		return processMsg[T](msg)
	case <-timeout.C:
		return nil, false, fmt.Errorf("timeout reached while waiting for message")
	case _, ok := <-*r.closeMsg:
		if !ok {
			return nil, false, fmt.Errorf("close channel was closed")
		}
		r.notifedClosed = true
		return r.nextIfNotifedClosed(timeout)
	}
}

func (r *ReceiverRabbitmq[T]) nextIfNotifedClosed(timeout *time.Timer) (Envelope[T], bool, error) {
	if r.depleteTimmer == nil {
		r.depleteTimmer = time.NewTimer(time.Millisecond * 500)
	}
	if timeout == nil {
		select {
		case msg, ok := <-*r.inputMsgs:
			if !ok {
				return nil, false, fmt.Errorf("read channel was closed")
			}
			return processMsg[T](msg)
		case <-r.depleteTimmer.C:
			r.asumeNoInFlightMsgs = true
			return nil, false, nil
		}
	}

	select {
	case msg, ok := <-*r.inputMsgs:
		if !ok {
			return nil, false, fmt.Errorf("read channel was closed")
		}
		return processMsg[T](msg)
	case <-r.depleteTimmer.C:
		r.asumeNoInFlightMsgs = true
		return r.nextIfNoInFlightMsgs()
	case <-timeout.C:
		return nil, false, fmt.Errorf("timeout reached while waiting for message")
	}
}

func (r *ReceiverRabbitmq[T]) nextIfNoInFlightMsgs() (Envelope[T], bool, error) {
	select {
	case msg, ok := <-*r.inputMsgs:
		if !ok {
			return nil, false, fmt.Errorf("read channel was closed")
		}
		return processMsg[T](msg)
	default:
		return nil, false, nil
	}
}

func (r *ReceiverRabbitmq[T]) LimitUnacked(limit int) error {
	if r.inputCh == nil {
		return fmt.Errorf("read channel is not initialized")
	}
	return r.inputCh.Qos(
		limit, // prefetch count
		0,     // prefetch size
		false, // global
	)
}
func (r *ReceiverRabbitmq[T]) NotifyClose() {
	if r.inputCh == nil {
		return
	}
	connCloseChan := make(chan *amqp.Error)
	r.conn.NotifyClose(connCloseChan)

	go func() {
		err := <-connCloseChan
		if err != nil {
			r.isClosed.Store(true)
			log.Warnf("Conexión cerrada por RabbitMQ: %s", err)
		}
	}()
}
func (r *ReceiverRabbitmq[T]) IsClosed() bool {
	return r.isClosed.Load()
}

func processMsg[T any](msg amqp.Delivery) (Envelope[T], bool, error) {
	var receivedMovie T
	err := json.Unmarshal(msg.Body, &receivedMovie)
	if err != nil {
		return nil, false, fmt.Errorf("failed to unmarshal film: %v", err)
	}
	envelope := EnvelopeRabbitmq[T]{
		msg: receivedMovie,
		tag: &msg,
	}
	return &envelope, true, nil
}

func (r *EnvelopeRabbitmq[T]) Msg() T {
	return r.msg
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

func (s *SenderRabbitmq[T]) Send(row *T) error {
	if s.exchangeName == "" {
		return fmt.Errorf("write exchange is not initialized")
	}
	buf, err := json.Marshal(*row)
	if err != nil {
		return fmt.Errorf("failed to marshal film: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = s.outputCh.PublishWithContext(ctx,
		s.exchangeName, // exchange
		"",             // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "text/json",
			Body:        buf,
		})
	if err != nil {
		return fmt.Errorf("failed to publish a message: %v in chan %s", err, s.exchangeName)
	}
	//log.Infof("PUBLISHED message in chan %s", s.exchangeName)
	return nil
}

func (s *SenderRabbitmq[T]) LimitUnacked(limit int) error {
	if s.outputCh == nil {
		return fmt.Errorf("write channel is not initialized")
	}
	return s.outputCh.Qos(
		limit, // prefetch count
		0,     // prefetch size
		false, // global
	)
}

func (s *SenderRabbitmq[T]) NotifyBlocked() {
	blockedCh := make(chan amqp.Blocking)
	s.conn.NotifyBlocked(blockedCh)

	go func() {
		for block := range blockedCh {
			if block.Active {
				s.isBlocked.Store(true)
				log.Warnf("Conexión bloqueada por RabbitMQ: %s", block.Reason)
			} else {
				s.isBlocked.Store(false)
				log.Infof("Conexión desbloqueada por RabbitMQ")
			}
		}
	}()
}

func (s *SenderRabbitmq[T]) IsBlocked() bool {
	return s.isBlocked.Load()
}

func (s *SenderRabbitmq[T]) NotifyClose() {
	connCloseChan := make(chan *amqp.Error)
	s.conn.NotifyClose(connCloseChan)

	go func() {
		err := <-connCloseChan
		if err != nil {
			s.isClosed.Store(true)
			log.Warnf("Conexión cerrada por RabbitMQ: %s", err)
		}
	}()
}

func (s *SenderRabbitmq[T]) IsClosed() bool {
	return s.isClosed.Load()
}

func (m *MiddlewareRabbitmq[T]) createQueue(exchangeName string, groupName string) (*amqp.Queue, *amqp.Channel, error) {
	ch, err := m.Conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open a channel: %v", err)
	}
	err = ch.ExchangeDeclare(
		exchangeName, // name
		"fanout",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to declare exchange %v", err)
	}

	queue, err := ch.QueueDeclare(
		groupName, // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)

	if err != nil {
		return nil, nil, fmt.Errorf("failed to declare queue %v", err)
	}

	err = ch.QueueBind(
		queue.Name,   // queue name
		"",           // routing key
		exchangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to declare queue %v", err)
	}
	return &queue, ch, nil
}

func closeExchangeName(readExchangeName string) string {
	return fmt.Sprintf("%s_close", readExchangeName)
}
