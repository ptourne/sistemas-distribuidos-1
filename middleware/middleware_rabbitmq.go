package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MiddlewareRabbitmq[T any] struct {
	Conn *amqp.Connection
}

type ReceiverRabbitmq[T any] struct {
	inputMsgs                    *<-chan amqp.Delivery
	inputCh                      *amqp.Channel
	closeMsg                     *<-chan amqp.Delivery
	closeCh                      *amqp.Channel
	producerCountReqCh           *amqp.Channel
	producerCountReqExchangeName string
	producerCountResMsg          *<-chan amqp.Delivery
	producerCountResCh           *amqp.Channel
	notifedClosed                bool
	asumeNoInFlightMsgs          bool
	depleteTimmer                *time.Timer
}

type EnvelopeRabbitmq[T any] struct {
	msg T
	tag *amqp.Delivery
}

type SenderRabbitmq[T any] struct {
	exchangeName             string
	outputCh                 *amqp.Channel
	closeCh                  *amqp.Channel
	producerCountCh          *amqp.Channel
	producerCountReplier     chan struct{}
	producerCountReplierKill chan struct{}
}

var log = logger.NewConsoleLogger("middleware", logger.Debug)

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

func (m *ReceiverRabbitmq[T]) Qos(prefetchCount int, prefetchSize int) error {
	return m.inputCh.Qos(prefetchCount, prefetchSize, false)
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
	if r.producerCountReqCh != nil {
		r.producerCountReqCh.Close()
		r.producerCountReqCh = nil
	}
	if r.producerCountResCh != nil {
		r.producerCountResCh.Close()
		r.producerCountResCh = nil
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
	if s.producerCountCh != nil {
		close(s.producerCountReplierKill)
		<-s.producerCountReplier
		s.producerCountCh.Close()
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
	inputMsg, inputCh, err := m.createConsumer(readExchangeName, queueName)
	if err != nil {
		return nil, err
	}

	closeMsg, closeCh, err := m.createConsumer(closeExchangeName(readExchangeName), "")
	if err != nil {
		return nil, err
	}

	producerCountResMsg, producerCountResCh, err := m.createConsumer(producerCountResExchangeName(readExchangeName), "")
	if err != nil {
		return nil, err
	}

	producerCountReqExchangeName := producerCountReqExchangeName(readExchangeName)
	producerCountReqCh, err := m.createProducer(producerCountReqExchangeName)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err)
	}

	receiver := &ReceiverRabbitmq[T]{
		inputMsgs:                    &inputMsg,
		inputCh:                      inputCh,
		closeMsg:                     &closeMsg,
		closeCh:                      closeCh,
		producerCountReqCh:           producerCountReqCh,
		producerCountReqExchangeName: producerCountReqExchangeName,
		producerCountResCh:           producerCountResCh,
		producerCountResMsg:          &producerCountResMsg,
	}
	return receiver, nil
}

func (m *MiddlewareRabbitmq[T]) createProducer(readExchangeName string) (*amqp.Channel, error) {
	producerCountReqCh, err := m.Conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %v", err)
	}
	err = producerCountReqCh.ExchangeDeclare(
		readExchangeName, // name
		"fanout",         // type
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // no-wait
		nil,              // arguments
	)
	return producerCountReqCh, nil
}

func (m *MiddlewareRabbitmq[T]) createConsumer(readExchangeName string, queueName string) (<-chan amqp.Delivery, *amqp.Channel, error) {
	inputQueue, inputCh, err := m.createQueue(readExchangeName, queueName)
	if err != nil {
		return nil, nil, err
	}
	msgs, err := inputCh.Consume(
		inputQueue.Name, // queue
		"",              // consumer
		false,           // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to register a consumer %v", err)
	}
	return msgs, inputCh, nil
}

func (m *MiddlewareRabbitmq[T]) WriteTo(outputName string) (Sender[T], error) {
	outputCh, err := m.createProducer(outputName)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err)
	}

	closeCh, err := m.createProducer(closeExchangeName(outputName))
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err)
	}

	producerCountReqMsg, producerCountReqCh, err := m.createConsumer(producerCountReqExchangeName(outputName), "")
	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer %v", err)
	}

	producerCountResCh, err := m.createProducer(producerCountResExchangeName(outputName))
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err)
	}

	producerCountReplier := make(chan struct{})
	producerCountReplierKill := make(chan struct{})
	task := func() {
		close(producerCountReplier)
		producerCountResCh.Close()
		producerCountReqCh.Close()
		for {
			select {
			case msg := <-producerCountReqMsg:
				req, ok, err := processMsg[CountProducerReq](msg)
				if err != nil {
					log.Errorf("error getting count producer request: %s", err)
					return
				}
				if !ok {
					log.Debugf("count producer request was closed")
					return
				}
				err = publish(req.Msg(), producerCountResCh, producerCountResExchangeName(outputName))
				if err != nil {
					log.Errorf("Error sending reply: %s", err)
					req.Nack(false)
					return
				}
				req.Ack(false)
			case <-producerCountReplierKill:
				return
			}
		}
	}
	_ = task
	go task()

	sender := &SenderRabbitmq[T]{
		exchangeName:             outputName,
		outputCh:                 outputCh,
		closeCh:                  closeCh,
		producerCountReplier:     producerCountReplier,
		producerCountReplierKill: producerCountReplierKill,
	}
	return sender, nil
}

func publish[T any](msg T, producerCountResCh *amqp.Channel, outputName string) error {
	buf, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal reply: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err = producerCountResCh.PublishWithContext(ctx,
		outputName, // exchange
		"",         // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType: "text/json",
			Body:        buf,
		})
	if err != nil {
		return fmt.Errorf("failed to publish a message: %v in chan %s", err, outputName)
	}
	log.Infof("PUBLISHED message in chan %s", outputName)
	return nil
}

type CountProducerReq struct {
	ID uint `json:"id"`
}

// Returns:
// - Envelope[T]: The next message from the input channel.
// - bool: True if the message was successfully processed, false otherwise.
// - error: An error if occurred during processing, nil otherwise.
//
// If timer triggers, the function returns an error: "timeout reached while waiting for message"
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

func (s *SenderRabbitmq[T]) Send(row *T) error {
	if s.exchangeName == "" {
		return fmt.Errorf("write exchange is not initialized")
	}
	err := publish[T](*row, s.outputCh, s.exchangeName)

	if err != nil {
		return fmt.Errorf("failed to publish a message: %v in chan %s", err, s.exchangeName)
	}
	return nil
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

	queueName := ""
	if groupName != "" {
		queueName = fmt.Sprintf("%s_%s", exchangeName, groupName)
	}
	queue, err := ch.QueueDeclare(
		queueName, // name
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

func producerCountReqExchangeName(readExchangeName string) string {
	return fmt.Sprintf("%s_count_req", readExchangeName)
}

func producerCountResExchangeName(readExchangeName string) string {
	return fmt.Sprintf("%s_count_rep", readExchangeName)
}

func (r *ReceiverRabbitmq[T]) CountProducers() (uint, error) {
	producerCount := 0
	reqID := rand.UintN(1000000000)
	err := publish(CountProducerReq{}, r.producerCountReqCh, r.producerCountReqExchangeName)
	if err != nil {
		return 0, err
	}
	timer := time.NewTimer(time.Millisecond * 500)
	shouldContinue := true
	for shouldContinue {
		select {
		case <-timer.C:
			shouldContinue = false
			break
		default:
			select {
			case msg := <-*r.producerCountResMsg:
				rep, _, err := processMsg[CountProducerReq](msg)
				if err != nil {
					rep.Nack(false)
					return 0, err
				}
				if rep.Msg().ID == reqID {
					producerCount++
				}
				rep.Ack(false)
			case <-timer.C:
				shouldContinue = false
				break
			}
		}
	}
	return uint(producerCount), nil
}
