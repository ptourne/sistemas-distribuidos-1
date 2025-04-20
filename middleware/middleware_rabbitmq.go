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
	input                  ReceiverChannel[T]
	close                  ReceiverChannel[CloseNotification]
	producerCountReq       SenderChannel[CountProducerReq]
	producerCountRes       ReceiverChannel[CountProducerReq]
	asumeNoInFlightMsgs    bool
	depleteTimmerStartTime *time.Time
}

type ReceiverChannel[T any] struct {
	exchangeName string
	queueName    string
	amqpCh       *amqp.Channel
	C            *<-chan amqp.Delivery
}

func (r *ReceiverChannel[T]) Close() {
	if r.amqpCh != nil {
		log.Debugf("Clossing channel '%s', '%s'", r.exchangeName, r.queueName)
		r.amqpCh.Close()
		r.amqpCh = nil
	}
}

type EnvelopeRabbitmq[T any] struct {
	msg T
	tag *amqp.Delivery
}

type SenderRabbitmq[T any] struct {
	exchangeName             string
	output                   SenderChannel[T]
	close                    *SenderChannel[CloseNotification]
	producerCountReplier     chan struct{}
	producerCountReplierKill *chan struct{}
}

type SenderChannel[T any] struct {
	exchangeName string
	ch           *amqp.Channel
}

func (s *SenderChannel[T]) Close() {
	if s.ch != nil {
		log.Debugf("Clossing channel '%s'", s.exchangeName)
		s.ch.Close()
		s.ch = nil
	}
}

var log = logger.NewConsoleLogger("middleware", logger.Info)

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
	return m.input.amqpCh.Qos(prefetchCount, prefetchSize, false)
}

func (r *ReceiverRabbitmq[T]) Close() error {
	log.Debugf("CLOSING RECEIVER: '%s', '%s", r.input.exchangeName, r.input.queueName)
	r.input.Close()
	r.close.Close()
	r.producerCountReq.Close()
	r.producerCountRes.Close()
	return nil
}

type CloseNotification struct{}

func (s *SenderRabbitmq[T]) Close() error {
	log.Debugf("CLOSING SENDER")
	s.output.Close()
	if s.close != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := s.close.Publish(ctx, CloseNotification{})

		if err != nil {
			return fmt.Errorf("failed to publish a message: %v in chan %s", err, closeExchangeName(s.exchangeName))
		}
		log.Infof("CLOSEDDD message in chan %s", closeExchangeName(s.exchangeName))
		s.close.Close()
		s.close = nil
	}
	if s.producerCountReplierKill != nil {
		close(*s.producerCountReplierKill)
		<-s.producerCountReplier
		s.producerCountReplierKill = nil
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
	input, err := createConsumer[T, T](m, readExchangeName, queueName)
	if err != nil {
		return nil, err
	}

	close, err := createConsumer[T, CloseNotification](m, closeExchangeName(readExchangeName), "")
	if err != nil {
		return nil, err
	}

	producerCountRes, err := createConsumer[T, CountProducerReq](m, producerCountResExchangeName(readExchangeName), "")
	if err != nil {
		return nil, err
	}

	producerCountReqExchangeName := producerCountReqExchangeName(readExchangeName)
	producerCountReq, err := CreateProducer[T, CountProducerReq](m, producerCountReqExchangeName)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err)
	}

	receiver := &ReceiverRabbitmq[T]{
		input:            input,
		close:            close,
		producerCountReq: *producerCountReq,
		producerCountRes: producerCountRes,
	}
	return receiver, nil
}

func CreateProducer[T, I any](m *MiddlewareRabbitmq[T], readExchangeName string) (*SenderChannel[I], error) {
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
	if err != nil {
		return nil, err
	}
	newVar := SenderChannel[I]{readExchangeName, producerCountReqCh}
	return &newVar, nil
}

func createConsumer[T, I any](m *MiddlewareRabbitmq[T], readExchangeName string, queueName string) (ReceiverChannel[I], error) {
	inputQueue, inputCh, err := m.createQueue(readExchangeName, queueName)
	if err != nil {
		return ReceiverChannel[I]{}, nil
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
		return ReceiverChannel[I]{}, fmt.Errorf("failed to register a consumer %v", err)
	}
	return ReceiverChannel[I]{readExchangeName, queueName, inputCh, &msgs}, nil
}

func (m *MiddlewareRabbitmq[T]) WriteTo(outputName string, subscribers []string) (Sender[T], error) {
	output, err := CreateProducer[T, T](m, outputName)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err)
	}

	closep, err := CreateProducer[T, CloseNotification](m, closeExchangeName(outputName))
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err)
	}

	for _, sub := range subscribers {
		_, ch, err := m.createQueue(outputName, sub)
		if err != nil {
			return nil, fmt.Errorf("cannot create subscriber %s: %v", sub, err)
		}
		ch.Close()
	}

	producerCountReq, err := createConsumer[T, CountProducerReq](m, producerCountReqExchangeName(outputName), "")
	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer %v", err)
	}

	producerCountRes, err := CreateProducer[T, CountProducerReq](m, producerCountResExchangeName(outputName))
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err)
	}

	producerCountReplier := make(chan struct{})
	producerCountReplierKill := make(chan struct{})
	task := func() {
		defer close(producerCountReplier)
		defer producerCountRes.Close()
		defer producerCountReq.Close()
		for {
			select {
			case msg := <-*producerCountReq.C:
				req, ok, err := processMsg[CountProducerReq](msg)
				if err != nil {
					log.Errorf("error getting count producer request: %s when processing message '%s' from '%s' as '%s'", err, string(msg.Body), producerCountReq.exchangeName, producerCountReq.queueName)
					return
				}
				if !ok {
					log.Debugf("count producer request was closed")
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), 500*time.Second)
				err = producerCountRes.Publish(ctx, req.Msg())
				if err != nil {
					log.Errorf("Error sending reply: %s", err)
					req.Nack(false)
					cancel()
					return
				}
				cancel()
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
		output:                   *output,
		close:                    closep,
		producerCountReplier:     producerCountReplier,
		producerCountReplierKill: &producerCountReplierKill,
	}
	return sender, nil
}

func (s *SenderChannel[T]) Publish(ctx context.Context, msg T) error {
	buf, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal reply: %v", err)
	}
	log.Debugf("PUBLISHING message '%s' in chan %s", string(buf), s.exchangeName)
	err = s.ch.PublishWithContext(ctx,
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
	log.Infof("PUBLISHED message in chan %s", s.exchangeName)
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
	if r.input.amqpCh == nil {
		return nil, false, fmt.Errorf("read channel is not initialized")
	}

	if r.asumeNoInFlightMsgs {
		return nil, false, nil
	}

	if timeout == nil {
		select {
		case msg, ok := <-*r.input.C:
			if !ok {
				return nil, false, fmt.Errorf("read channel was closed")
			}
			return processMsg[T](msg)

		case producerCount := <-r.CountProducersAsync():
			if producerCount.Err != nil {
				return nil, false, fmt.Errorf("error while counting producers: %w", producerCount.Err)
			}
			if producerCount.Res == 0 {
				return nil, false, fmt.Errorf("no producers available")
			}
		}
	}

	select {
	case msg, ok := <-*r.input.C:
		if !ok {
			return nil, false, fmt.Errorf("read channel was closed")
		}
		return processMsg[T](msg)
	case <-timeout.C:
		return nil, false, fmt.Errorf("timeout reached while waiting for message")
	case _, ok := <-*r.close.C:
		return r.nextHandleCloseMsg(ok, timeout)
	}
}

func (r *ReceiverRabbitmq[T]) nextHandleCloseMsg(ok bool, timeout *time.Timer) (Envelope[T], bool, error) {
	if !ok {
		return nil, false, fmt.Errorf("close channel was closed")
	}
	newVar := time.Now()
	r.depleteTimmerStartTime = &newVar
	return r.nextIfNotifedClosed(timeout)
}

func (r *ReceiverRabbitmq[T]) nextIfNotifedClosed(timeout *time.Timer) (Envelope[T], bool, error) {
	log.Infof("nextIfNotifedClosed")
	remaining := time.Millisecond*500 - time.Since(*r.depleteTimmerStartTime) // timeout until finish sending
	remaining = max(remaining, time.Millisecond*500, time.Millisecond*500)
	log.Infof("remaining time: %v", remaining)
	depleteTimmer := time.NewTimer(remaining)
	defer depleteTimmer.Stop()

	if timeout == nil {
		log.Infof("timeout is nil")
		select {
		case msg, ok := <-*r.input.C:
			if !ok {
				return nil, false, fmt.Errorf("read channel was closed")
			}
			return processMsg[T](msg)
		case <-depleteTimmer.C:
			r.asumeNoInFlightMsgs = true
			return nil, false, nil
		}
	}

	log.Infof("timeout is not nil")
	select {
	case msg, ok := <-*r.input.C:
		log.Infof("msg received: ok: %v, msg: %s", ok, string(msg.Body))
		if !ok {
			return nil, false, fmt.Errorf("read channel was closed")
		}
		return processMsg[T](msg)
	case <-depleteTimmer.C:
		r.asumeNoInFlightMsgs = true
		return r.nextIfNoInFlightMsgs()
	case <-timeout.C:
		return nil, false, fmt.Errorf("timeout reached while waiting for message")
	}
}

func (r *ReceiverRabbitmq[T]) nextIfNoInFlightMsgs() (Envelope[T], bool, error) {
	select {
	case msg, ok := <-*r.input.C:
		if !ok {
			return nil, false, fmt.Errorf("read channel was closed")
		}
		return processMsg[T](msg)
	case <-time.NewTimer(time.Millisecond * 500).C:
		r.asumeNoInFlightMsgs = true
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
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err := s.output.Publish(ctx, *row)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to publish a message: %v in chan %s", err, s.exchangeName)
	}
	cancel()
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
	log.Debugf("createQueue: Creating queue '%s' for exchange '%s'", queueName, exchangeName)
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
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Second)
	err := r.producerCountReq.Publish(ctx, CountProducerReq{reqID})
	if err != nil {
		cancel()
		return 0, err
	}
	cancel()
	timer := time.NewTimer(time.Millisecond * 500)
	shouldContinue := true
	for shouldContinue {
		select {
		case <-timer.C:
			shouldContinue = false
		default:
			select {
			case msg := <-*r.producerCountRes.C:
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

type AsyncRes[T any] struct {
	Res T
	Err error
}

func (r *ReceiverRabbitmq[T]) CountProducersAsync() chan AsyncRes[uint] {
	res := make(chan AsyncRes[uint], 1)
	go func() {
		count, err := r.CountProducers()
		res <- AsyncRes[uint]{Res: count, Err: err}
	}()
	return res
}
