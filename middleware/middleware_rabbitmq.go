package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MiddlewareRabbitmq[T any] struct {
	Conn *amqp.Connection
}

type ReceiverRabbitmq[T any] struct {
	input                        ReceiverChannel[T]
	close                        ReceiverChannel[CloseNotification]
	producerCountReq             SenderChannel[CountProducerReq]
	producerCountRes             ReceiverChannel[CountProducerReq]
	asumeNoInFlightMsgs          bool
	timeCloseNotificationArrived *time.Time
	lastProducerCount            int
	// isBlocked           atomic.Bool
	// isClosed            atomic.Bool
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
	isBlocked                atomic.Bool
	isClosed                 atomic.Bool
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
		for range 20 {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			err := s.close.Publish(ctx, CloseNotification{})

			if err != nil {
				return fmt.Errorf("failed to publish a message: %v in chan %s", err, closeExchangeName(s.exchangeName))
			}
		}
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

func (m *MiddlewareRabbitmq[T]) ConsumeFrom(sourceName string, groupName string) (Receiver[T], error) {
	log.Infof("Creating ConsumeFrom exchange '%s' with groupName '%s' ", sourceName, groupName)
	if sourceName == "" {
		return nil, fmt.Errorf("readExchangeName is empty, should be a valid name")
	}
	if groupName == "" {
		return nil, fmt.Errorf("groupQueueName is empty, should be a valid name")
	}
	return m.createReadQueueRK(sourceName, groupName, "fanout", "")
}

func (m *MiddlewareRabbitmq[T]) ConsumeFromRK(sourceName string, groupName string, t string, routingKey string) (Receiver[T], error) {
	log.Infof("Creating ConsumeFrom exchange '%s' with groupName '%s' ", sourceName, groupName)
	if sourceName == "" {
		return nil, fmt.Errorf("readExchangeName is empty, should be a valid name")
	}
	if groupName == "" {
		return nil, fmt.Errorf("groupQueueName is empty, should be a valid name")
	}
	return m.createReadQueueRK(sourceName, groupName, t, routingKey)
}

func (m *MiddlewareRabbitmq[T]) createReadQueueRK(readExchangeName string, queueName string, t string, routingKey string) (Receiver[T], error) {
	input, err := createConsumerRK[T, T](m, readExchangeName, queueName, t, routingKey)
	if err != nil {
		return nil, err
	}

	close, err := createConsumerRK[T, CloseNotification](m, closeExchangeName(readExchangeName), "", "fanout", "")
	if err != nil {
		return nil, err
	}

	producerCountRes, err := createConsumerRK[T, CountProducerReq](m, producerCountResExchangeName(readExchangeName), "", "fanout", "")
	if err != nil {
		return nil, err
	}

	producerCountReq, err := CreateProducerRK[T, CountProducerReq](m, producerCountReqExchangeName(readExchangeName), "fanout")
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err)
	}

	receiver := &ReceiverRabbitmq[T]{
		input:             input,
		close:             close,
		producerCountReq:  *producerCountReq,
		producerCountRes:  producerCountRes,
		lastProducerCount: -1,
	}

	// log.Debugf("ReceiverRabbitmq: '%s', '%s', producer count: %d", receiver.input.exchangeName, receiver.input.queueName, receiver.lastProducerCount)
	return receiver, nil
}

func CreateProducerRK[T, I any](m *MiddlewareRabbitmq[T], readExchangeName string, t string) (*SenderChannel[I], error) {
	producerCountReqCh, err := m.Conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %v", err)
	}
	// log.Debugf("exchangeDeclare: '%s'", readExchangeName)
	err = producerCountReqCh.ExchangeDeclare(
		readExchangeName, // name
		t,                // type
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

func createConsumerRK[T, I any](m *MiddlewareRabbitmq[T], readExchangeName string, queueName string, t string, routingKey string) (ReceiverChannel[I], error) {
	inputQueue, inputCh, err := m.createQueueRK(readExchangeName, queueName, t, routingKey)
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
	subscribersMap := make(map[string][]string)
	for _, sub := range subscribers {
		subscribersMap[sub] = []string{""}
	}
	return m.WriteToRK(outputName, subscribersMap, "fanout")
}

func (m *MiddlewareRabbitmq[T]) WriteToRK(outputName string, subscribers map[string][]string, t string) (Sender[T], error) {
	log.Infof("Creating WriteTo exchange '%s'", outputName)
	output, err := CreateProducerRK[T, T](m, outputName, t)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err)
	}

	closep, err := CreateProducerRK[T, CloseNotification](m, closeExchangeName(outputName), "fanout")
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err)
	}

	for sub, routingKeys := range subscribers {
		for _, routingKey := range routingKeys {
			_, ch, err := m.createQueueRK(outputName, sub, t, routingKey)
			if err != nil {
				return nil, fmt.Errorf("cannot create subscriber %s: %v", sub, err)
			}
			_, ch2, err := m.createQueueRK(closeExchangeName(outputName), sub, "fanout", "")
			if err != nil {
				return nil, fmt.Errorf("cannot create subscriber %s: %v", sub, err)
			}
			ch2.Close()
			ch.Close()
		}
	}

	producerCountReq, err := createConsumerRK[T, CountProducerReq](m, producerCountReqExchangeName(outputName), "", "fanout", "")
	if err != nil {
		return nil, fmt.Errorf("failed to register a consumer %v", err)
	}

	producerCountRes, err := CreateProducerRK[T, CountProducerReq](m, producerCountResExchangeName(outputName), "fanout")
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err)
	}

	producerCountReplier := make(chan struct{})
	producerCountReplierKill := make(chan struct{})
	task := func() {
		// log.Debugf("Listening on producer count replier")
		defer close(producerCountReplier)
		defer producerCountRes.Close()
		defer producerCountReq.Close()
		for {
			// log.Debugf("Waiting for producer count request: '%s' from '%s'", producerCountReq.exchangeName, producerCountReq.queueName)
			select {
			case msg := <-*producerCountReq.C:
				log.Infof("RECEIVED producer count request: '%s' from '%s'", producerCountReq.exchangeName, producerCountReq.queueName)
				req, ok, err := processMsg[CountProducerReq](msg)
				if err != nil {
					log.Errorf("error getting count producer request: %s when processing message '%s' from '%s' as '%s'", err, string(msg.Body), producerCountReq.exchangeName, producerCountReq.queueName)
					return
				}
				if !ok {
					log.Debugf("count producer request was closed")
					return
				}
				// log.Debugf("Received producer count request: '%s' from '%s' with id: %d", producerCountReq.exchangeName, producerCountReq.queueName, req.Msg().ID)
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
				log.Infof("kill go routine for producer count request: '%s' from '%s'", producerCountReq.exchangeName, producerCountReq.queueName)
				return
			}
		}
	}
	go task()

	sender := &SenderRabbitmq[T]{
		exchangeName:             outputName,
		output:                   *output,
		close:                    closep,
		producerCountReplier:     producerCountReplier,
		producerCountReplierKill: &producerCountReplierKill,
	}
	sender.isBlocked.Store(false)
	sender.isClosed.Store(false)
	return sender, nil
}

func (s *SenderChannel[T]) Publish(ctx context.Context, msg T) error {
	return s.PublishRK(ctx, msg, "")
}

func (s *SenderChannel[T]) PublishRK(ctx context.Context, msg T, routingKey string) error {
	buf, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal reply: %v", err)
	}
	// log.Debugf("PUBLISHING message '%s' in chan %s", string(buf), s.exchangeName)
	err = s.ch.PublishWithContext(ctx,
		s.exchangeName, // exchange
		routingKey,     // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "text/json",
			Body:        buf,
		})
	if err != nil {
		return fmt.Errorf("failed to publish a message: %v in chan %s", err, s.exchangeName)
	}
	log.Debugf("PUBLISHED message in chan %s", s.exchangeName)
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
		panic("Should not call Next() when already received close notification")
	}

	if r.lastProducerCount == 0 {
		return r.nextIfNotifedClosed(timeout)
	}

	var timeoutC <-chan time.Time = nil
	if timeout != nil {
		timeoutC = timeout.C
	}

	for {
		select {
		case msg, ok := <-*r.input.C:
			if !ok {
				return nil, false, fmt.Errorf("read channel was closed")
			}
			return processMsg[T](msg)

		case _, ok := <-*r.close.C:
			log.Debugf("A close channel was closed ARRIVE")
			var err error
			r.lastProducerCount, err = r.CountProducers()
			log.Debugf("cant prod: %d", r.lastProducerCount)
			if err != nil {
				return nil, false, fmt.Errorf("failed to get producer count %v", err)
			}
			if r.lastProducerCount == 0 {
				log.Infof("cant producers 0")
				return r.nextHandleCloseMsg(ok, timeout)
			}
		case <-timeoutC:
			return nil, false, fmt.Errorf("timeout reached while waiting for message")
		}
	}

}

func (r *ReceiverRabbitmq[T]) nextHandleCloseMsg(ok bool, timeout *time.Timer) (Envelope[T], bool, error) {
	if !ok {
		return nil, false, fmt.Errorf("close channel was closed")
	}
	newVar := time.Now()
	r.timeCloseNotificationArrived = &newVar
	return r.nextIfNotifedClosed(timeout)
}

func (r *ReceiverRabbitmq[T]) nextIfNotifedClosed(timeout *time.Timer) (Envelope[T], bool, error) {
	// log.Infof("nextIfNotifedClosed")
	const timeUntilArrivalToBroker = time.Millisecond * 500
	const transmissionTime = time.Millisecond * 500
	remaining := timeUntilArrivalToBroker - time.Since(*r.timeCloseNotificationArrived) // timeout until finish sending
	remaining = max(remaining+transmissionTime, transmissionTime)
	depleteTimmer := time.NewTimer(remaining)
	defer depleteTimmer.Stop()

	var timeoutC <-chan time.Time = nil
	if timeout != nil {
		timeoutC = timeout.C
	}

	select {
	case msg, ok := <-*r.input.C:
		log.Debugf("msg received FROM nextIfNotifedClosed: msg: %s", string(msg.Body))
		if !ok {
			return nil, false, fmt.Errorf("read channel was closed")
		}
		return processMsg[T](msg)
	case <-depleteTimmer.C:
		log.Debugf("depleteTimmer reached")
		r.asumeNoInFlightMsgs = true
		return nil, false, nil
	case <-timeoutC:
		return nil, false, fmt.Errorf("timeout reached while waiting for message")
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
	return s.SendRK(row, "")
}

func (s *SenderRabbitmq[T]) SendRK(row *T, routingKey string) error {
	if s.exchangeName == "" {
		return fmt.Errorf("write exchange is not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err := s.output.PublishRK(ctx, *row, routingKey)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to publish a message: %v in chan %s", err, s.exchangeName)
	}
	cancel()
	return nil
}

func (m *MiddlewareRabbitmq[T]) createQueue(exchangeName string, groupName string) (*amqp.Queue, *amqp.Channel, error) {
	return m.createQueueRK(exchangeName, groupName, "fanaout", "")
}

func (m *MiddlewareRabbitmq[T]) createQueueRK(exchangeName string, groupName string, t string, routingKey string) (*amqp.Queue, *amqp.Channel, error) {
	ch, err := m.Conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open a channel: %v", err)
	}
	err = ch.ExchangeDeclare(
		exchangeName, // name
		t,            // type
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
		routingKey,   // routing key
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
	return fmt.Sprintf("%s_count_res", readExchangeName)
}

func (r *ReceiverRabbitmq[T]) CountProducers() (int, error) {
	log.Infof("Calling count producers '%s'-'%s'", r.input.exchangeName, r.input.queueName)
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

	return producerCount, nil
}
