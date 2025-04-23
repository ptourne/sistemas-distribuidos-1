package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand/v2"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"

	amqp "github.com/rabbitmq/amqp091-go"
)

const HEART_BEAT_INTERVAL = 1000 * time.Millisecond
const HEART_BEAT_RTT = 500 * time.Millisecond

type MiddlewareRabbitmq[T any] struct {
	Conn *amqp.Connection
}

type ReceiverRabbitmq[T any] struct {
	input                        ReceiverChannel[T]
	close                        ReceiverChannel[HeartBeat]
	asumeNoInFlightMsgs          bool
	wereSomeProducers            bool
	noMoreProducers              bool
	heartBeatListener            ReceiverChannel[HeartBeat]
	timeCloseNotificationArrived *time.Time
	m                            *MiddlewareRabbitmq[T]
}

type ReceiverChannel[T any] struct {
	exchangeName string
	queueName    string
	anonimous    bool
	amqpCh       *amqp.Channel
	C            *<-chan amqp.Delivery
}

func (r ReceiverChannel[T]) Purge() error {
	_, err := r.amqpCh.QueuePurge(r.queueName, false)
	return err
}

func (r *ReceiverChannel[T]) Close() {
	if r.amqpCh != nil {
		log.Debugf("Clossing channel '%s', '%s'", r.exchangeName, r.queueName)
		if r.anonimous {
			r.amqpCh.QueueDelete(r.queueName, false, false, false)
		}
		r.amqpCh.Close()
		r.amqpCh = nil
	}
}

type EnvelopeRabbitmq[T any] struct {
	msg T
	tag *amqp.Delivery
}

type SenderRabbitmq[T any] struct {
	exchangeName string
	output       SenderChannel[T]
	heart        chan struct{}
	killHeart    *chan struct{}
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
	return m.input.amqpCh.Qos(prefetchCount, prefetchSize, false)
}

func (r *ReceiverRabbitmq[T]) Close() error {
	log.Debugf("CLOSING RECEIVER: '%s', '%s", r.input.exchangeName, r.input.queueName)
	r.input.Close()
	r.close.Close()
	r.heartBeatListener.Close()
	r.m = nil
	return nil
}

type HeartBeat struct {
	ID uint `json:"id"`
}

func (s *SenderRabbitmq[T]) Close() error {
	log.Debugf("CLOSING SENDER")
	s.output.Close()
	if s.killHeart != nil {
		close(*s.killHeart)
		<-s.heart
		s.killHeart = nil
	}
	return nil
}

func (m *MiddlewareRabbitmq[T]) SuscribeTo(sourceName string) (Receiver[T], error) {
	log.Infof("Creating SuscribeTo exchange '%s' with groupName", sourceName)

	if sourceName == "" {
		return nil, fmt.Errorf("readExchangeName is empty, should be a valid name")
	}
	return m.createReadQueue(sourceName, "")
}

func (m *MiddlewareRabbitmq[T]) ConsumeFrom(sourceName string, groupName string) (Receiver[T], error) {
	log.Infof("Creating ConsumeFrom exchange '%s' with groupName '%s' ", sourceName, groupName)
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

	close, err := createConsumer[T, HeartBeat](m, heartBeatName(readExchangeName), "")
	if err != nil {
		return nil, err
	}

	heartBeatListener, err := createConsumer[T, HeartBeat](m, heartBeatName(readExchangeName), "")
	if err != nil {
		return nil, err
	}
	if err != nil {
		return nil, err
	}
	receiver := &ReceiverRabbitmq[T]{
		input:             input,
		close:             close,
		heartBeatListener: heartBeatListener,
		m:                 m,
	}
	log.Debugf("ReceiverRabbitmq: '%s', '%s'", receiver.input.exchangeName, receiver.input.queueName)
	return receiver, nil
}

func CreateProducer[T, I any](m *MiddlewareRabbitmq[T], readExchangeName string) (*SenderChannel[I], error) {
	producerCountReqCh, err := m.Conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open a channel: %v", err)
	}
	log.Debugf("exchangeDeclare: '%s'", readExchangeName)
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
		return ReceiverChannel[I]{}, err
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
	return ReceiverChannel[I]{readExchangeName, inputQueue.Name, queueName == "", inputCh, &msgs}, nil
}

func (m *MiddlewareRabbitmq[T]) WriteTo(outputName string, subscribers []string) (Sender[T], error) {
	log.Infof("Creating WriteTo exchange '%s'", outputName)
	output, err := CreateProducer[T, T](m, outputName)
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err)
	}

	// This could go inside the routine
	// This is flawed as multiple writers could take the same ID.
	// heartBeatListener, err := createConsumer[T, HeartBeat](m, heartBeatName(outputName), "")
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to declare exchange %v", err)
	// }
	// knownProducers, knownProducerCount, err := getProducers(heartBeatListener)
	// if err != nil {
	// 	return nil, err
	// }
	// heartBeatListener.Close()

	// writerID := choseID(knownProducerCount, knownProducers)
	writerID := rand.UintN(uint(math.Pow(2, 32)))

	heartBeat, err := CreateProducer[T, HeartBeat](m, heartBeatName(outputName))
	if err != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err)
	}
	// end selection

	heart := make(chan struct{})
	producerCountReplierKill := make(chan struct{})
	task := func() {
		defer close(heart)
		defer heartBeat.Close()
		pulse := time.After(HEART_BEAT_INTERVAL)
		for {
			select {
			case <-pulse:
				pulse = time.After(HEART_BEAT_INTERVAL)
				ctx, cancel := context.WithTimeout(context.Background(), time.Duration(HEART_BEAT_INTERVAL))
				err = heartBeat.Publish(ctx, HeartBeat{uint(writerID)})
				cancel()
				if err != nil {
					log.Errorf("Error sending heartbeat: %s", err)
					return
				}
			case <-producerCountReplierKill:
				log.Infof("kill go routine for producer heart beat: '%s'", heartBeat.exchangeName)
				return
			}
		}
	}
	go task()

	sender := &SenderRabbitmq[T]{
		exchangeName: outputName,
		output:       *output,
		heart:        heart,
		killHeart:    &producerCountReplierKill,
	}
	return sender, nil
}

func getProducers(heartBeatListener ReceiverChannel[HeartBeat]) (map[uint]bool, uint, error) {
	timer := time.After(HEART_BEAT_INTERVAL + HEART_BEAT_RTT)
	knownProducers := make(map[uint]bool)
	count := uint(0)
	err := heartBeatListener.Purge()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to purge queue: %s", err)
	}
	for {
		select {
		case <-timer:
			return knownProducers, count, nil
		case msg := <-*heartBeatListener.C:
			envelope, ok, err := processMsg[HeartBeat](msg)
			if err != nil {
				log.Errorf("Error processing message: %s", err)
				continue
			}
			if !ok {
				log.Errorf("Invalid message type")
				continue
			}
			envelope.Ack(false)
			cID := envelope.Msg().ID
			if !knownProducers[cID] {
				knownProducers[cID] = true
				count++
			}
		}
	}
}

func choseID(knownProducerCount uint, knownProducersCache map[uint]bool) uint {
	idCandidate := uint(1)
	for range knownProducerCount + 1 {
		if !knownProducersCache[idCandidate] {
			break
		}
		idCandidate++
	}
	return idCandidate
}

func (s *SenderChannel[T]) Publish(ctx context.Context, msg T) error {
	buf, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("failed to marshal reply: %v", err)
	}
	// log.Debugf("PUBLISHING message '%s' in chan %s", string(buf), s.exchangeName)
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
	log.Debugf("PUBLISHED message in chan %s", s.exchangeName)
	return nil
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

	if !r.wereSomeProducers {
		log.Debugf("Waiting for producers")
		heartBeatListener, err := createConsumer[T, HeartBeat](r.m, heartBeatName(r.input.exchangeName), "")
		if err != nil {
			return nil, false, fmt.Errorf("failed to declare exchange %v", err)
		}
		select {
		case <-*heartBeatListener.C:
			r.wereSomeProducers = true
		case <-time.After(time.Minute):
			return nil, false, fmt.Errorf("No producers on sight")
		}

	}

	if r.noMoreProducers {
		return r.nextIfNotifedClosed(timeout)
	}

	var timeoutC <-chan time.Time = nil
	if timeout != nil {
		timeoutC = timeout.C
	}

	noMoreProducers, kill, err := r.monitorClose()
	if err != nil {
		return nil, false, err
	}
	defer close(kill)
	for {
		select {
		case msg, ok := <-*r.input.C:
			if !ok {
				return nil, false, fmt.Errorf("read channel was closed")
			}
			return processMsg[T](msg)

		case _, ok := <-noMoreProducers:
			return r.nextHandleCloseMsg(ok, timeout)
		case <-timeoutC:
			return nil, false, fmt.Errorf("timeout reached while waiting for message")
		}
	}

}

func (r *ReceiverRabbitmq[T]) nextHandleCloseMsg(ok bool, timeout *time.Timer) (Envelope[T], bool, error) {
	if !ok {
		return nil, false, fmt.Errorf("close channel was closed")
	}
	r.noMoreProducers = true
	newVar := time.Now()
	r.timeCloseNotificationArrived = &newVar
	return r.nextIfNotifedClosed(timeout)
}

func (r *ReceiverRabbitmq[T]) nextIfNotifedClosed(timeout *time.Timer) (Envelope[T], bool, error) {
	log.Infof("nextIfNotifedClosed")
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

// func (r *ReceiverRabbitmq[T]) nextIfNoInFlightMsgs() (Envelope[T], bool, error) {
// 	select {
// 	case msg, ok := <-*r.input.C:
// 		if !ok {
// 			return nil, false, fmt.Errorf("read channel was closed")
// 		}
// 		return processMsg[T](msg)
// 	case <-time.NewTimer(time.Millisecond * 500).C:
// 		r.asumeNoInFlightMsgs = true
// 		return nil, false, nil
// 	}
// }

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

func heartBeatName(readExchangeName string) string {
	return fmt.Sprintf("%s_hb", readExchangeName)
}

func (r *ReceiverRabbitmq[T]) CountProducers() (uint, error) {
	_, count, err := getProducers(r.heartBeatListener)
	return count, err
}

func (r *ReceiverRabbitmq[T]) monitorClose() (result <-chan bool, kill chan struct{}, err error) {
	res := make(chan bool)
	kill = make(chan struct{})
	go func() {
		for {
			timer := time.After(HEART_BEAT_INTERVAL + HEART_BEAT_RTT)
			select {
			case <-timer:
				res <- true
				return
			case msg := <-*r.heartBeatListener.C:
				envelope, ok, err := processMsg[HeartBeat](msg)
				if err != nil {
					log.Errorf("Error processing message: %s", err)
					continue
				}
				if !ok {
					log.Errorf("Invalid message type")
					continue
				}
				envelope.Ack(false)
			case <-kill:
				res <- false
				return
			}
		}
	}()
	return res, kill, nil
}
