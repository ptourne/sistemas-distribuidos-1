package rabbitmq

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	PREFETCH_MAX = 5
)

type middlewareRabbitmq[T codec.Serializable] struct {
	Conn *amqp.Connection
}

type RabbitMQConnector struct {
	Conn *amqp.Connection
}

type ReceiverChannel[T any] struct {
	exchangeName 	string
	queueName    	string
	amqpCh       	*amqp.Channel
	C            	*<-chan amqp.Delivery
}

type receiverRabbitmq[T codec.Serializable] struct {
	input           ReceiverChannel[T]
	closeReceiver   ReceiverChannel[CloseNotification]
	closeSender 	SenderChannel[CloseNotification]
	finishCids   	map[string]struct {
		finishesDoneReceived  int
		msg 				middleware.Envelope[T]
	}
	peers 			int
	prefetch 		int
	prefetchCids 	map[string]int
}

type CloseNotification struct{}

func (c CloseNotification) Encode() ([]byte, error) {
	return nil, nil
}

func (c CloseNotification) Decode(data []byte) error {
	return nil
}

func Connector() (*RabbitMQConnector, error) {
	conn, err := amqp.Dial("amqp://guest:guest@rabbitmq:5672/")
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}
	return &RabbitMQConnector{Conn: conn}, nil
}

func NewMiddleware[T codec.Serializable](c *RabbitMQConnector) middleware.Connection[T] {
	return &middlewareRabbitmq[T]{Conn: c.Conn}
}


func (r *ReceiverChannel[T]) Close() {
	if r.amqpCh != nil {
		log.Debugf("Clossing channel '%s', '%s'", r.exchangeName, r.queueName)
		r.amqpCh.Close()
		r.amqpCh = nil
	}
}

type EnvelopeRabbitmq[T codec.Serializable] struct {
	msg T
	tag *amqp.Delivery
	finishesDone []struct{ 
		sender *SenderChannel[CloseNotification] 
		cid string
	}
	cid string
	t middleware.TypeMsg
}

type SenderRabbitmq[T codec.Serializable] struct {
	exchangeName             string
	output                   SenderChannel[T]
	isBlocked                atomic.Bool
	isClosed                 atomic.Bool
}

type SenderChannel[T codec.Serializable] struct {
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

func NewRabbitmq[T codec.Serializable]() (middleware.Connection[T], error) {
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

	middleware := middlewareRabbitmq[T]{
		Conn: conn,
	}
	return &middleware, nil
}

func (m *middlewareRabbitmq[T]) Close() error {
	log.Infof("CLOSING MIDDLEWARE")
	if m.Conn != nil {
		m.Conn.Close()
		m.Conn = nil
	}
	return nil
}

func (m *receiverRabbitmq[T]) Qos(prefetchCount int, prefetchSize int) error {
	return m.input.amqpCh.Qos(prefetchCount, prefetchSize, false)
}

func (r *receiverRabbitmq[T]) Close() error {
	log.Debugf("CLOSING RECEIVER: '%s', '%s", r.input.exchangeName, r.input.queueName)
	r.input.Close()
	r.closeReceiver.Close()
	r.closeSender.Close()
	return nil
}

func (s *SenderRabbitmq[T]) Close() error {
	log.Debugf("CLOSING SENDER")
	s.output.Close()
	return nil
}

func (m *middlewareRabbitmq[T]) ConsumeFrom(sourceName string, groupName string, peers int,prefetch int) (middleware.Receiver[T], error) {
	log.Infof("Creating ConsumeFrom exchange '%s' with groupName '%s' ", sourceName, groupName)
	if sourceName == "" {
		return nil, fmt.Errorf("readExchangeName is empty, should be a valid name")
	}
	if groupName == "" {
		return nil, fmt.Errorf("groupQueueName is empty, should be a valid name")
	}
	return m.createReadQueueRK(sourceName, groupName, "fanout", "", peers, prefetch)
}

func (m *middlewareRabbitmq[T]) ConsumeFromRK(sourceName string, groupName string, t string, routingKey string, peers int,prefetch int) (middleware.Receiver[T], error) {
	log.Infof("Creating ConsumeFrom exchange '%s' with groupName '%s' ", sourceName, groupName)
	if sourceName == "" {
		return nil, fmt.Errorf("readExchangeName is empty, should be a valid name")
	}
	if groupName == "" {
		return nil, fmt.Errorf("groupQueueName is empty, should be a valid name")
	}
	return m.createReadQueueRK(sourceName, groupName, t, routingKey, peers, prefetch)
}

func (m *middlewareRabbitmq[T]) createReadQueueRK(readExchangeName string, queueName string, t string, routingKey string, peers int, prefetch int) (middleware.Receiver[T], error) {
	input, err := createConsumerRK[T, T](m, readExchangeName, queueName, t, routingKey)
	if err != nil {
		return nil, err
	}

	closeReceiver, err := createConsumerRK[T, CloseNotification](m, closeExchangeName(readExchangeName), "", "fanout", "")
	if err != nil {
		return nil, err
	}

	closeSender, err := CreateProducerRK[T, CloseNotification](m, closeExchangeName(readExchangeName), "fanout")
	if err != nil {
		return nil, err
	}

	receiver := &receiverRabbitmq[T]{
		input:             input,
		closeReceiver:     closeReceiver,
		closeSender:       closeSender,
		peers :            peers,
		prefetch:          prefetch,
	}

	return receiver, nil
}

func CreateProducerRK[T, I codec.Serializable](m *middlewareRabbitmq[T], readExchangeName string, t string) (SenderChannel[I], error) {
	ch, err := m.Conn.Channel()
	if err != nil {
		return SenderChannel[I]{}, fmt.Errorf("failed to open a channel: %v", err)
	}
	err = ch.ExchangeDeclare(
		readExchangeName, // name
		t,                // type
		true,             // durable
		false,            // auto-deleted
		false,            // internal
		false,            // no-wait
		nil,              // arguments
	)
	if err != nil {
		return SenderChannel[I]{}, err
	}
	newVar := SenderChannel[I]{readExchangeName, ch}
	return newVar, nil
}

func createConsumerRK[T, I codec.Serializable](m *middlewareRabbitmq[T], readExchangeName string, queueName string, t string, routingKey string) (ReceiverChannel[I], error) {
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

func (m *middlewareRabbitmq[T]) WriteTo(outputName string, subscribers []string) (middleware.Sender[T], error) {
	subscribersMap := make(map[string][]string)
	for _, sub := range subscribers {
		subscribersMap[sub] = []string{""}
	}
	return m.WriteToRK(outputName, subscribersMap, "fanout")
}

func (m *middlewareRabbitmq[T]) WriteToRK(outputName string, subscribers map[string][]string, t string) (middleware.Sender[T], error) {
	log.Infof("Creating WriteTo exchange '%s'", outputName)
	output, err := CreateProducerRK[T, T](m, outputName, t)
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

	sender := &SenderRabbitmq[T]{
		exchangeName:  outputName,
		output:        output,
	}
	sender.isBlocked.Store(false)
	sender.isClosed.Store(false)
	return sender, nil
}

func (s *SenderChannel[T]) Publish(ctx context.Context, msg T, cid string, t middleware.TypeMsg) error {
	return s.PublishRK(ctx, msg, "", cid, t)
}


func (s *SenderChannel[T]) PublishRK(ctx context.Context, msg T, routingKey string, cid string, t middleware.TypeMsg) error {
	buf, err := msg.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
	// log.Debugf("PUBLISHING message '%s' in chan %s", string(buf), s.exchangeName)
	err = s.ch.PublishWithContext(ctx,
		s.exchangeName, // exchange
		routingKey,     // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "application/message",
			Body:        buf,
			Headers:     amqp.Table{
				"cid": cid,
				"type": t,
			},
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
func (r *receiverRabbitmq[T]) Next(timeout *time.Timer) (middleware.Envelope[T], bool, error) {
	if r.input.amqpCh == nil {
		return nil, false, fmt.Errorf("read channel is not initialized")
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
			msgProcessEnvelope, ok, err := processMsg[T](msg)
			if !ok || err != nil {
				return nil, false, fmt.Errorf("failed to process close notification: %v", err)
			}
			for cid := range r.prefetchCids {
				r.prefetchCids[cid]--
				if r.prefetchCids[cid] == 0 {
					msgProcessEnvelope.finishesDone = append(msgProcessEnvelope.finishesDone, struct{
						sender *SenderChannel[CloseNotification]
						cid string
					} { 
						&r.closeSender,
						cid,
					})
				}
			}
			if msgProcessEnvelope.Type() == middleware.FinishCid {
				r.finishCids[msgProcessEnvelope.Cid()] = struct {
					finishesDoneReceived int
					msg                 middleware.Envelope[T]
				}{
					finishesDoneReceived: 0,
					msg: msgProcessEnvelope,
				}
				r.closeSender.Publish(context.Background(), CloseNotification{}, msgProcessEnvelope.Cid(), middleware.FinishCid)
			}
			return msgProcessEnvelope, true, nil


		case msg, ok := <-*r.closeReceiver.C:
			if !ok {
				return nil, false, fmt.Errorf("read channel was closed")
			}
			var msgProcessEnvelope middleware.Envelope[CloseNotification]
			var err error
			msgProcessEnvelope, ok, err = processMsg[CloseNotification](msg)
			if !ok || err != nil {
				return nil, false, fmt.Errorf("failed to process close notification: %v", err)
			}
			cid := msgProcessEnvelope.Cid()
			t := msgProcessEnvelope.Type()

			if t == middleware.FinishDone {
				_, exists := r.finishCids[cid]
				if !exists {
					continue
				} else {
					finishCid := r.finishCids[cid]
					finishCid.finishesDoneReceived = finishCid.finishesDoneReceived - 1
					r.finishCids[cid] = finishCid
					if finishCid.finishesDoneReceived == r.peers {
						log.Debugf("Finishes done received for Cid %s", cid)
						delete(r.finishCids, cid)
						return finishCid.msg, false, nil					
					}
				}
			} else if t == middleware.FinishCid { 
				log.Infof("Finish received for Cid %s", cid)
				_, exists := r.finishCids[cid]
				if exists {
					continue
				}
				r.prefetchCids[cid] = r.prefetch + PREFETCH_MAX
			} else {
				return nil, false, fmt.Errorf("unknown type %d", t)
			}
			
		case <-timeoutC:
			return nil, false, fmt.Errorf("timeout reached while waiting for message")
		}
	}

}

func processMsg[T codec.Serializable](msg amqp.Delivery) (*EnvelopeRabbitmq[T], bool, error) {
	var received T
	err := received.Decode(msg.Body)
	if err != nil {
		return nil, false, fmt.Errorf("failed to decode item: %v", err)
	}
	cidRaw, ok := msg.Headers["cid"]
	if !ok {
		return nil, false, fmt.Errorf("no existe cid")
	}

	cid, ok := cidRaw.(string)
	if !ok {
		return nil, false, fmt.Errorf("cid no es un string, es %T", cidRaw)
	}
	
	envelope := EnvelopeRabbitmq[T]{
		msg: received,
		tag: &msg,
		finishesDone: []struct {
			sender *SenderChannel[CloseNotification]
			cid string
		}{},
		cid: cid,
	}
	return &envelope, true, nil
}

func (r *EnvelopeRabbitmq[T]) Msg() T {
	return r.msg
}

func (r *EnvelopeRabbitmq[T]) Cid() string {
	return r.cid
}

func (r *EnvelopeRabbitmq[T]) Type() middleware.TypeMsg {
	return r.t
}

func (r *EnvelopeRabbitmq[T]) Ack(multiple bool) error {
	if r.tag == nil {
		return fmt.Errorf("tag is not initialized or already acked")
	}
	err := r.tag.Ack(multiple)
	if err != nil {
		return fmt.Errorf("failed to ack message: %v", err)
	}
	for _, finishDone := range r.finishesDone  {
		err = finishDone.sender.Publish(context.Background(), CloseNotification{}, finishDone.cid, middleware.FinishDone)
		if err != nil {
			return fmt.Errorf("failed to ack message in close notification: %v", err)
		}
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

func (s *SenderRabbitmq[T]) Send(row *T, cid string, t middleware.TypeMsg) error {
	return s.SendRK(row, "", cid, t)
}

func (s *SenderRabbitmq[T]) SendRK(row *T, routingKey string, cid string, t middleware.TypeMsg) error {
	if s.exchangeName == "" {
		return fmt.Errorf("write exchange is not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err := s.output.PublishRK(ctx, *row, routingKey, cid, t)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to publish a message: %v in chan %s", err, s.exchangeName)
	}
	cancel()
	return nil
}

func (m *middlewareRabbitmq[T]) createQueue(exchangeName string, groupName string) (*amqp.Queue, *amqp.Channel, error) {
	return m.createQueueRK(exchangeName, groupName, "fanaout", "")
}

func (m *middlewareRabbitmq[T]) createQueueRK(exchangeName string, groupName string, t string, routingKey string) (*amqp.Queue, *amqp.Channel, error) {
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
