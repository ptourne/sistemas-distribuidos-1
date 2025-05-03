package rabbitmq

import (
	"bytes"
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

type middlewareRabbitmq[T codec.Serializable[T]] struct {
	Conn *amqp.Connection
}

type RabbitMQConnector struct {
	Conn *amqp.Connection
}

type ReceiverChannel[T codec.Serializable[T]] struct {
	exchangeName string
	queueName    string
	amqpCh       *amqp.Channel
	C            *<-chan amqp.Delivery
}

type receiverRabbitmq[T codec.Serializable[T]] struct {
	input         ReceiverChannel[T]
	closeReceiver ReceiverChannel[*CloseNotification]
	closeSender   SenderChannel[*CloseNotification]
	finishCids    map[string]struct {
		finishDonePending int
		msg               middleware.Envelope[T]
	}
	peers        int
	prefetch     int
	prefetchCids map[string]int
}

type CloseNotificationType uint8

const (
	closeNotificationFinishCid CloseNotificationType = iota
	closeNotificationFinishCidDone
)

type CloseNotification struct {
	notificationType CloseNotificationType
}

func (c CloseNotification) Encode() ([]byte, error) {
	return codec.Uint8Encode(uint8(c.notificationType))
}

func (c *CloseNotification) Decode(data []byte) (*CloseNotification, error) {
	r := bytes.NewReader(data)
	val, err := codec.Uint8Decode(r)
	if err != nil {
		return nil, err
	}
	return &CloseNotification{notificationType: CloseNotificationType(val)}, nil
}

func NewMiddleware[T codec.Serializable[T]](c *RabbitMQConnector) middleware.Connection[T] {
	return &middlewareRabbitmq[T]{Conn: c.Conn}
}

func (r *ReceiverChannel[T]) Close() {
	if r.amqpCh != nil {
		log.Debugf("Closing channel '%s', '%s'", r.exchangeName, r.queueName)
		r.amqpCh.Close()
		r.amqpCh = nil
	}
}

type EnvelopeRabbitmq[T codec.Serializable[T]] struct {
	msg          T
	tag          *amqp.Delivery
	finishesDone []struct {
		sender *SenderChannel[*CloseNotification]
		cid    string
	}
	cid string
	t   middleware.TypeMsg
}

type TypeMsgInternal int

const (
	normal TypeMsgInternal = iota
	eofCid
)

func (t TypeMsgInternal) String() string {
	switch t {
	case normal:
		return "normal"
	case eofCid:
		return "eofCid"
	default:
		return "Unknown TypeMsg"
	}
}

func FromStringTypeMsgInternal(s string) (TypeMsgInternal, error) {
	switch s {
	case "normal":
		return normal, nil
	case "eofCid":
		return eofCid, nil
	default:
		return -1, fmt.Errorf("unknown TypeMsg: %s", s)
	}
}

type SenderRabbitmq[T codec.Serializable[T]] struct {
	exchangeName string
	output       SenderChannel[T]
	isBlocked    atomic.Bool
	isClosed     atomic.Bool
}

type SenderChannel[T codec.Serializable[T]] struct {
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

type configuration struct {
	User     string
	Password string
	Host     string
	Port     uint16
}

func NewConfiguration(user, password, host string, port uint16) configuration {
	return configuration{
		User:     user,
		Password: password,
		Host:     host,
		Port:     port,
	}
}

func DefaultConfiguration() configuration {
	return NewConfiguration("guest", "guest", "rabbitmq", 5672)
}

func Url(c configuration) string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/", c.User, c.Password, c.Host, c.Port)
}

var log = logger.NewConsoleLogger("middleware", logger.Info)

func Connector() (*RabbitMQConnector, error) {
	return ConnectorCustom(DefaultConfiguration())
}
func ConnectorCustom(config configuration) (*RabbitMQConnector, error) {
	url := Url(config)
	conn, err := amqp.Dial(url)
	for range 5 {
		if err == nil {
			break
		}
		log.Errorf("Failed to connect to RabbitMQ: %v", err)
		time.Sleep(5 * time.Second)
		log.Infof("Retrying connection...")
		conn, err = amqp.Dial(url)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}
	return &RabbitMQConnector{Conn: conn}, nil
}

func (m *middlewareRabbitmq[T]) Close() error {
	log.Infof("CLOSING MIDDLEWARE")
	if m.Conn != nil {
		m.Conn.Close()
		m.Conn = nil
	}
	return nil
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

func (m *middlewareRabbitmq[T]) ConsumeFrom(sourceName string, groupName string, peers int, prefetch int) (middleware.Receiver[T], error) {
	log.Infof("Creating ConsumeFrom exchange '%s' with groupName '%s' ", sourceName, groupName)
	if sourceName == "" {
		return nil, fmt.Errorf("readExchangeName is empty, should be a valid name")
	}
	if groupName == "" {
		return nil, fmt.Errorf("groupQueueName is empty, should be a valid name")
	}
	return m.createReadQueueRK(sourceName, groupName, "fanout", "", peers, prefetch)
}

func (m *middlewareRabbitmq[T]) ConsumeFromRK(sourceName string, groupName string, t string, routingKey string, peers int, prefetch int) (middleware.Receiver[T], error) {
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
	input.amqpCh.Qos(prefetch, 0, false)

	closeReceiver, err := createConsumerRK[T, *CloseNotification](m, closeExchangeName(readExchangeName), "", "fanout", "")
	if err != nil {
		return nil, err
	}

	closeSender, err := CreateProducerRK[T, *CloseNotification](m, closeExchangeName(readExchangeName), "fanout")
	if err != nil {
		return nil, err
	}

	receiver := &receiverRabbitmq[T]{
		input:         input,
		closeReceiver: closeReceiver,
		closeSender:   closeSender,
		peers:         peers,
		prefetch:      prefetch,
		finishCids: make(map[string]struct {
			finishDonePending int
			msg               middleware.Envelope[T]
		}),
		prefetchCids: make(map[string]int),
	}

	return receiver, nil
}

func CreateProducerRK[T codec.Serializable[T], I codec.Serializable[I]](m *middlewareRabbitmq[T], readExchangeName string, t string) (SenderChannel[I], error) {
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

func createConsumerRK[T codec.Serializable[T], I codec.Serializable[I]](m *middlewareRabbitmq[T], readExchangeName string, queueName string, t string, routingKey string) (ReceiverChannel[I], error) {
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
		exchangeName: outputName,
		output:       output,
	}
	sender.isBlocked.Store(false)
	sender.isClosed.Store(false)
	return sender, nil
}

func (s *SenderChannel[T]) Publish(ctx context.Context, msg T, cid string) error {
	return s.PublishRK(ctx, msg, "", cid)
}

func (s *SenderChannel[T]) PublishRK(ctx context.Context, msg T, routingKey string, cid string) error {
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
			Headers: amqp.Table{
				"cid":  cid,
				"type": normal.String(),
			},
		})
	if err != nil {
		return fmt.Errorf("failed to publish a message: %v in chan %s", err, s.exchangeName)
	}
	log.Debugf("PUBLISHED message in chan %s msg:%v with type %v and cid %v", s.exchangeName, msg, normal.String(), cid)
	return nil
}

func (s *SenderRabbitmq[T]) SendEOF(cid string) error {
	return s.SendEOFRK("", cid)
}

func (s *SenderRabbitmq[T]) SendEOFRK(routingKey string, cid string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err := s.output.SendEOFRK(ctx, routingKey, cid)
	cancel()
	return err
}

func (s *SenderChannel[T]) SendEOFRK(ctx context.Context, routingKey string, cid string) error {
	err := s.ch.PublishWithContext(ctx,
		s.exchangeName, // exchange
		routingKey,     // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "application/message",
			Body:        []byte{},
			Headers: amqp.Table{
				"cid":  cid,
				"type": eofCid.String(),
			},
		})
	if err != nil {
		return fmt.Errorf("failed to publish a message: %v in chan %s", err, s.exchangeName)
	}
	log.Debugf("PUBLISHED message in chan %s with type %v and cid %v", s.exchangeName, eofCid.String(), cid)
	return nil
}

// Returns:
// - Envelope[T]: The next message from the input channel.
// - bool: True if the message was successfully processed, false otherwise.
// - error: An error if occurred during processing, nil otherwise.
//
// If timer triggers, the function returns an error: "timeout reached while waiting for message"
func (r *receiverRabbitmq[T]) Next(ctx context.Context) (middleware.Envelope[T], bool, error) {
	if r.input.amqpCh == nil {
		return nil, false, fmt.Errorf("read channel is not initialized")
	}

	var timeoutPrefetchCid <-chan time.Time = time.After(1 * time.Second)
	for {
		select {
		case msg, ok := <-*r.closeReceiver.C:
			shouldContinue, shouldReturn, e, err := r.handleFinishNotification(ok, msg)
			if shouldContinue {
				continue
			}
			if shouldReturn {
				return e, false, err
			}
		default:
			select {
			case msg, ok := <-*r.closeReceiver.C:
				shouldContinue, shouldReturn, e, err := r.handleFinishNotification(ok, msg)
				if shouldContinue {
					continue
				}
				if shouldReturn {
					return e, false, err
				}
			case msg, ok := <-*r.input.C:
				log.Debugf("Received message: %v, from input '%s'", msg.Body, r.input.exchangeName)
				if !ok {
					return nil, false, fmt.Errorf("read channel was closed")
				}
				t, cid, msgbody, tag, err := unpackMsg[T](msg)
				if err != nil {
					return nil, false, fmt.Errorf("failed to process close notification: %v", err)
				}

				finishesDone := make([]struct {
					sender *SenderChannel[*CloseNotification]
					cid    string
				}, 0)
				for cid := range r.prefetchCids {
					r.prefetchCids[cid]--
					if r.prefetchCids[cid] == 0 {
						log.Debugf("Cid prefetch emptied %s", cid)
						finishesDone = append(finishesDone, struct {
							sender *SenderChannel[*CloseNotification]
							cid    string
						}{
							&r.closeSender,
							cid,
						})
						delete(r.prefetchCids, cid)
					}
				}
				if t == eofCid {
					log.Debugf("Finish received for Cid %s", cid)
					r.finishCids[cid] = struct {
						finishDonePending int
						msg               middleware.Envelope[T]
					}{
						finishDonePending: r.peers + 1,
						msg: &EnvelopeRabbitmq[T]{
							msg: msgbody,
							tag: tag,
							finishesDone: make([]struct {
								sender *SenderChannel[*CloseNotification]
								cid    string
							}, 0),
							cid: cid,
							t:   middleware.EOF,
						},
					}
					r.closeSender.Publish(context.Background(), &CloseNotification{closeNotificationFinishCid}, cid)
					log.Debugf("Finish sent for Cid %s", cid)
					r.closeSender.Publish(context.Background(), &CloseNotification{closeNotificationFinishCidDone}, cid)
					for _, finishDone := range finishesDone {
						log.Debugf("Finish done sent for Cid %s", finishDone.cid)
						err = finishDone.sender.Publish(context.Background(), &CloseNotification{closeNotificationFinishCidDone}, finishDone.cid)
						if err != nil {
							return nil, false, fmt.Errorf("failed to ack message in close notification: %v", err)
						}
					}
					continue
				}
				return &EnvelopeRabbitmq[T]{
					msg:          msgbody,
					tag:          tag,
					finishesDone: finishesDone,
					cid:          cid,
					t:            middleware.Normal,
				}, true, nil

			case <-timeoutPrefetchCid:
				//todo eliminar los finishCids han llegado y mandarles el msg
				log.Debugf("Timeout prefetch cid")
				for CidMsg := range r.prefetchCids {
					log.Debugf("Finish done sent for Cid %s", CidMsg)
					err := r.closeSender.Publish(context.Background(), &CloseNotification{closeNotificationFinishCidDone}, CidMsg)
					if err != nil {
						return nil, false, fmt.Errorf("failed to ack message in close notification: %v", err)
					}
					delete(r.prefetchCids, CidMsg)
				}
				timeoutPrefetchCid = nil
				continue
			case <-ctx.Done():
				log.Debugf("Timeout reached while waiting for message")
				return nil, false, fmt.Errorf("timeout reached while waiting for message")
			}
		}
	}

}

func (r *receiverRabbitmq[T]) handleFinishNotification(ok bool, msg amqp.Delivery) (
	shouldContinue bool,
	shouldReturn bool,
	e middleware.Envelope[T],
	err error,
) {
	log.Debugf("Received message from close receiver '%s'", r.closeReceiver.exchangeName)
	if !ok {
		return false, true, nil, fmt.Errorf("read channel was closed")
	}
	_, cid, notification, tag, err := unpackMsg[*CloseNotification](msg)
	if err != nil {
		return false, true, nil, fmt.Errorf("failed to process close notification: %v", err)
	}
	defer tag.Ack(true)

	switch notification.notificationType {
	case closeNotificationFinishCidDone:
		log.Debugf("Finish done received for Cid %s", cid)
		finishCid, exists := r.finishCids[cid]
		if !exists {
			return true, false, nil, nil
		} else {
			finishCid.finishDonePending--
			r.finishCids[cid] = finishCid
			log.Debugf("Finish done pending for Cid %s: %d", cid, finishCid.finishDonePending)
			if finishCid.finishDonePending == 0 {
				log.Debugf("Finishes done received for Cid %s", cid)
				delete(r.finishCids, cid)
				return false, true, finishCid.msg, nil
			}
		}
	case closeNotificationFinishCid:
		log.Infof("Finish received for Cid %s", cid)
		_, exists := r.finishCids[cid]
		if exists {
			return true, false, nil, nil
		}
		r.prefetchCids[cid] = r.prefetch + PREFETCH_MAX
		log.Debugf("Finish received for Cid %s", cid)
	}
	return false, false, nil, nil
}

func unpackMsg[T codec.Serializable[T]](msg amqp.Delivery) (t TypeMsgInternal, cid string, received T, tag *amqp.Delivery, err error) {
	tag = &msg
	cidRaw, ok := msg.Headers["cid"]
	if !ok {
		return t, cid, received, tag, fmt.Errorf("cid missing from header")
	}

	cid, ok = cidRaw.(string)
	if !ok {
		return t, cid, received, tag, fmt.Errorf("cd is not a string: %T", cidRaw)
	}

	typeMessageRaw, ok := msg.Headers["type"]
	if !ok {
		return t, cid, received, tag, fmt.Errorf("type missing from header")
	}

	typeMessageInternal, err := FromStringTypeMsgInternal(typeMessageRaw.(string))
	if err != nil {
		return t, cid, received, tag, fmt.Errorf("failed to decode type message: %v", err)
	}

	if typeMessageInternal == normal {
		var nul T
		received, err = nul.Decode(msg.Body)
		if err != nil {
			return t, cid, received, tag, fmt.Errorf("failed to decode item: %v", err)
		}
	}

	// envelope := EnvelopeRabbitmq[T]{
	// 	msg: received,
	// 	tag: &msg,
	// 	finishesDone: []struct {
	// 		sender *SenderChannel[*CloseNotification]
	// 		cid    string
	// 	}{},
	// 	cid: cid,
	// 	t:   typeMessageInternal,
	// }
	return typeMessageInternal, cid, received, tag, nil
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
	for _, finishDone := range r.finishesDone {
		err = finishDone.sender.Publish(context.Background(), &CloseNotification{closeNotificationFinishCidDone}, finishDone.cid)
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

func (s *SenderRabbitmq[T]) Send(row T, cid string) error {
	return s.SendRK(row, "", cid)
}

func (s *SenderRabbitmq[T]) SendRK(row T, routingKey string, cid string) error {
	if s.exchangeName == "" {
		return fmt.Errorf("write exchange is not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err := s.output.PublishRK(ctx, row, routingKey, cid)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to publish a message: %v in chan %s", err, s.exchangeName)
	}
	cancel()
	return nil
}

func (m *middlewareRabbitmq[T]) createQueue(exchangeName string, groupName string) (*amqp.Queue, *amqp.Channel, error) {
	return m.createQueueRK(exchangeName, groupName, "fanout", "")
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
