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
	Log  *logger.ConsoleLogger
}

type RabbitMQConnector struct {
	Conn *amqp.Connection
}

type ReceiverChannel[T codec.Serializable[T]] struct {
	exchangeName string
	queueName    string
	amqpCh       *amqp.Channel
	C            *<-chan amqp.Delivery
	Log          *logger.ConsoleLogger
}

type receiverRabbitmq[T codec.Serializable[T]] struct {
	input         ReceiverChannel[T]
	closeReceiver ReceiverChannel[*CloseNotification]
	closeSender   SenderChannel[*CloseNotification]
	finishCids    map[string]struct {
		finishDonePending uint
		msg               middleware.Envelope[T]
	} // this is used by the receiver that encounters the eof in the input channel. finishDonePending is decremented every time a peer receiver confirms it has pruned the cid from its prefetch. Then, msg is the original msg with the eof tag.
	pendingPrune  []middleware.Envelope[T] // client ids with a pending prune msg
	consumerCount uint
	prefetch      int
	prefetchCids  map[string]int
	Log           *logger.ConsoleLogger
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

func NewMiddleware[T codec.Serializable[T]](c *RabbitMQConnector, log *logger.ConsoleLogger) middleware.Connection[T] {
	return &middlewareRabbitmq[T]{Conn: c.Conn, Log: log}
}

func (r *ReceiverChannel[T]) Close() {
	if r.amqpCh != nil {
		r.Log.Debugf("Closing channel '%s', '%s'", r.exchangeName, r.queueName)
		r.amqpCh.Close()
		r.amqpCh = nil
	}
}

type TypeMsgInternal int

const (
	normal TypeMsgInternal = iota
	eofCid
	prune
)

func (t TypeMsgInternal) String() string {
	switch t {
	case normal:
		return "normal"
	case eofCid:
		return "eofCid"
	case prune:
		return "prune"
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
	case "prune":
		return prune, nil
	default:
		return -1, fmt.Errorf("unknown TypeMsg: %s", s)
	}
}

type SenderRabbitmq[T codec.Serializable[T]] struct {
	exchangeName string
	output       SenderChannel[T]
	isBlocked    atomic.Bool
	isClosed     atomic.Bool
	Log          *logger.ConsoleLogger
	lastId       uint64
	idSender     string
}

type Metadata struct {
	Acked bool
	Seqno uint64
}

type SenderChannel[T codec.Serializable[T]] struct {
	exchangeName string
	ch           *amqp.Channel
	Log          *logger.ConsoleLogger
}

func (s *SenderChannel[T]) Close() {
	if s.ch != nil {
		s.Log.Debugf("Closing channel '%s'", s.exchangeName)
		s.ch.Close()
		s.ch = nil
	}
}

type Configuration struct {
	User     string
	Password string
	Host     string
	Port     uint16
}

func NewConfiguration(user, password, host string, port uint16) Configuration {
	return Configuration{
		User:     user,
		Password: password,
		Host:     host,
		Port:     port,
	}
}

func DefaultConfiguration() Configuration {
	return NewConfiguration("guest", "guest", "rabbitmq", 5672)
}

func Url(c Configuration) string {
	return fmt.Sprintf("amqp://%s:%s@%s:%d/", c.User, c.Password, c.Host, c.Port)
}

func Connector() (*RabbitMQConnector, error) {
	return ConnectorCustom(DefaultConfiguration())
}
func ConnectorCustom(config Configuration) (*RabbitMQConnector, error) {
	url := Url(config)
	conn, err := amqp.Dial(url)
	for range 5 {
		if err == nil {
			break
		}
		time.Sleep(5 * time.Second)
		conn, err = amqp.Dial(url)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to RabbitMQ: %v", err)
	}
	return &RabbitMQConnector{Conn: conn}, nil
}

func (m *middlewareRabbitmq[T]) Close() error {
	m.Log.Infof("CLOSING MIDDLEWARE")
	if m.Conn != nil {
		m.Conn.Close()
		m.Conn = nil
	}
	return nil
}

func (r *receiverRabbitmq[T]) Close() error {
	r.Log.Infof("CLOSING RECEIVER: '%s', '%s", r.input.exchangeName, r.input.queueName)
	r.input.Close()
	r.closeReceiver.Close()
	r.closeSender.Close()
	return nil
}

func (s *SenderRabbitmq[T]) Close() error {
	s.Log.Infof("CLOSING SENDER: '%s'", s.exchangeName)
	s.output.Close()
	return nil
}

// func (m *middlewareRabbitmq[T]) ConsumeFrom(sourceName string, groupName string, consumerCount uint, prefetch int) (middleware.Receiver[T], error) {
// 	m.Log.Infof("Creating ConsumeFrom exchange '%s' with groupName '%s' and prefetch %d", sourceName, groupName, prefetch)
// 	if sourceName == "" {
// 		return nil, fmt.Errorf("readExchangeName is empty, should be a valid name")
// 	}
// 	if groupName == "" {
// 		return nil, fmt.Errorf("groupQueueName is empty, should be a valid name")
// 	}
// 	return m.createReadQueueRK(sourceName, groupName, "fanout", "", consumerCount, uint(prefetch))
// }

func (m *middlewareRabbitmq[T]) ConsumeFrom(sourceName string, groupName string, routingKey string, prefetch int) (middleware.Receiver[T], error) {
	t := "direct"
	return m.createReadQueueRK(sourceName, groupName, t, routingKey, uint(prefetch))
}

func (m *middlewareRabbitmq[T]) ConsumeFromRK(sourceName string, groupName string, t string, routingKey string, prefetch int) (middleware.Receiver[T], error) {
	m.Log.Infof("Creating ConsumeFrom exchange '%s' with groupName '%s' and prefetch %d", sourceName, groupName, prefetch)
	if sourceName == "" {
		return nil, fmt.Errorf("readExchangeName is empty, should be a valid name")
	}
	if groupName == "" {
		return nil, fmt.Errorf("groupQueueName is empty, should be a valid name")
	}
	return m.createReadQueueRK(sourceName, groupName, t, routingKey, uint(prefetch))
}

func (m *middlewareRabbitmq[T]) createReadQueueRK(readExchangeName string, queueName string, t string, routingKey string, prefetch uint) (middleware.Receiver[T], error) {

	input, err := createConsumerRK[T, T](m, readExchangeName, queueName, t, routingKey, prefetch)
	if err != nil {
		return nil, err
	}

	closeReceiver, err := createConsumerRK[T, *CloseNotification](m, closeExchangeName(readExchangeName, queueName, routingKey), "", "fanout", "", 100)
	if err != nil {
		return nil, err
	}

	closeSender, err := CreateProducerRK[T, *CloseNotification](m, closeExchangeName(readExchangeName, queueName, routingKey), "fanout")
	if err != nil {
		return nil, err
	}

	receiver := &receiverRabbitmq[T]{
		input:         input,
		closeReceiver: closeReceiver,
		closeSender:   closeSender,
		consumerCount: 1,
		prefetch:      int(prefetch),
		finishCids: make(map[string]struct {
			finishDonePending uint
			msg               middleware.Envelope[T]
		}),
		pendingPrune: make([]middleware.Envelope[T], 0),
		prefetchCids: make(map[string]int),
		Log:          m.Log,
	}

	return receiver, nil
}

func CreateProducerRK[T codec.Serializable[T], I codec.Serializable[I]](m *middlewareRabbitmq[T], readExchangeName string, t string) (SenderChannel[I], error) {
	ch, err := m.Conn.Channel()
	if err != nil {
		return SenderChannel[I]{}, fmt.Errorf("failed to open a channel: %v", err)
	}
	if err = ch.Confirm(false); err != nil {
		return SenderChannel[I]{}, fmt.Errorf("failed to enable publisher confirms: %v", err)
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
	newVar := SenderChannel[I]{
		exchangeName: readExchangeName,
		ch:           ch,
		Log:          m.Log,
	}
	return newVar, nil
}

func createConsumerRK[T codec.Serializable[T], I codec.Serializable[I]](m *middlewareRabbitmq[T], readExchangeName string, queueName string, t string, routingKey string, prefetch uint) (ReceiverChannel[I], error) {
	inputQueue, inputCh, err := m.createQueueRK(readExchangeName, queueName, t, routingKey)

	if err != nil {
		return ReceiverChannel[I]{}, nil
	}
	m.Log.Infof("PREFETCH %d with queuename: %s", prefetch, queueName)
	inputCh.Qos(int(prefetch), 0, false)

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
	return ReceiverChannel[I]{readExchangeName, queueName, inputCh, &msgs, m.Log}, nil
}

// func (m *middlewareRabbitmq[T]) WriteTo(outputName string, subscribers []string, idWorker string) (middleware.Sender[T], error) {
// 	subscribersMap := make(map[string][]string)
// 	for _, sub := range subscribers {
// 		subscribersMap[sub] = []string{""}
// 	}
// 	return m.writeToRK(outputName, subscribersMap, "fanout", idWorker)
// }

func (m *middlewareRabbitmq[T]) WriteTo(outputName string, subscribers []string, idWorker string) (middleware.Sender[T], error) {
	subscribersMap := make(map[string][]string)
	for _, sub := range subscribers {
		subscribersMap[sub] = []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9"}
	}
	return m.writeToRK(outputName, subscribersMap, "direct", idWorker)
}

func (m *middlewareRabbitmq[T]) writeToRK(outputName string, subscribers map[string][]string, t string, idWorker string) (middleware.Sender[T], error) {
	m.Log.Infof("Creating WriteTo exchange '%s'", outputName)
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
			ch.Close()
		}
	}

	sender := &SenderRabbitmq[T]{
		exchangeName: outputName,
		output:       output,
		Log:          m.Log,
		lastId:       0,
		idSender:     idWorker,
	}
	sender.isBlocked.Store(false)
	sender.isClosed.Store(false)
	return sender, nil
}

func (s *SenderChannel[T]) Publish(ctx context.Context, msg T, cid string) error {
	// s.Log.Debugf("Publish msg %+v in %s", msg, s.exchangeName)
	return s.PublishRK(ctx, msg, "", cid)
}

func (s *SenderChannel[T]) PublishRK(ctx context.Context, msg T, routingKey string, cid string) error {
	buf, err := msg.Encode()
	s.Log.Debugf("Publish msg %+v as %x", msg, buf)
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
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
	s.Log.Debugf("PUBLISHED message in chan %s and rk: %s, msg:%v with type %v and cid %v", s.exchangeName, routingKey, msg, normal.String(), cid)
	return nil
}

func (s *SenderRabbitmq[T]) SendEOF(cid string) error {
	rk := s.idSender
	return s.SendEOFRK(rk, cid)
}

func (s *SenderRabbitmq[T]) SendEOFAllID(cid string) error {
	for i := 0; i < 10; i++ {
		rk := fmt.Sprintf("%d", i)
		err := s.SendEOFRK(rk, cid)
		if err != nil {
			return fmt.Errorf("failed to publish a message: %v in chan %s", err, s.exchangeName)
		}
	}
	return nil
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
	s.Log.Debugf("PUBLISHED message in chan %s with type %v and cid %v", s.exchangeName, eofCid.String(), cid)
	return nil
}

// Returns:
// - Envelope[T]: The next message from the input channel.
// - bool: True if the message was successfully processed, false otherwise.
// - error: An error if occurred during processing, nil otherwise.
//
// If timer triggers, the function returns an error: "timeout reached while waiting for message"
func (r *receiverRabbitmq[T]) Next(ctx context.Context) (middleware.Envelope[T], error) {
	if r.input.amqpCh == nil {
		return nil, fmt.Errorf("read channel is not initialized")
	}
	var ctxDone <-chan struct{}
	if ctx != nil {
		ctxDone = ctx.Done()
	}

	var timeoutPrefetchCid <-chan time.Time
	skipResetTimer := false
	for {
		if len(r.pendingPrune) > 0 {
			pendingPrune := r.pendingPrune[0]
			r.Log.Debugf("returning from pendingPrune: %+v", pendingPrune)
			r.pendingPrune = r.pendingPrune[1:]
			return pendingPrune, nil
		}

		if !skipResetTimer {
			timeoutPrefetchCid = time.After(1 * time.Second)
		}
		skipResetTimer = false

		select {
		case msg, ok := <-*r.closeReceiver.C:
			shouldContinue, shouldReturn, e, err := r.handleFinishNotification(ok, msg)
			if shouldContinue {
				continue
			}
			if shouldReturn {
				r.Log.Debugf("return %v envelope", e.Type())
				return e, err
			}
		default:
			select {
			case msg, ok := <-*r.closeReceiver.C:
				shouldContinue, shouldReturn, e, err := r.handleFinishNotification(ok, msg)
				if shouldContinue {
					continue
				}
				if shouldReturn {
					return e, err
				}
			case msg, ok := <-*r.input.C:
				if !ok {
					return nil, fmt.Errorf("read channel was closed")
				}
				r.Log.Debugf("Received message from input in receiver %s", r.input.queueName)
				t, cid, msgbody, tag, err := unpackMsg[T](msg)
				if err != nil {
					return nil, fmt.Errorf("failed to process close notification: %v", err)
				}

				for prefetchCid := range r.prefetchCids {
					r.prefetchCids[prefetchCid]--
					if r.prefetchCids[prefetchCid] == 0 {
						r.Log.Debugf("Cid prefetch emptied %s", prefetchCid)
						r.pendingPrune = append(r.pendingPrune, newPrune2Envelope[T](prefetchCid, r.closeSender))
						delete(r.prefetchCids, prefetchCid)
					}
				}
				switch t {
				case prune:
					err = tag.Ack(false)
					if err != nil {
						r.Log.Errorf("failed to ack message in close notification: %v", err)
					}
					r.Log.Debugf("ignoring prune msg")
					continue
				case eofCid:
					// We set de listener for finishDones send on ack of prune msgs
					//r.Log.Infof("Consumer count: %d for channel %s", r.consumerCount, r.input.exchangeName)
					r.finishCids[cid] = struct {
						finishDonePending uint
						msg               middleware.Envelope[T]
					}{
						finishDonePending: r.consumerCount,
						msg:               newEOFEnvelope[T](cid),
					}
					r.Log.Infof("LIDER eof received on channel for Cid %s in %s", cid, r.input.queueName)
					r.Log.Debugf("%s added finishCid[%s] = %+v", r.input.exchangeName, cid, r.finishCids[cid])
					err := r.closeSender.Publish(context.Background(), &CloseNotification{closeNotificationFinishCid}, cid)
					if err != nil {
						return nil, fmt.Errorf("failed to ack message in close notification: %v", err)
					}
					err = r.closeSender.Prune(cid)
					if err != nil {
						return nil, fmt.Errorf("failed to send message in close notification: %v", err)
					}
					r.pendingPrune = append(r.pendingPrune, newPrune2Envelope[T](cid, r.closeSender))

					err = tag.Ack(false)
					if err != nil {
						r.Log.Errorf("failed to ack message in close notification: %v", err)
					}
					r.Log.Debugf("return prune callback envelope")
					continue

				}
				r.Log.Debugf("return normal envelope")
				return newNormalEnvelope(cid, msgbody, tag), nil

			case <-timeoutPrefetchCid:
				r.Log.Debugf("Timeout prefetch cid")
				r.Log.Debugf("r.prefetchCids: %v", r.prefetchCids)
				for CidMsg := range r.prefetchCids {
					r.Log.Debugf("return prune callback envelope for %s", CidMsg)
					r.pendingPrune = append(r.pendingPrune, newPrune2Envelope[T](CidMsg, r.closeSender))
					delete(r.prefetchCids, CidMsg)
				}
				timeoutPrefetchCid = nil
				skipResetTimer = true
				continue
			case <-ctxDone:
				r.Log.Debugf("Timeout reached while waiting for message")
				return nil, &middleware.TimeoutErr{}
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
	r.Log.Debugf("Received message from close receiver '%s'", r.closeReceiver.exchangeName)
	if !ok {
		return false, true, nil, fmt.Errorf("read channel was closed")
	}
	r.Log.Debugf("Received message from close receiver '%s'", r.closeReceiver.exchangeName)
	t, cid, notification, tag, err := unpackMsg[*CloseNotification](msg)
	if err != nil {
		return false, true, nil, fmt.Errorf("failed to process close notification: %v", err)
	}
	defer tag.Ack(true)
	if t == prune {
		return true, false, nil, nil
	}

	name := r.input.queueName

	switch notification.notificationType {
	case closeNotificationFinishCidDone:
		r.Log.Debugf("%s, Finish done received for Cid %s", r.input.exchangeName, cid)
		finishCid, exists := r.finishCids[cid]
		if !exists {
			r.Log.Debugf("Finish done received for non-existent Cid %s", cid)
			return true, false, nil, nil
		} else {
			r.Log.Debugf("Finish done pending for Cid before %s: %d | Receiver %s", cid, r.finishCids[cid].finishDonePending, r.input.queueName)
			finishCid.finishDonePending--
			r.finishCids[cid] = finishCid
			r.Log.Debugf("Finish done pending for Cid %s: %d in %s", cid, r.finishCids[cid].finishDonePending, r.input.queueName)
			if r.finishCids[cid].finishDonePending == 0 {
				r.Log.Infof("Finishes done received for Cid %s in %s", cid, r.input.queueName)
				delete(r.finishCids, cid)
				return false, true, finishCid.msg, nil
			}
		}
	case closeNotificationFinishCid:
		_, exists := r.finishCids[cid]
		if exists {
			return true, false, nil, nil
		}
		r.Log.Infof("Finish received for Cid %s in %s", cid, r.input.queueName)
		r.prefetchCids[cid] = r.prefetch + PREFETCH_MAX
		r.Log.Debugf("r.prefetchCids[%s] = %d | Receiver %s", cid, r.prefetchCids[cid], name)
		return true, false, nil, nil
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

func (s *SenderRabbitmq[T]) Send(row T, cid string) error {
	rk := s.idSender
	return s.SendRK(row, rk, cid)
}

func (s *SenderRabbitmq[T]) SendMsgID(row T, cid string) error {
	last := fmt.Sprintf("%d", s.lastId)
	rk := string(last[len(last)-1])
	return s.SendRK(row, rk, cid)
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

func (s *SenderRabbitmq[T]) Prune(cid string) error {
	if s.exchangeName == "" {
		return fmt.Errorf("write exchange is not initialized")
	}
	err := s.output.Prune(cid)
	if err != nil {
		return fmt.Errorf("failed to prune a message: %v in chan %s", err, s.exchangeName)
	}
	return nil
}

func (s SenderChannel[T]) Prune(cid string) error {
	c, err := s.ch.PublishWithDeferredConfirm(s.exchangeName, "", false, false,
		amqp.Publishing{
			ContentType: "application/message",
			Body:        []byte{},
			Headers: amqp.Table{
				"cid":  cid,
				"type": prune.String(),
			},
		})
	if err != nil {
		return fmt.Errorf("failed to send prune")
	}
	if ok := c.Wait(); !ok {
		return fmt.Errorf("Failed to wait for prune reception")
	}
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
		if routingKey == "" {
			queueName = fmt.Sprintf("%s->%s", exchangeName, groupName)
		} else {
			queueName = fmt.Sprintf("%s->%s[%s]", exchangeName, groupName, routingKey)
		}
	}
	m.Log.Debugf("createQueue: Creating queue '%s' for exchange '%s'", queueName, exchangeName)
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

func closeExchangeName(readExchangeName string, queueName string, routingKey string) string {
	if routingKey == "" {
		return fmt.Sprintf("%s->%s:close", readExchangeName, queueName)
	}
	return fmt.Sprintf("%s->%s[%s]:close", readExchangeName, queueName, routingKey)
}
