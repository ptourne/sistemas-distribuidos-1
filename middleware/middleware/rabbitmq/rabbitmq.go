package rabbitmq

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strings"
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
	finishCids    map[uint64]struct {
		finishDoneIds     map[string]*amqp.Delivery
		finishDonePending uint
		msgEOF            *amqp.Delivery // this is the envelope that will be returned when all finishDoneIds have been received. It contains the original msg with the eof tag.
	} // this is used by the receiver that encounters the eof in the input channel. finishDonePending is decremented every time a peer receiver confirms it has pruned the cid from its prefetch. Then, msg is the original msg with the eof tag.
	pendingPrune  []middleware.Envelope[T] // client ids with a pending prune msg
	consumerCount uint
	prefetch      int
	// prefetchCids  map[string]int
	Log        *logger.ConsoleLogger
	routingKey string
}

type CloseNotificationType uint8

const (
	closeNotificationFinishCid CloseNotificationType = iota
	closeNotificationFinishCidDone
)

type CloseNotification struct {
	notificationType CloseNotificationType
	idWorker         string
}

func (c CloseNotification) Encode() ([]byte, error) {
	// return codec.Uint8Encode(uint8(c.notificationType))
	t, err := codec.Uint8Encode(uint8(c.notificationType))
	if err != nil {
		return nil, err
	}
	//encodeo el id
	idWorker, err := codec.StringEncode(c.idWorker)
	if err != nil {
		return nil, fmt.Errorf("failed to encode idWorker: %v", err)
	}
	data := make([]byte, 0)
	data = append(data, t...)
	data = append(data, idWorker...)
	return data, nil
}

func (c *CloseNotification) Decode(data []byte) (*CloseNotification, error) {
	r := bytes.NewReader(data)
	val, err := codec.Uint8Decode(r)
	if err != nil {
		return nil, err
	}
	idWorker, err := codec.StringDecode(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode idWorker: %v", err)
	}
	return &CloseNotification{notificationType: CloseNotificationType(val), idWorker: idWorker}, nil
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
	exchangeName  string
	output        SenderChannel[T]
	isBlocked     atomic.Bool
	isClosed      atomic.Bool
	Log           *logger.ConsoleLogger
	idSender      string
	consumerCount uint
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
	m.Log.Debugf("CLOSING MIDDLEWARE")
	if m.Conn != nil {
		m.Conn.Close()
		m.Conn = nil
	}
	return nil
}

func (r *receiverRabbitmq[T]) Close() error {
	r.Log.Debugf("CLOSING RECEIVER: '%s', '%s", r.input.exchangeName, r.input.queueName)
	r.input.Close()
	r.closeReceiver.Close()
	r.closeSender.Close()
	return nil
}

func (s *SenderRabbitmq[T]) Close() error {
	s.Log.Debugf("CLOSING SENDER: '%s'", s.exchangeName)
	s.output.Close()
	return nil
}

func (m *middlewareRabbitmq[T]) ConsumeFrom(sourceName string, groupName string, routingKey string, prefetch int, consumerCount uint) (middleware.Receiver[T], error) {
	t := "direct"
	return m.createReadQueueRK(sourceName, groupName, t, routingKey, uint(prefetch), consumerCount)
}

func (m *middlewareRabbitmq[T]) createReadQueueRK(readExchangeName string, queueName string, t string, routingKey string, prefetch uint, consumerCount uint) (middleware.Receiver[T], error) {

	input, err := createConsumerRK[T, T](m, readExchangeName, queueName, t, routingKey, prefetch)
	if err != nil {
		return nil, err
	}

	m.Log.Debugf("Creating close exchange '%s'", closeExchangeName(readExchangeName, queueName))

	closeReceiver, err := createConsumerRK[T, *CloseNotification](m, closeExchangeName(readExchangeName, queueName), "", "fanout", "", 100)
	if err != nil {
		return nil, err
	}

	closeSender, err := CreateProducerRK[T, *CloseNotification](m, closeExchangeName(readExchangeName, queueName), "fanout")
	if err != nil {
		return nil, err
	}

	var finishcCids map[uint64]struct {
		finishDoneIds     map[string]*amqp.Delivery
		finishDonePending uint
		msgEOF            *amqp.Delivery
	}

	if routingKey == "0" {
		finishcCids = make(map[uint64]struct {
			finishDoneIds     map[string]*amqp.Delivery
			finishDonePending uint
			msgEOF            *amqp.Delivery
		})
	} else {
		finishcCids = nil
	}

	receiver := &receiverRabbitmq[T]{
		input:         input,
		closeReceiver: closeReceiver,
		closeSender:   closeSender,
		consumerCount: consumerCount,
		prefetch:      int(prefetch),
		finishCids:    finishcCids,
		pendingPrune:  make([]middleware.Envelope[T], 0),
		// prefetchCids: make(map[string]int),
		Log:        m.Log,
		routingKey: routingKey,
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
	isCloseExchange := strings.Contains(readExchangeName, "close")
	durable := !isCloseExchange
	autoDeleted := isCloseExchange

	err = ch.ExchangeDeclare(
		readExchangeName, // name
		t,                // type
		durable,          // durable
		autoDeleted,      // auto-deleted
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
	m.Log.Debugf("PREFETCH %d with queuename: %s", prefetch, queueName)
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
	return ReceiverChannel[I]{readExchangeName, inputQueue.Name, inputCh, &msgs, m.Log}, nil
}

func (m *middlewareRabbitmq[T]) WriteTo(outputName string, subscribers []string, idWorker string, consumerCount uint) (middleware.Sender[T], error) {
	subscribersMap := make(map[string][]string)
	for _, sub := range subscribers {
		for i := 0; i < int(consumerCount); i++ {
			subscribersMap[sub] = append(subscribersMap[sub], fmt.Sprintf("%d", i))
		}
		m.Log.Debugf("subscribersMap[%s] = %v  for %s", sub, subscribersMap[sub], outputName)
	}
	return m.writeToRK(outputName, subscribersMap, "direct", idWorker, consumerCount)
}

func (m *middlewareRabbitmq[T]) writeToRK(outputName string, subscribers map[string][]string, t string, idWorker string, consumerCount uint) (middleware.Sender[T], error) {
	m.Log.Debugf("Creating WriteTo exchange '%s'", outputName)
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
		exchangeName:  outputName,
		output:        output,
		Log:           m.Log,
		idSender:      idWorker,
		consumerCount: consumerCount,
	}
	sender.isBlocked.Store(false)
	sender.isClosed.Store(false)
	return sender, nil
}

func (s *SenderChannel[T]) Publish(ctx context.Context, msg T, cid uint64, id uint64) error {
	// s.Log.Debugf("Publish msg %+v in %s", msg, s.exchangeName)
	return s.PublishRK(ctx, msg, "", cid, id)
}

func (s *SenderChannel[T]) PublishRK(ctx context.Context, msg T, routingKey string, cid uint64, id uint64) error {
	buf, err := msg.Encode()
	s.Log.Debugf("Publish msg %+v as %x and rk %s", msg, buf, routingKey)
	if err != nil {
		return fmt.Errorf("failed to encode message: %v", err)
	}
	bufId := make([]byte, 8)
	binary.BigEndian.PutUint64(bufId, id)
	cidC := int64(cid)
	err = s.ch.PublishWithContext(ctx,
		s.exchangeName, // exchange
		routingKey,     // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "application/message",
			Body:        buf,
			Headers: amqp.Table{
				"cid":  cidC,
				"type": normal.String(),
				"id":   bufId,
			},
		})
	if err != nil {
		return fmt.Errorf("failed to publish a message: %v in chan %s", err, s.exchangeName)
	}
	s.Log.Debugf("PUBLISHED message in chan %s and rk: %s, msg:%v with type %v and cid %v", s.exchangeName, routingKey, msg, normal.String(), cid)
	return nil
}

func (s *SenderRabbitmq[T]) SendEOFONE(cid uint64, rk string) error { //NO USAR SOLO TESTING
	err := s.SendEOFRK(rk, cid)
	if err != nil {
		return fmt.Errorf("failed to publish a message: %v in chan %s", err, s.exchangeName)
	}

	return nil
}

func (s *SenderRabbitmq[T]) SendEOF(cid uint64) error {
	for i := 0; i < int(s.consumerCount); i++ {
		rk := fmt.Sprintf("%d", i)
		err := s.SendEOFRK(rk, cid)
		if err != nil {
			return fmt.Errorf("failed to publish a message: %v in chan %s", err, s.exchangeName)
		}
	}
	return nil
}

func (s *SenderRabbitmq[T]) SendEOFRK(routingKey string, cid uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err := s.output.SendEOFRK(ctx, routingKey, cid)
	cancel()
	return err
}

func (s *SenderChannel[T]) ResendEOFToPeers(cid uint64) error {
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	bufId := make([]byte, 8)
	binary.BigEndian.PutUint64(bufId, uint64(0))
	cidC := int64(cid)

	err := s.ch.PublishWithContext(ctx,
		s.exchangeName, // exchange
		"",             // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "application/message",
			Body:        []byte{},
			Headers: amqp.Table{
				"cid":  cidC,
				"type": eofCid.String(),
				"id":   bufId,
			},
		})
	cancel()
	if err != nil {
		return fmt.Errorf("failed to publish a message: %v in chan %s", err, s.exchangeName)
	}
	return nil

}

func (s *SenderChannel[T]) SendEOFRK(ctx context.Context, routingKey string, cid uint64) error {
	s.Log.Debugf("SendEOFRK in chan %s with routingKey %s and cid %d", s.exchangeName, routingKey, cid)
	bufId := make([]byte, 8)
	binary.BigEndian.PutUint64(bufId, uint64(0))
	cidC := int64(cid)
	err := s.ch.PublishWithContext(ctx,
		s.exchangeName, // exchange
		routingKey,     // routing key
		false,          // mandatory
		false,          // immediate
		amqp.Publishing{
			ContentType: "application/message",
			Body:        []byte{},
			Headers: amqp.Table{
				"cid":  cidC,
				"type": eofCid.String(),
				"id":   bufId,
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

	for {
		if len(r.pendingPrune) > 0 {
			pendingPrune := r.pendingPrune[0]
			r.Log.Debugf("returning from pendingPrune: %+v", pendingPrune)
			r.pendingPrune = r.pendingPrune[1:]
			return pendingPrune, nil
		}

		select {
		case msg, ok := <-*r.closeReceiver.C:
			shouldContinue, shouldReturn, e, err := r.handleFinishNotification(ok, msg)
			if shouldContinue {
				continue
			}
			if shouldReturn {
				if err != nil {
					r.Log.Errorf("Error handling finish notification: %v", err)
					return nil, err
				}
				r.Log.Debugf("return %v envelope", e.Type())
				return e, nil
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
				t, cid, id, msgbody, tag, err := unpackMsg[T](msg)
				if err != nil {
					return nil, fmt.Errorf("failed to process close notification: %v", err)
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
					if r.routingKey == "0" {
						r.Log.Debugf("EOF received on channel for Cid %d in %s", cid, r.input.queueName)
						finishCid, exists := r.finishCids[cid]
						if !exists {
							r.Log.Debugf("EOF received for non-existent Cid %d in %s", cid, r.input.queueName)
							r.finishCids[cid] = struct {
								finishDoneIds     map[string]*amqp.Delivery
								finishDonePending uint
								msgEOF            *amqp.Delivery
							}{
								finishDoneIds:     make(map[string]*amqp.Delivery),
								finishDonePending: r.consumerCount,
								msgEOF:            tag,
							}
						} else {
							finishCid.msgEOF = tag
							r.finishCids[cid] = finishCid
							r.Log.Debugf("VAMOSS EOF received for Cid %d in %s", cid, r.input.queueName)
						}
						r.Log.Infof("LIDER eof received on channel for Cid %d in %s", cid, r.input.queueName)
						r.Log.Debugf("%s added finishCid[%d] = %+v", r.input.exchangeName, cid, r.finishCids[cid])

						err = r.closeSender.Prune(cid)
						if err != nil {
							return nil, fmt.Errorf("failed to send message in close notification: %v", err)
						}
						r.pendingPrune = append(r.pendingPrune, newPrune2Envelope[T](cid, r.closeSender, r.routingKey, nil, tag.Redelivered))

						r.Log.Debugf("return prune callback envelope")
						continue
					} else {
						r.Log.Infof("EOF received on channel YEII NOT LEADER for Cid %d in %s", cid, r.input.queueName)
						r.pendingPrune = append(r.pendingPrune, newPrune2Envelope[T](cid, r.closeSender, r.routingKey, tag, tag.Redelivered))
						continue
					}
				}
				r.Log.Debugf("return normal envelope")
				return newNormalEnvelope(cid, msgbody, tag, id), nil //TODO

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
	if !ok {
		return false, true, nil, fmt.Errorf("read channel was closed")
	}
	t, cid, _, notification, tag, err := unpackMsg[*CloseNotification](msg)
	if err != nil {
		r.Log.Errorf("failed to unpack close notification: %v", err)
		return false, true, nil, fmt.Errorf("failed to process close notification: %v", err)
	}

	if t == eofCid {
		if r.routingKey != "0" {
			r.Log.Infof("EOF received on channel YEII NOT LEADER for Cid %d in %s", cid, r.input.queueName)
			r.pendingPrune = append(r.pendingPrune, newPrune2Envelope[T](cid, r.closeSender, r.routingKey, tag, tag.Redelivered))
		} else {
			err = tag.Ack(false)
			if err != nil {
				r.Log.Errorf("failed to ack message in close notification: %v", err)
				return false, true, nil, fmt.Errorf("failed to ack message in close notification: %v", err)
			}
		}
		return true, false, nil, nil
	}

	if r.routingKey != "0" {
		defer tag.Ack(false)
		return true, false, nil, nil

	}
	if t == prune {
		r.Log.Debugf("IGNORINGGG prune msg in %s", r.input.queueName)
		if tag != nil {
			err = tag.Ack(false)
			if err != nil {
				r.Log.Errorf("failed to ack prune message: %v", err)
				return false, true, nil, fmt.Errorf("failed to ack prune message: %v", err)
			}
		}
		r.Log.Debugf("IGNORINGGG DONE")
		return true, false, nil, nil
	}

	// name := r.input.queueName

	switch notification.notificationType {
	case closeNotificationFinishCidDone:
		if r.routingKey == "0" {
			r.Log.Debugf("%s, Finish done received for Cid %d", r.input.exchangeName, cid)
			finishCid, exists := r.finishCids[cid]
			if !exists {
				finishCidNew := struct {
					finishDoneIds     map[string]*amqp.Delivery
					finishDonePending uint
					msgEOF            *amqp.Delivery
				}{
					finishDoneIds:     make(map[string]*amqp.Delivery),
					finishDonePending: r.consumerCount,
				}
				finishCidNew.finishDoneIds[notification.idWorker] = tag
				r.finishCids[cid] = finishCidNew
				r.Log.Debugf("Finish done received for non-existent Cid VAMOSS %d count: %d", cid, r.finishCids[cid].finishDonePending)
				return true, false, nil, nil
			} else {
				r.Log.Debugf("Finish done pending for Cid before %d: %+v | Receiver okk %s", cid, r.finishCids[cid].finishDoneIds, r.input.queueName)
				// finishCid.finishDonePending--
				// r.finishCids[cid] = finishCid
				// r.Log.Debugf("Finish done pending for Cid %d: %d in %s", cid, r.finishCids[cid].finishDonePending, r.input.queueName)
				_, exists := finishCid.finishDoneIds[notification.idWorker]
				if !exists {
					finishCid.finishDoneIds[notification.idWorker] = tag
				} else {
					tag.Ack(false)
				}
				if finishCid.finishDonePending == uint(len(finishCid.finishDoneIds)) {
					r.Log.Infof("Finishes done received for Cid %d in %s", cid, r.input.queueName)
					delete(r.finishCids, cid)
					return false, true, newEOFEnvelope[T](cid, finishCid.msgEOF, finishCid.finishDoneIds), nil
				}
				r.finishCids[cid] = finishCid
			}
		} else { //not leader
			return true, false, nil, nil
		}
	}
	return false, false, nil, nil
}

func unpackMsg[T codec.Serializable[T]](msg amqp.Delivery) (t TypeMsgInternal, cid uint64, id uint64, received T, tag *amqp.Delivery, err error) {
	tag = &msg
	cidRaw, ok := msg.Headers["cid"]
	if !ok {
		return t, cid, id, received, tag, fmt.Errorf("cid missing from header")
	}

	cidC, ok := cidRaw.(int64)
	if !ok {
		return t, cid, id, received, tag, fmt.Errorf("cid is not a int64: %T", cidRaw)
	}
	cid = uint64(cidC)

	idBuf, ok := msg.Headers["id"]
	if !ok {
		return t, cid, id, received, tag, fmt.Errorf("id missing from header")
	}

	id = binary.BigEndian.Uint64(idBuf.([]byte))

	typeMessageRaw, ok := msg.Headers["type"]
	if !ok {
		return t, cid, id, received, tag, fmt.Errorf("type missing from header")
	}

	typeMessageInternal, err := FromStringTypeMsgInternal(typeMessageRaw.(string))
	if err != nil {
		return t, cid, id, received, tag, fmt.Errorf("failed to decode type message: %v", err)
	}

	if typeMessageInternal == normal {
		var nul T
		received, err = nul.Decode(msg.Body)
		if err != nil {
			return t, cid, id, received, tag, fmt.Errorf("failed to decode item: %v", err)
		}
	}

	return typeMessageInternal, cid, id, received, tag, nil
}

func (s *SenderRabbitmq[T]) Send(row T, cid uint64, id uint64) error {
	lastRk := id % uint64(s.consumerCount)
	// s.Log.Infof("lastRk: %d", lastRk)
	rk := fmt.Sprintf("%d", lastRk)
	return s.sendRK(row, rk, cid, id)
}

func (s *SenderRabbitmq[T]) SendRK(row T, cid uint64, id uint64, routingKey string) error {
	if routingKey == "" {
		return fmt.Errorf("routing key cannot be empty")
	}
	return s.sendRK(row, routingKey, cid, id)
}

func (s *SenderRabbitmq[T]) sendRK(row T, routingKey string, cid uint64, id uint64) error {
	if s.exchangeName == "" {
		return fmt.Errorf("write exchange is not initialized")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	err := s.output.PublishRK(ctx, row, routingKey, cid, id)
	if err != nil {
		cancel()
		return fmt.Errorf("failed to publish a message: %v in chan %s", err, s.exchangeName)
	}
	cancel()
	return nil
}

func (s *SenderRabbitmq[T]) Prune(cid uint64) error {
	if s.exchangeName == "" {
		return fmt.Errorf("write exchange is not initialized")
	}
	err := s.output.Prune(cid)
	if err != nil {
		return fmt.Errorf("failed to prune a message: %v in chan %s", err, s.exchangeName)
	}
	return nil
}

func (s SenderChannel[T]) Prune(cid uint64) error {
	bufId := make([]byte, 8)
	binary.BigEndian.PutUint64(bufId, uint64(0))
	cidC := int64(cid)
	c, err := s.ch.PublishWithDeferredConfirm(s.exchangeName, "", false, false,
		amqp.Publishing{
			ContentType: "application/message",
			Body:        []byte{},
			Headers: amqp.Table{
				"cid":  cidC,
				"type": prune.String(),
				"id":   bufId,
			},
		})
	if err != nil {
		return fmt.Errorf("failed to send prune")
	}
	if ok := c.Wait(); !ok {
		return fmt.Errorf("failed to wait for prune reception")
	}
	return nil
}

func (m *middlewareRabbitmq[T]) createQueueRK(exchangeName string, groupName string, t string, routingKey string) (*amqp.Queue, *amqp.Channel, error) {
	ch, err := m.Conn.Channel()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to open a channel: %v", err)
	}
	isCloseQueue := groupName == ""
	durable := !isCloseQueue
	autoDeleted := isCloseQueue
	exclusive := isCloseQueue
	err = ch.ExchangeDeclare(
		exchangeName, // name
		t,            // type
		durable,      // durable
		autoDeleted,  // auto-deleted
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
		queueName,   // name
		durable,     // durable
		autoDeleted, // delete when unused
		exclusive,   // exclusive
		false,       // no-wait
		nil,         // arguments
	)

	if err != nil {
		return nil, nil, fmt.Errorf("failed to declare queue %v", err)
	}

	// m.Log.Debugf("ROUTING KEY: %s with quename %s", routingKey, queue.Name)
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

func closeExchangeName(readExchangeName string, queueName string) string {
	// if routingKey == "" {
	return fmt.Sprintf("%s->%s:close", readExchangeName, queueName)
	// }
	// return fmt.Sprintf("%s->%s[%s]:close", readExchangeName, queueName, routingKey)
}
