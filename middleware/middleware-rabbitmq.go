package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"

	amqp "github.com/rabbitmq/amqp091-go"
)


type MiddlewareRabbitmq[T any] struct {
	Conn *amqp.Connection
}

type ReceiverRabbitmq[T any] struct {
	Ch *amqp.Channel
	readChan *<-chan amqp.Delivery
}

type EnvelopeRabbitmq[T any] struct {
	msg T
	tag *amqp.Delivery
}

type SenderRabbitmq[T any] struct {
	exchangeName string
	Ch *amqp.Channel
}

var log = logger.NewConsoleLogger("middleware rabbitmq", logger.Debug)

//sacar middleware
func NewMiddlewareRabbitmq[T any]() (MiddlewareCola[T], error) {
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
	
	middleware:= MiddlewareRabbitmq[T]{
		Conn: conn,
	};
	return &middleware, nil
}

func (m *MiddlewareRabbitmq[T]) Close() error {
	log.Infof("CLOSING")
	if m.Conn != nil {
		m.Conn.Close()
		m.Conn = nil
	}
	return nil
}
//group 
//wrapper
//close

func (m *MiddlewareRabbitmq[T]) CreateReadQueue(readExchangeName string, readQueueName string) (Receiver[T], error){
	inputQueue, ch, err1 := m.createQueue(readExchangeName, readQueueName)
	if err1 != nil {
		return nil, err1
	}
	msgs, err2 := ch.Consume(
		inputQueue.Name, // queue
		"",              // consumer
		false,           // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
		
	if err2 != nil {
		return nil, fmt.Errorf("failed to register a consumer %v", err2)
	}
	receiver := &ReceiverRabbitmq[T]{
		readChan: &msgs,
		Ch: ch,
	}
	return receiver, nil
}

func (m *MiddlewareRabbitmq[T]) CreateWriteQueue(writeExchangeName string) (Sender[T], error){
	ch, err2 := m.Conn.Channel()
	if err2 != nil {
		return nil, fmt.Errorf("failed to open a channel: %v", err2)
	}
	err3 := ch.ExchangeDeclare(
		writeExchangeName, // name
		"fanout",          // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)
	if err3 != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err3)
	}
	sender := &SenderRabbitmq[T]{
		exchangeName: writeExchangeName,
		Ch: ch,
	}
	return sender, nil
}

func (r *ReceiverRabbitmq[T]) Next(timeout *time.Timer) (Envelope[T], error) {
	if r.readChan == nil {
		return nil, fmt.Errorf("read channel is not initialized")
	}
	processMsg := func(msg amqp.Delivery) (Envelope[T], error) {
		var receivedMovie T
		err := json.Unmarshal(msg.Body, &receivedMovie)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal film: %v", err)
		}
		envelope := EnvelopeRabbitmq[T]{
			msg: receivedMovie,
			tag: &msg,
		}
		return &envelope, nil
	}

	if timeout == nil {
		msg, ok := <-*r.readChan
		if !ok {
			return nil, fmt.Errorf("read channel was closed")
		}
		return processMsg(msg)
	}

	select {
	case msg, ok := <-*r.readChan:
		if !ok {
			return nil, fmt.Errorf("read channel was closed")
		}
		return processMsg(msg)
	case <-timeout.C:
		return nil, fmt.Errorf("timeout reached while waiting for message")
	}
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
	buf, err1 := json.Marshal(*row)
	if err1 != nil {
		return fmt.Errorf("failed to marshal film: %v", err1)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err2 := s.Ch.PublishWithContext(ctx,
		s.exchangeName, // exchange
		"",                // routing key
		false,             // mandatory
		false,             // immediate
		amqp.Publishing{
			ContentType: "text/json",
			Body: buf,
		})
	if err2 != nil {
		return fmt.Errorf("failed to publish a message: %v in chan %s", err2, s.exchangeName)
	}
	log.Infof("PUBLISHEDDD message in chan %s", s.exchangeName)
	return nil
}


func (m *MiddlewareRabbitmq[T]) createQueue(exchangeName string, queueName string) (*amqp.Queue, *amqp.Channel, error){
	ch, err2 := m.Conn.Channel()
	if err2 != nil {
		return nil,nil, fmt.Errorf("failed to open a channel: %v", err2)
	}
	err1 := ch.ExchangeDeclare(
		exchangeName, // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err1 != nil {
		return nil,nil, fmt.Errorf("failed to declare exchange %v", err1)
	}

	queue, err2 := ch.QueueDeclare(
		queueName,    // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	
	if err2 != nil {
		return nil,nil, fmt.Errorf("failed to declare queue %v", err2)
	}

	err3 := ch.QueueBind(
		queue.Name, // queue name
		"",              // routing key
		exchangeName, // exchange
		false,
		nil,
	)
	if err3 != nil {
		return nil,nil, fmt.Errorf("failed to declare queue %v", err3)
	}
	return &queue, ch, nil
}