package middleware

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)


type MiddlewareRabbitmq struct {
	Conn *amqp.Connection
	Ch    *amqp.Channel
	readChan *<-chan amqp.Delivery
	writeExchangeName string

}

var log = logger.NewConsoleLogger("coordinator", logger.Debug)


func NewMiddlewareRabbitmq() (*MiddlewareRabbitmq, error) {
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
	ch, err2 := conn.Channel()
	if err2 != nil {
		return nil, fmt.Errorf("failed to open a channel: %v", err2)
	}
	middleware:= MiddlewareRabbitmq{
		Conn: conn,
		Ch:    ch,
		readChan: nil,
		writeExchangeName: "",
	};
	return &middleware, nil
}

func (m *MiddlewareRabbitmq) CreateReadWriteQueue(readExchangeName string, readQueueName string, writeExchangeName string) error{
	if readExchangeName != "" {
		inputQueue, err1 := m.createQueue(readExchangeName, readQueueName)
		if err1 != nil {
			return err1
		}

		msgs, err2 := m.Ch.Consume(
			inputQueue.Name, // queue
			"",              // consumer
			false,           // auto-ack
			false,           // exclusive
			false,           // no-local
			false,           // no-wait
			nil,             // args
		)
		if err2 != nil {
			return fmt.Errorf("failed to register a consumer %v", err2)
		}
		m.readChan = &msgs
	}

	if writeExchangeName != "" {
		err3 := m.Ch.ExchangeDeclare(
			writeExchangeName, // name
			"fanout",          // type
			true,              // durable
			false,             // auto-deleted
			false,             // internal
			false,             // no-wait
			nil,               // arguments
		)
		if err3 != nil {
			return fmt.Errorf("failed to declare exchange %v", err3)
		}
		m.writeExchangeName = writeExchangeName
	}
	return nil
}
func (m *MiddlewareRabbitmq) Read(timeout *time.Timer) (*common.Row, error){
	select {
	case msg := <-*m.readChan:
		var receivedMovie common.Row
		err := json.Unmarshal(msg.Body, &receivedMovie)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal film: %v", err)
		}
		return &receivedMovie, nil

	case <-timeout.C:
		return nil, fmt.Errorf("timeout reached while waiting for message")
	}
}

func (m *MiddlewareRabbitmq) Write(row *common.Row, ctx context.Context) error {
	buf, err1 := json.Marshal(*row)
	if err1 != nil {
		return fmt.Errorf("failed to marshal film: %v", err1)
	}
	err2 := m.Ch.PublishWithContext(ctx,
		m.writeExchangeName, // exchange
		"",                // routing key
		false,             // mandatory
		false,             // immediate
		amqp.Publishing{
			ContentType: "text/json",
			Body: buf,
		})
	if err2 != nil {
		return fmt.Errorf("failed to publish a message: %v", err2)
	}
	return nil
}


func (m *MiddlewareRabbitmq) createQueue(exchangeName string, queueName string) (*amqp.Queue, error){
	err1 := m.Ch.ExchangeDeclare(
		exchangeName, // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err1 != nil {
		return nil, fmt.Errorf("failed to declare exchange %v", err1)
	}

	queue, err2 := m.Ch.QueueDeclare(
		queueName,    // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	
	if err2 != nil {
		return nil, fmt.Errorf("failed to declare queue %v", err2)
	}

	err3 := m.Ch.QueueBind(
		queue.Name, // queue name
		"",              // routing key
		exchangeName, // exchange
		false,
		nil,
	)
	if err3 != nil {
		return nil, fmt.Errorf("failed to declare queue %v", err3)
	}
	return &queue, nil
}