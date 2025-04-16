package cola

import (
	"encoding/json"
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	amqp "github.com/rabbitmq/amqp091-go"
)


type MiddlewareRabbitmq struct {
	connection *amqp.Connection
	ch    *amqp.Channel
	readChan *<-chan amqp.Delivery

}

func NewMiddlewareRabbitmq(connection *amqp.Connection, channel *amqp.Channel) *MiddlewareRabbitmq {
	middleware:= MiddlewareRabbitmq{
		connection: connection,
		ch:    channel,
		readChan: nil,
	};
	return &middleware
}

func (m *MiddlewareRabbitmq) createInputQueue(readExchangeName string, readQueueName string, writeExchangeName string) (error){
	if readExchangeName != "" {
		inputQueue, err1 := m.createQueue(readExchangeName, readQueueName)
		if err1 != nil {
			return err1
		}

		msgs, err2 := m.ch.Consume(
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
		err3 := m.ch.ExchangeDeclare(
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
	}
	return nil
}
func (m *MiddlewareRabbitmq) readInputQueue(inputExchangeName string, inputQueueName string, outputExchangeName string) (*common.Row, error){
	msg := <-*m.readChan
	var receivedMovie common.Row
	err := json.Unmarshal(msg.Body, &receivedMovie)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal film: %v", err)
	}
	return &receivedMovie, nil
}


func (m *MiddlewareRabbitmq) createQueue(exchangeName string, queueName string) (*amqp.Queue, error){
	err1 := m.ch.ExchangeDeclare(
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

	queue, err2 := m.ch.QueueDeclare(
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

	err3 := m.ch.QueueBind(
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