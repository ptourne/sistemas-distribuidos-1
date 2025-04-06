package main

import (
	"context"
	"log"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/tutorials/3-pub-sub/common"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	common.FailOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	common.FailOnError(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"logs",   // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	common.FailOnError(err, "Failed to declare an exchange")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	test_logs := []string{
		"log 1",
		"log 2",
		"log 3",
		"log 4",
		"log 5",
	}

	for {
		for _, line := range test_logs {
			err = ch.PublishWithContext(ctx,
				"logs", // exchange
				"",     // routing key
				false,  // mandatory
				false,  // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte(line),
				})
			common.FailOnError(err, "Failed to publish a message")

			log.Printf(" [x] Sent %s", line)
			time.Sleep(1 * time.Second)
		}
	}

}
