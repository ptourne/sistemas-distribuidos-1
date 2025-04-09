package main

import (
	"bytes"
	"context"
	"log"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/tutorials/3-pub-sub-var/common"
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

	spanish_and_argentinian_films := []common.Film{
		common.Film{
			Title:     "Camila",
			Countries: []string{"AR"},
		},
		common.Film{
			Title:     "Alcarràs",
			Countries: []string{"ES"},
		},
		common.Film{
			Title:     "La La Land",
			Countries: []string{"US"},
		},
		common.Film{
			Title:     "El secreto de sus ojos",
			Countries: []string{"AR"},
		},
		common.Film{
			Title:     "Relatos salvajes",
			Countries: []string{"AR", "ES"},
		},
		common.Film{
			Title:     "El buen patrón",
			Countries: []string{"ES"},
		},
		common.Film{
			Title:     "Mientras Dure la Guerra",
			Countries: []string{"AR", "ES"},
		},
		common.Film{
			Title:     "Maixabel",
			Countries: []string{"ES"},
		},
		common.Film{
			Title:     "Esperando la Carroza",
			Countries: []string{"AR"},
		},
		common.Film{
			Title:     "Tapas",
			Countries: []string{"AR", "ES"},
		},
	}

	for {
		for _, film := range spanish_and_argentinian_films {
			buf := new(bytes.Buffer)
			err := film.Encode(buf)
			common.FailOnError(err, "Failed to encode film")

			err = ch.PublishWithContext(ctx,
				"logs", // exchange
				"",     // routing key
				false,  // mandatory
				false,  // immediate
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        buf.Bytes(),
				})
			common.FailOnError(err, "Failed to publish a message")

			log.Printf(" [x] Sent %s", film)
			time.Sleep(1 * time.Second)
		}
	}


}
