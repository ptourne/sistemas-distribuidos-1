package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"os"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	log := logger.NewConsoleLogger("coordinator", logger.Debug)
	var conn *amqp.Connection
	log.Infof("Connecting to RabbitMQ")
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
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
		return
	}
	defer conn.Close()
	log.Infof("Connected to RabbitMQ")

	ch, err := conn.Channel()
	unwrap(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"movies_metadata", // name
		"fanout",          // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)
	unwrap(err, "Failed to declare an exchange")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	file, err := os.Open("/datasets/movies_metadata.csv")
	unwrap(err, "Failed to open CSV file")
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read()
	unwrap(err, "Failed to read CSV header")
	line := 0
	log.Debugf("Starting CSV processing")
	for {
		line++
		if line%1000 == 0 {
			log.Infof("Processed %d lines", line)
		}
		data, err := reader.Read()
		if err != nil {
			if err == io.EOF {
				break
			}
			log.Errorf("Error reading CSV line: %v", err)
			continue
		}
		if len(data) < 24 {
			continue
		}

		film := Film(data)

		buf, err := json.Marshal(film)
		if err != nil {
			log.Errorf("Failed to encode film: %v", err)
			continue
		}

		unwrap(err, "Failed to encode film")

		err = ch.PublishWithContext(ctx,
			"movies_metadata", // exchange
			"",                // routing key
			false,             // mandatory
			false,             // immediate
			amqp.Publishing{
				ContentType: "text/json",
				Body:        buf,
			})
		unwrap(err, "Failed to publish a message")

		// log.Debugf(" [x] Sent %s", film.Strings["title"])
		// time.Sleep(1 * time.Second)
	}
	log.Debugf("CSV processing completed")

	// filter_release_date_l_2010_and_include_es

	err = ch.ExchangeDeclare(
		"filter_release_date_l_2010_and_include_es", // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	unwrap(err, "Failed to declare exchange filter_release_date_l_2010_and_include_es")

	inputQueue, err := ch.QueueDeclare(
		"",    // name
		false, // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)

	unwrap(err, "Failed to declare a queue")

	err = ch.QueueBind(
		inputQueue.Name, // queue name
		"",              // routing key
		"filter_release_date_l_2010_and_include_es", // exchange
		false,
		nil,
	)
	unwrap(err, "Failed to bind a queue")

	msgs, err := ch.Consume(
		inputQueue.Name, // queue
		"",              // consumer
		false,           // auto-ack
		false,           // exclusive
		false,           // no-local
		false,           // no-wait
		nil,             // args
	)
	unwrap(err, "Failed to register a consumer")
	log.Debugf("Reading results")
	for msg := range msgs {
		var film common.Row
		err := json.Unmarshal(msg.Body, &film)
		if err != nil {
			log.Errorf("Failed to unmarshal film: %v", err)
			continue
		}
		log.Infof("Received film: %v %v", film.Strings["title"], film.Arrays["genres"])
	}
}

// adult,belongs_to_collection,budget,genres,homepage,id,imdb_id,original_language,
// original_title,overview,popularity,poster_path,production_companies,
// production_countries,
// release_date,revenue,runtime,spoken_languages,status,tagline,title,video,vote_average,vote_count

func Film(data []string) common.Row {
	budget, genres, id, overview, production_countries, release_date, revenue, title :=
		data[2], data[3], data[5], data[9], data[13], data[14], data[15], data[20]

	return common.Row{
		Strings: map[string]string{
			"movieID":              id,
			"title":                title,
			"overview":             overview,
			"production_countries": production_countries,
			"genres":               genres,
			"release_date":         release_date,
			"budget":               budget,
			"revenue":              revenue,
		},
	}
}

func unwrap(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
