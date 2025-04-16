package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"time"

	"slices"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logger.NewConsoleLogger("coordinator", logger.Debug)

func main() {
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

	output := outputChannel(err, ch, log)
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
	if err != nil {
		return
	}
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

	expected_output := []common.Row{
		{Strings: map[string]string{"title": "La Cienaga"}, Arrays: map[string][]string{"genres": []string{"Comedy", "Drama"}}},
		{Strings: map[string]string{"title": "Burnt Money"}, Arrays: map[string][]string{"genres": []string{"Crime"}}},
		{Strings: map[string]string{"title": "The City of No Limits"}, Arrays: map[string][]string{"genres": []string{"Thriller", "Drama"}}},
		{Strings: map[string]string{"title": "Nicotina"}, Arrays: map[string][]string{"genres": []string{"Drama", "Action", "Comedy", "Thriller"}}},
		{Strings: map[string]string{"title": "Lost Embrace"}, Arrays: map[string][]string{"genres": []string{"Drama", "Foreign"}}},
		{Strings: map[string]string{"title": "Whisky"}, Arrays: map[string][]string{"genres": []string{"Comedy", "Drama", "Foreign"}}},
		{Strings: map[string]string{"title": "The Holy Girl"}, Arrays: map[string][]string{"genres": []string{"Drama", "Foreign"}}},
		{Strings: map[string]string{"title": "The Aura"}, Arrays: map[string][]string{"genres": []string{"Crime", "Drama", "Thriller"}}},
		{Strings: map[string]string{"title": "Bombón: The Dog"}, Arrays: map[string][]string{"genres": []string{"Drama"}}},
		{Strings: map[string]string{"title": "Rolling Family"}, Arrays: map[string][]string{"genres": []string{"Drama", "Comedy"}}},
		{Strings: map[string]string{"title": "The Method"}, Arrays: map[string][]string{"genres": []string{"Drama", "Thriller"}}},
		{Strings: map[string]string{"title": "Every Stewardess Goes to Heaven"}, Arrays: map[string][]string{"genres": []string{"Drama", "Romance", "Foreign"}}},
		{Strings: map[string]string{"title": "Tetro"}, Arrays: map[string][]string{"genres": []string{"Drama", "Mystery"}}},
		{Strings: map[string]string{"title": "The Secret in Their Eyes"}, Arrays: map[string][]string{"genres": []string{"Crime", "Drama", "Mystery", "Romance"}}},
		{Strings: map[string]string{"title": "Liverpool"}, Arrays: map[string][]string{"genres": []string{"Drama"}}},
		{Strings: map[string]string{"title": "The Headless Woman"}, Arrays: map[string][]string{"genres": []string{"Drama", "Mystery", "Thriller"}}},
		{Strings: map[string]string{"title": "The Last Summer of La Boyita"}, Arrays: map[string][]string{"genres": []string{"Drama"}}},
		{Strings: map[string]string{"title": "The Appeared"}, Arrays: map[string][]string{"genres": []string{"Horror", "Thriller", "Mystery"}}},
		{Strings: map[string]string{"title": "The Fish Child"}, Arrays: map[string][]string{"genres": []string{"Drama", "Thriller", "Romance", "Foreign"}}},
		{Strings: map[string]string{"title": "Cleopatra"}, Arrays: map[string][]string{"genres": []string{"Drama", "Comedy", "Foreign"}}},
		{Strings: map[string]string{"title": "Roma"}, Arrays: map[string][]string{"genres": []string{"Drama", "Foreign"}}},
		{Strings: map[string]string{"title": "Conversations with Mother"}, Arrays: map[string][]string{"genres": []string{"Comedy", "Drama", "Foreign"}}},
		{Strings: map[string]string{"title": "The Education of Fairies"}, Arrays: map[string][]string{"genres": []string{"Drama"}}},
		{Strings: map[string]string{"title": "The Good Life"}, Arrays: map[string][]string{"genres": []string{"Drama"}}},
	}

	timer := time.NewTimer(time.Second * 10)
	should_loop := true
	for should_loop {
		select {
		case msg := <-output:
			var receivedMovie common.Row
			err := json.Unmarshal(msg.Body, &receivedMovie)
			if err != nil {
				log.Errorf("Failed to unmarshal film: %v", err)
				continue
			}
			log.Infof("Received film: %s %v", receivedMovie.Strings["title"], receivedMovie.Arrays["genres"])
			// log.Infof("Received film debug: %+v", receivedMovie)
			expected_output = remove(expected_output, receivedMovie)
			if len(expected_output) == 0 {
				log.Infof("All expected films received")
			}
			timer.Reset(time.Second * 5)
			break
		case <-timer.C:
			should_loop = false
			break
		}
	}
	timer.Stop()
	if len(expected_output) > 0 {
		log.Errorf("Not all expected films received. Missing %v", expected_output)
	}
	newTimer := time.NewTimer(time.Second * 5)
	select {
	case <-newTimer.C:
		log.Infof("No extra films received")
	case extraFilm := <-output:
		log.Errorf("Extra film received: %+v", extraFilm)
	}
}

func remove(slice []common.Row, movie common.Row) []common.Row {
	for i, v := range slice {
		if v.Strings["title"] == movie.Strings["title"] && stringSlicesEqual(v.Arrays["genres"], movie.Arrays["genres"]) {
			log.Infof("Film matched expected")
			return slices.Delete(slice, i, i+1)
		}
	}
	return slice
}

func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func outputChannel(err error, ch *amqp.Channel, log *logger.ConsoleLogger) <-chan amqp.Delivery {
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
	return msgs
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
