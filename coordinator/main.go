package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"sync"
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

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		processCredits(conn)
	}()

	go func() {
		defer wg.Done()
		processMovies(conn)
	}()

	wg.Wait()
}

func processCredits(conn *amqp.Connection) {
	ch, err := conn.Channel()
	unwrap(err, "Failed to open a channel (credits)")
	defer ch.Close()

	err = ch.ExchangeDeclare("credits", "fanout", true, false, false, false, nil)
	unwrap(err, "Failed to declare exchange 'credits'")

	output := outputChannel("clean_credits", err, ch, log) // TODO: cambiar por el res final

	file, err := os.Open("/datasets/credits.csv")
	unwrap(err, "Failed to open credits.csv")
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read()
	unwrap(err, "Failed to read CSV header")
	credits_count := 0
	for {
		if credits_count%1000 == 0 {
			log.Infof("Processed %d lines from credits", credits_count)
		}
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(data) < 3 {
			log.Errorf("Invalid credits row: %v", err)
			continue
		}
		credit := common.Row{
			Strings: map[string]string{
				"ID":   data[2],
				"cast": data[0],
			},
		}
		buf, err := json.Marshal(credit)
		if err != nil {
			log.Errorf("Marshal error: %v", err)
			continue
		}
		err = ch.PublishWithContext(context.Background(), "credits", "", false, false, amqp.Publishing{
			ContentType: "text/json",
			Body:        buf,
		})
		if err != nil {
			log.Errorf("Failed to publish credit: %v", err)
		} else {
			credits_count++
		}
	}

	timer := time.NewTimer(time.Second * 10)
	should_loop := true
	credits_received := 0
	for should_loop {
		select {
		case msg := <-output:
			var receivedCredit common.Row // TODO: receivedActor, check results
			err := json.Unmarshal(msg.Body, &receivedCredit)
			if err != nil {
				log.Errorf("Failed to unmarshal credit: %v", err)
				continue
			}
			credits_received++
			//log.Infof("Received credit: %s %v", receivedCredit.Strings["ID"], receivedCredit.Arrays["cast"]) // TODO: change => actorName, count
			timer.Reset(time.Second * 5)
			break
		case <-timer.C:
			should_loop = false
			break
		}
	}
	timer.Stop()
	log.Infof("Processed %d credits, received %d credits", credits_count, credits_received) // Processed 45476 credits, received 45397 credits
	if credits_received != 45397 {
		log.Errorf("Not all credits received. Expected 45397, got %d", credits_received)
	}
	// TODO: check expected results
	newTimer := time.NewTimer(time.Second * 5)
	select {
	case <-newTimer.C:
		log.Infof("No extra credits received")
	case extraCredit := <-output:
		log.Errorf("Extra credit received: %+v", extraCredit)
	}
}

func processMovies(conn *amqp.Connection) {
	ch, err := conn.Channel()
	unwrap(err, "Failed to open a channel")
	defer ch.Close()

	output := outputChannel("filter_release_date_l_2010_and_include_es", err, ch, log)
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
	log.Debugf("Starting CSV processing (movies_metadata)")
	for {
		line++
		if line%1000 == 0 {
			log.Infof("Processed %d lines from movies_metadata", line)
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
	log.Debugf("CSV processing completed (movies_metadata)")

	expected_output := outputQueryOne()

	timer := time.NewTimer(time.Second * 10)
	should_loop := true
	for should_loop {
		select {
		case msg := <-output:
			var receivedMovie common.Row
			err := json.Unmarshal(msg.Body, &receivedMovie)
			if err != nil {
				log.Errorf("Failed to unmarshal film %v: %v", receivedMovie, err)
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

func outputQueryOne() []common.Row {
	return []common.Row{
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

func outputChannel(name string, err error, ch *amqp.Channel, log *logger.ConsoleLogger) <-chan amqp.Delivery {
	err = ch.ExchangeDeclare(
		name,     // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	unwrap(err, "Failed to declare exchange")

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
		name,            // exchange
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
