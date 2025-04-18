package main

import (
	"encoding/csv"
	"io"
	"os"
	"sync"
	"time"

	"slices"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
)

const MIDDLEWARE = "rabbitmq"

var log = logger.NewConsoleLogger("coordinator", logger.Debug)

func main() {
	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		processRatings()
	}()

	// go func() {
	// 	defer wg.Done()
	// 	processCredits()
	// }()

	// go func() {
	// 	defer wg.Done()
	// 	processMovies()
	// }()

	wg.Wait()
}

func processCredits() {
	middlewareChan, err := middleware.NewRabbitmq[common.Row]()
	if err != nil {
		unwrap(err, "Failed to create middleware")
	}
	log.Infof("Connected to middleware: %s", MIDDLEWARE)
	defer middlewareChan.Close()

	nameReadQueue := "clean_credits" // TODO: change
	nameWriteQueue := "credits"

	receiver, err := middlewareChan.SuscribeTo(nameReadQueue)
	if err != nil {
		unwrap(err, "Failed to create read queue")
	}
	defer receiver.Close()

	sender, err := middlewareChan.WriteTo(nameWriteQueue)
	if err != nil {
		unwrap(err, "Failed to create write queue")
	}

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
		err = sender.Send(&credit)
		if err != nil {
			log.Errorf("Failed to send credit: %v", err)
		} else {
			credits_count++
		}

	}

	timer := time.NewTimer(time.Second * 10)
	credits_received := 0
	for {
		envelope, ok, err := receiver.Next(timer)
		if err != nil {
			if err.Error() == "timeout reached while waiting for message" {
				log.Infof("Timeout reached while waiting for message")
				break
			} else {
				log.Errorf("Failed to read message: %v", err)
				continue
			}
		}
		if !ok {
			log.Infof("No more credits")
			break
		}
		receivedCredit := envelope.Msg()
		credits_received++
		if credits_received%10000 == 0 {
			log.Infof("Processed %d credits", credits_received)
		}
		if receivedCredit.Strings["ID"] == "" {
			log.Debugf("Received empty credit: %v", receivedCredit)
			continue
		}
		err = envelope.Ack(true)
		unwrap(err, "Failed to ack message")
		timer.Reset(time.Second * 20)
	}
	timer.Stop()
	log.Infof("Processed %d credits, received %d credits", credits_count, credits_received) // Processed 45476 credits, received 45397 credits
	if credits_received != 45397 {
		log.Errorf("Not all credits received. Expected 45397, got %d", credits_received)
	}
	// TODO: check expected results
}

func processRatings() {
	middlewareChan, err := middleware.NewRabbitmq[common.Row]()
	if err != nil {
		unwrap(err, "Failed to create middleware")
	}
	log.Infof("Connected to middleware: %s", MIDDLEWARE)
	defer middlewareChan.Close()

	nameReadQueue := "clean_ratings" // TODO: change
	nameWriteQueue := "ratings"

	receiver, err := middlewareChan.SuscribeTo(nameReadQueue)
	if err != nil {
		unwrap(err, "Failed to create read queue")
	}
	defer receiver.Close()

	sender, err := middlewareChan.WriteTo(nameWriteQueue)
	if err != nil {
		unwrap(err, "Failed to create write queue")
	}
	file, err := os.Open("/datasets/ratings.csv")
	unwrap(err, "Failed to open ratings.csv")
	defer file.Close()

	reader := csv.NewReader(file)
	_, err = reader.Read()
	unwrap(err, "Failed to read CSV header")
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		cleanRatings(sender, reader, log)
	}()

	go func() {
		defer wg.Done()
		receiveRatings(receiver, log)
	}()
	wg.Wait()
}

func receiveRatings(receiver middleware.Receiver[common.Row], log *logger.ConsoleLogger) {
	timer := time.NewTimer(time.Hour * 1) // ToDo: change
	ratings_received := 0
	receiver.NotifyClose()
	for {
		if receiver.IsClosed() {
			middlewareChan, err := middleware.NewRabbitmq[common.Row]()
			if err != nil {
				unwrap(err, "Failed to create middleware")
			}
			log.Infof("Reconnected to middleware %s from ratings_consumer", MIDDLEWARE)
			defer middlewareChan.Close()

			nameReadQueue := "clean_ratings" // TODO: change

			receiver, err = middlewareChan.SuscribeTo(nameReadQueue)
			if err != nil {
				unwrap(err, "Failed to create read queue")
			}
			defer receiver.Close()
			receiver.NotifyClose()
		}
		envelope, ok, err := receiver.Next(timer)
		if err != nil {
			if err.Error() == "timeout reached while waiting for message" {
				log.Infof("Timeout reached while waiting for message")
				break
			} else {
				log.Errorf("Failed to read message: %v", err)
				continue
			}
		}
		if !ok {
			log.Infof("No more ratings")
			break
		}
		receivedRating := envelope.Msg()
		ratings_received++
		if ratings_received%100000 == 0 {
			log.Infof("Received %d ratings", ratings_received)
		}
		if receivedRating.Strings["movieID"] == "" {
			log.Debugf("Received empty rating: %v", receivedRating)
			continue
		}
		err = envelope.Ack(true)
		unwrap(err, "Failed to ack message")
		timer.Reset(time.Hour * 1)
	}
	timer.Stop()
	log.Infof("Finished receiving. Received %d ratings", ratings_received)
}

func cleanRatings(sender middleware.Sender[common.Row], reader *csv.Reader, log *logger.ConsoleLogger) {
	ratings_count := 0
	err := sender.LimitUnacked(1000)
	unwrap(err, "Failed to set QoS")

	sender.NotifyBlocked()
	sender.NotifyClose()

	for {
		if ratings_count%100000 == 0 {
			log.Infof("Processed %d lines from ratings", ratings_count)
		}
		// if ratings_count%100000 == 0 {
		// 	time.Sleep(1 * time.Second)
		// }
		data, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil || len(data) < 4 {
			log.Errorf("Invalid ratings row: %v", err)
			continue
		}
		rating := common.Row{
			Strings: map[string]string{
				"movieID": data[1],
				"rating":  data[2],
			},
		}
		attempt := 0

		for {
			if sender.IsBlocked() {
				log.Warnf("RabbitMQ está bloqueado, esperando desbloqueo...")
				time.Sleep(2 * time.Second)
				continue
			}
			err = sender.Send(&rating)

			if err == nil {
				break
			}
			if sender.IsClosed() {
				log.Warnf("Connection closed, trying to reconnect...")
				middlewareChan, err := middleware.NewRabbitmq[common.Row]()
				if err != nil {
					unwrap(err, "Failed to create middleware")
				} else {
					log.Infof("Reconnected to middleware %s from ratings_producer", MIDDLEWARE)
				}
				defer middlewareChan.Close()
				nameWriteQueue := "ratings"
				sender, err = middlewareChan.WriteTo(nameWriteQueue)
				if err != nil {
					unwrap(err, "Failed to create write queue")
				}
				err = sender.LimitUnacked(1000)
				unwrap(err, "Failed to set QoS")
				sender.NotifyBlocked()
				sender.NotifyClose()

			}

			time.Sleep(time.Duration(500*(1<<attempt)) * time.Millisecond)
			attempt++
		}
		ratings_count++
	}
	log.Infof("Processed %d ratings", ratings_count)
}

func processMovies() {
	middlewareChan, err := middleware.NewRabbitmq[common.Row]()
	if err != nil {
		unwrap(err, "Failed to create middleware")
	}
	log.Infof("Connected to middleware: %s", MIDDLEWARE)
	defer middlewareChan.Close()

	nameReadQueue := "filter_release_date_l_2010_and_include_es"
	nameWriteQueue := "movies_metadata"

	receiver, err := middlewareChan.SuscribeTo(nameReadQueue)
	if err != nil {
		unwrap(err, "Failed to create read queue")
	}
	defer receiver.Close()

	sender, err := middlewareChan.WriteTo(nameWriteQueue)
	if err != nil {
		unwrap(err, "Failed to create write queue")
	}

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

		sender.Send(&film)

	}
	log.Debugf("CSV processing completed (movies_metadata)")

	expected_output := outputQueryOne()

	timer := time.NewTimer(time.Second * 20)

	for {
		envelope, ok, err := receiver.Next(timer)
		if err != nil {
			if err.Error() == "timeout reached while waiting for message" {
				log.Infof("Timeout reached while waiting for message")
				break
			} else {
				log.Errorf("Failed to read message: %v", err)
				continue
			}
		}
		if !ok {
			log.Infof("No more films")
			break
		}
		receivedMovie := envelope.Msg()
		log.Infof("Received film: %s %v", receivedMovie.Strings["title"], receivedMovie.Arrays["genres"])
		log.Infof("Received film debug: %+v", receivedMovie)
		expected_output = remove(expected_output, receivedMovie)
		if len(expected_output) == 0 {
			log.Infof("All expected films received")
			break
		}
		err = envelope.Ack(true)
		unwrap(err, "Failed to ack message")
		timer.Reset(time.Second * 20)
	}
	timer.Stop()
	if len(expected_output) > 0 {
		log.Errorf("Not all expected films received. Missing %v", expected_output)
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
