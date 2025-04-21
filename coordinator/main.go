package main

import (
	"encoding/csv"
	"io"
	"os"
	"time"

	"slices"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
)

const MIDDLEWARE = "rabbitmq"

var log = logger.NewConsoleLogger("coordinator", logger.Debug)

func main() {
	middlewareChan, err := middleware.NewRabbitmq[common.Row]()
	if err != nil {
		unwrap(err, "Failed to create middleware")
	}
	log.Infof("Connected to middleware: %s", MIDDLEWARE)

	defer middlewareChan.Close()

	q1Output := "filter_release_date_l_2010_and_include_es"
	// q2Output := "reduce_top_5_by_budget"
	nameWriteQueue := "movies_metadata"
	q1fOutput := "filter_one_production_country"

	q1Receiver, err := middlewareChan.ConsumeFrom(q1Output, "q1")
	if err != nil {
		unwrap(err, "Failed to create read queue")
	}
	defer q1Receiver.Close()

	q1fReceiver, err := middlewareChan.ConsumeFrom(q1fOutput, "q1f")
	if err != nil {
		unwrap(err, "Failed to create read queue")
	}
	defer q1fReceiver.Close()

	// q2Receiver, err := middlewareChan.ConsumeFrom(q2Output, "q2")
	// if err != nil {
	// 	unwrap(err, "Failed to create read queue")
	// }
	// defer q1Receiver.Close()

	sender, err := middlewareChan.WriteTo(nameWriteQueue, []string{"clean_movies"})
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

		sender.Send(&film)

	}
	sender.Close()
	log.Debugf("CSV processing completed")

	expectedOutputQ1 := []common.Row{
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

	timer := time.NewTimer(time.Second * 40)

	log.Infof("Verifying Q1")
	for {
		envelope, ok, err := q1Receiver.Next(timer)
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
		// log.Infof("Received film: %s %v", receivedMovie.Strings["title"], receivedMovie.Arrays["genres"])
		// log.Infof("Received film debug: %+v", receivedMovie)
		expectedOutputQ1 = removeQ1(expectedOutputQ1, receivedMovie)
		if len(expectedOutputQ1) == 0 {
			log.Infof("All expected films received")
			break
		}
		err = envelope.Ack(true)
		unwrap(err, "Failed to ack message")
		timer.Reset(time.Second * 20)
	}
	timer.Stop()
	if len(expectedOutputQ1) > 0 {
		log.Errorf("Not all expected films received. Missing %v", expectedOutputQ1)
	}


	timer2 := time.NewTimer(time.Second * 20)

	log.Infof("Verifying Q1F")
	cant := 0
	sum:= 0
	for {
		envelope, ok, err := q1fReceiver.Next(timer2)
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
			log.Infof("No more films IN")
			break
		}
		receivedMovie := envelope.Msg()
		log.Infof("Received film IN: %v", receivedMovie)
		cant++
		sum += int(receivedMovie.Numerics["budget"])
		err = envelope.Ack(true)
		unwrap(err, "Failed to ack message")
		timer2.Reset(time.Second * 20)
	}
	timer2.Stop()
	log.Infof("Received %d films IN", cant)
	log.Infof("Sum of budgets IN: %d", sum)

	// timer = time.NewTimer(time.Second * 40)

	// log.Infof("Verifying Q2f")

	// i := 0
	// for {
	// 	e, ok, err := q1fReceiver.Next(timer)
	// 	if err != nil {
	// 		if err.Error() == "timeout reached while waiting for message" {
	// 			log.Infof("Timeout reached while waiting for message")
	// 			break
	// 		} else {
	// 			log.Errorf("Failed to read message: %v", err)
	// 			continue
	// 		}
	// 	}
	// 	if !ok {
	// 		log.Infof("No more countries")
	// 		break
	// 	}
	// 	log.Infof("IN film %v", e.Msg())
	// 	i++
	// }
	// log.Infof("INDIANCOUNT:%d", i)
	// panic("")

	// Correct answer
	// expectedOutputQ2 := []common.Row{
	// 	{Strings: map[string]string{"country": "US"}, Numerics: map[string]uint{"budget_sum": 120153886644}},
	// 	{Strings: map[string]string{"country": "FR"}, Numerics: map[string]uint{"budget_sum": 2256831838}},
	// 	{Strings: map[string]string{"country": "GB"}, Numerics: map[string]uint{"budget_sum": 1611604610}},
	// 	{Strings: map[string]string{"country": "IN"}, Numerics: map[string]uint{"budget_sum": 1169682797}},
	// 	{Strings: map[string]string{"country": "JP"}, Numerics: map[string]uint{"budget_sum": 832585873}},
	// }

	// Incorrect current answer
	// expectedOutputQ2 := []common.Row{
	// 	{Numerics: map[string]uint{"budget_sum": 120139833644}, Strings: map[string]string{"country": "US"}},
	// 	{Numerics: map[string]uint{"budget_sum": 2249154073}, Strings: map[string]string{"country": "FR"}},
	// 	{Numerics: map[string]uint{"budget_sum": 1609754610}, Strings: map[string]string{"country": "GB"}},
	// 	{Numerics: map[string]uint{"budget_sum": 1165182787}, Strings: map[string]string{"country": "IN"}},
	// 	{Numerics: map[string]uint{"budget_sum": 832585873}, Strings: map[string]string{"country": "JP"}},
	// }


	// timer = time.NewTimer(time.Second * 400)

	// log.Infof("Verifying Q2")
	// for {
	// 	envelope, ok, err := q2Receiver.Next(timer)
	// 	if err != nil {
	// 		if err.Error() == "timeout reached while waiting for message" {
	// 			log.Infof("Timeout reached while waiting for message")
	// 			break
	// 		} else {
	// 			log.Errorf("Failed to read message: %v", err)
	// 			continue
	// 		}
	// 	}
	// 	if !ok {
	// 		log.Infof("No more countries")
	// 		break
	// 	}
	// 	receivedCountry := envelope.Msg()
	// 	log.Infof("Received country: %s %v", receivedCountry.Strings["country"], receivedCountry.Arrays["budget_sum"])
	// 	log.Infof("Received country debug: %+v", receivedCountry)
	// 	expectedOutputQ2 = removeQ2(expectedOutputQ2, receivedCountry)
	// 	if len(expectedOutputQ2) == 0 {
	// 		log.Infof("All expected films received")
	// 		break
	// 	}
	// 	err = envelope.Ack(true)
	// 	unwrap(err, "Failed to ack message")
	// 	timer.Reset(time.Second * 20)
	// }
	// timer.Stop()
	// if len(expectedOutputQ2) > 0 {
	// 	log.Errorf("Not all expected films received. Missing %v", expectedOutputQ2)
	// }
}

func removeQ1(slice []common.Row, movie common.Row) []common.Row {
	for i, v := range slice {
		if v.Strings["title"] == movie.Strings["title"] && stringSlicesEqual(v.Arrays["genres"], movie.Arrays["genres"]) {
			log.Infof("Film matched expected")
			return slices.Delete(slice, i, i+1)
		}
	}
	log.Errorf("Film not matched expected")
	return slice
}

func removeQ2(slice []common.Row, country common.Row) []common.Row {
	for i, v := range slice {
		if v.Strings["country"] == country.Strings["country"] {
			if v.Numerics["budget_sum"] == country.Numerics["budget_sum"] {
				log.Infof("Country matched expected 🟢")
			} else {
				log.Errorf("Budget sum not matched expected:😔 %d != %d", country.Numerics["budget_sum"], v.Numerics["budget_sum"])
			}
			return slices.Delete(slice, i, i+1)

		}
	}
	log.Errorf("Country not matched expected 🛑")
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
