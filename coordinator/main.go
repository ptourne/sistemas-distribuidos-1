package main

import (
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"time"

	"slices"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
)

const MIDDLEWARE = "rabbitmq"

var log = logger.NewConsoleLogger("coordinator", logger.Info)

func main() {
	middlewareChan, err := middleware.NewRabbitmq[common.Row]()
	if err != nil {
		unwrap(err, "Failed to create middleware")
	}
	middlewareChanByte, err := middleware.NewRabbitmq[[]byte]()
	if err != nil {
		unwrap(err, "Failed to create middleware")
	}
	log.Infof("Connected to middleware: %s", MIDDLEWARE)

	defer middlewareChan.Close()

	readFileByteQueue := "file_bytes"
	moviesMetadataName := "movies_metadata"
	creditsName := "credits"
	ratingsName := "ratings"
	q1Output := "filter_release_date_l_2010_and_include_es"
	q2Output := "reduce_top_5_by_budget"
	q3Output := "joiner_ratings"
	q4Output := "reduce_top_10_by_actor"
	q5Output := "filter_avg_rate"
	allQuerysToEndpointName :="all_querys_to_endpoint"

	receiverFileByte, err := middlewareChanByte.ConsumeFrom(readFileByteQueue, readFileByteQueue)
	if err != nil {
		unwrap(err, "Failed to create read queue")
	}
	defer receiverFileByte.Close()

	q1Receiver, err := middlewareChan.ConsumeFrom(q1Output, "q1")
	if err != nil {
		unwrap(err, "Failed to create read queue")
	}
	defer q1Receiver.Close()

	q2Receiver, err := middlewareChan.ConsumeFrom(q2Output, "q2")
	if err != nil {
		unwrap(err, "Failed to create read queue")
	}
	defer q2Receiver.Close()

	q3Receiver, err := middlewareChan.ConsumeFrom(q3Output, "q3")
	if err != nil {
		unwrap(err, "Failed to create read queue")
	}
	defer q3Receiver.Close()

	q4Receiver, err := middlewareChan.ConsumeFrom(q4Output, "q4")
	if err != nil {
		unwrap(err, "Failed to create read queue")
	}
	defer q4Receiver.Close()

	q5Receiver, err := middlewareChan.ConsumeFrom(q5Output, "q5")
	if err != nil {
		unwrap(err, "Failed to create read queue")
	}
	defer q5Receiver.Close()

	moviesMetadataSender, err := middlewareChan.WriteTo(moviesMetadataName, []string{"clean_movies"})
	if err != nil {
		unwrap(err, "Failed to create write queue")
	}

	creditsSender, err := middlewareChan.WriteTo(creditsName, []string{"clean_credits"})
	if err != nil {
		unwrap(err, "Failed to create write queue")
	}
	ratingsSender, err := middlewareChan.WriteTo(ratingsName, []string{"clean_ratings"})
	if err != nil {
		unwrap(err, "Failed to create write queue")
	}
	allQuerysToEndpointSender, err := middlewareChan.WriteTo(allQuerysToEndpointName, []string{allQuerysToEndpointName})
	if err != nil {
		unwrap(err, "Failed to create write queue")		
	}
	defer allQuerysToEndpointSender.Close()

	//lint:ignore S1019 Ignoring suggestion to simplify channel creation
	inputChannel := make(chan middleware.Envelope[[]byte], 0)

	go func() {
		for {
			envelope, ok, err := receiverFileByte.Next(nil)
			if err != nil {
				if err.Error() == "read channel was closed" {
					log.Infof("Channel closed: %v", readFileByteQueue)
					break
				}
				log.Errorf("Error reading from middleware: %v", err)
				continue
			}
			if !ok {
				log.Infof("Channel closed: %v", readFileByteQueue)
				break
			}
			inputChannel <- envelope
		}
		close(inputChannel)
	}()
OuterLoop:
	for {
		msgEnvelope := <-inputChannel
		msg := msgEnvelope.Msg()
		typeMsgRaw := binary.BigEndian.Uint32(msg[0:4])
		tipo := common.TypeMsg(typeMsgRaw)
		log.Infof("Received message type: %v", tipo)
		msgEnvelope.Ack(false)
		switch tipo {
		case common.FileName:
			fileName := string(msg[4:])
			var sender middleware.Sender[common.Row]
			var amount int
			var expectedLen int
			var create func([]string) common.Row
			switch fileName {
			case moviesMetadataName:
				log.Infof("Received file: %s", fileName)
				sender = moviesMetadataSender
				amount = 10000
				expectedLen = 24
				create = Film
			case creditsName:
				log.Infof("Received file: %s", fileName)
				sender = creditsSender
				amount = 10000
				expectedLen = 3
				create = Credit
			case ratingsName:
				log.Infof("Received file: %s", fileName)
				sender = ratingsSender
				amount = 1000000
				expectedLen = 3
				create = Rating
			default:
				panic(fmt.Sprintf("Unknown file name: %s", fileName))
			}

			connReader := &ConnReader{ch: inputChannel, lastReadNotIncluded: make([]byte, 0)}
			reader := csv.NewReader(connReader)

			d, err := reader.Read()
			log.Infof("Received header file: %v", d)
			unwrap(err, "Failed to read CSV header")
			line := 0
			log.Infof("Starting CSV processing")
			for {
				line++
				if line%amount == 0 {
					log.Infof("Processed %d lines from %s", line, fileName)
				}
				data, err := reader.Read()
				if err != nil {
					if err.Error() == "EOF" {
						log.Infof("Processed %d lines from %s", line, fileName)
						log.Infof("End of file reached")
						break
					}
					log.Errorf("Error reading CSV line: %v", err)
					// panic(fmt.Sprintf("Error reading CSV line: %v", err))
					continue
				}
				if len(data) < expectedLen {
					continue
				}

				row := create(data)

				sender.Send(&row)

			}
			log.Infof("CSV %s processing completed, closing", fileName)
			sender.Close()

		case common.AllFilesSent:
			log.Infof("Received ALL FILES SENT")
			break OuterLoop
		}
	}
	log.Infof("CSV processing completed")


	// expectedOutputQ1 := []common.Row{
	// 	{Strings: map[string]string{"title": "La Cienaga"}, Arrays: map[string][]string{"genres": []string{"Comedy", "Drama"}}},
	// 	{Strings: map[string]string{"title": "Burnt Money"}, Arrays: map[string][]string{"genres": []string{"Crime"}}},
	// 	{Strings: map[string]string{"title": "The City of No Limits"}, Arrays: map[string][]string{"genres": []string{"Thriller", "Drama"}}},
	// 	{Strings: map[string]string{"title": "Nicotina"}, Arrays: map[string][]string{"genres": []string{"Drama", "Action", "Comedy", "Thriller"}}},
	// 	{Strings: map[string]string{"title": "Lost Embrace"}, Arrays: map[string][]string{"genres": []string{"Drama", "Foreign"}}},
	// 	{Strings: map[string]string{"title": "Whisky"}, Arrays: map[string][]string{"genres": []string{"Comedy", "Drama", "Foreign"}}},
	// 	{Strings: map[string]string{"title": "The Holy Girl"}, Arrays: map[string][]string{"genres": []string{"Drama", "Foreign"}}},
	// 	{Strings: map[string]string{"title": "The Aura"}, Arrays: map[string][]string{"genres": []string{"Crime", "Drama", "Thriller"}}},
	// 	{Strings: map[string]string{"title": "Bombón: The Dog"}, Arrays: map[string][]string{"genres": []string{"Drama"}}},
	// 	{Strings: map[string]string{"title": "Rolling Family"}, Arrays: map[string][]string{"genres": []string{"Drama", "Comedy"}}},
	// 	{Strings: map[string]string{"title": "The Method"}, Arrays: map[string][]string{"genres": []string{"Drama", "Thriller"}}},
	// 	{Strings: map[string]string{"title": "Every Stewardess Goes to Heaven"}, Arrays: map[string][]string{"genres": []string{"Drama", "Romance", "Foreign"}}},
	// 	{Strings: map[string]string{"title": "Tetro"}, Arrays: map[string][]string{"genres": []string{"Drama", "Mystery"}}},
	// 	{Strings: map[string]string{"title": "The Secret in Their Eyes"}, Arrays: map[string][]string{"genres": []string{"Crime", "Drama", "Mystery", "Romance"}}},
	// 	{Strings: map[string]string{"title": "Liverpool"}, Arrays: map[string][]string{"genres": []string{"Drama"}}},
	// 	{Strings: map[string]string{"title": "The Headless Woman"}, Arrays: map[string][]string{"genres": []string{"Drama", "Mystery", "Thriller"}}},
	// 	{Strings: map[string]string{"title": "The Last Summer of La Boyita"}, Arrays: map[string][]string{"genres": []string{"Drama"}}},
	// 	{Strings: map[string]string{"title": "The Appeared"}, Arrays: map[string][]string{"genres": []string{"Horror", "Thriller", "Mystery"}}},
	// 	{Strings: map[string]string{"title": "The Fish Child"}, Arrays: map[string][]string{"genres": []string{"Drama", "Thriller", "Romance", "Foreign"}}},
	// 	{Strings: map[string]string{"title": "Cleopatra"}, Arrays: map[string][]string{"genres": []string{"Drama", "Comedy", "Foreign"}}},
	// 	{Strings: map[string]string{"title": "Roma"}, Arrays: map[string][]string{"genres": []string{"Drama", "Foreign"}}},
	// 	{Strings: map[string]string{"title": "Conversations with Mother"}, Arrays: map[string][]string{"genres": []string{"Comedy", "Drama", "Foreign"}}},
	// 	{Strings: map[string]string{"title": "The Education of Fairies"}, Arrays: map[string][]string{"genres": []string{"Drama"}}},
	// 	{Strings: map[string]string{"title": "The Good Life"}, Arrays: map[string][]string{"genres": []string{"Drama"}}},
	// }

	// timer := time.NewTimer(time.Second * 40)

	// log.Infof("Verifying Q1")
	// err = allQuerysToEndpointSender.Send(common.RowQueryName("Q1"))
	// if err != nil {
	// 	log.Errorf("Failed to send message: %v", err)
	// }
	// for {
	// 	envelope, ok, err := q1Receiver.Next(timer)
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
	// 		log.Infof("No more films")
	// 		break
	// 	}
	// 	receivedMovie := envelope.Msg()
	// 	err = allQuerysToEndpointSender.Send(common.RowQuery(receivedMovie))
	// 	if err != nil {
	// 		log.Errorf("Failed to send message: %v", err)
	// 		continue
	// 	}
	// 	// log.Infof("Received film: %s %v", receivedMovie.Strings["title"], receivedMovie.Arrays["genres"])
	// 	// log.Infof("Received film debug: %+v", receivedMovie)
	// 	expectedOutputQ1 = removeQ1(expectedOutputQ1, receivedMovie)
	// 	if len(expectedOutputQ1) == 0 {
	// 		log.Infof("All expected films received")
	// 		break
	// 	}
	// 	err = envelope.Ack(true)
	// 	unwrap(err, "Failed to ack message")
	// 	timer.Reset(time.Second * 20)
	// }
	// timer.Stop()
	// if len(expectedOutputQ1) > 0 {
	// 	log.Errorf("Not all expected films received. Missing %v", expectedOutputQ1)
	// }

	
	// log.Infof("Verifying Q2")
	// err = allQuerysToEndpointSender.Send(common.RowQueryName("Q2"))
	// if err != nil {
	// 	log.Errorf("Failed to send message: %v", err)
	// }
	// // Incorrect current answer
	// expectedOutputQ2 := []common.Row{
	// 	{Numerics: map[string]uint{"budget_sum": 120153886644}, Strings: map[string]string{"country": "US"}},
	// 	{Numerics: map[string]uint{"budget_sum": 2256831838}, Strings: map[string]string{"country": "FR"}},
	// 	{Numerics: map[string]uint{"budget_sum": 1611604610}, Strings: map[string]string{"country": "GB"}},
	// 	{Numerics: map[string]uint{"budget_sum": 1169682797}, Strings: map[string]string{"country": "IN"}},
	// 	{Numerics: map[string]uint{"budget_sum": 832585873}, Strings: map[string]string{"country": "JP"}},
	// }

	// timer = time.NewTimer(time.Second * 40)
	// for {
	// 	envelope, ok, err := q2Receiver.Next(nil)
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
	// 	err = allQuerysToEndpointSender.Send(common.RowQuery(receivedCountry))
	// 	if err != nil {
	// 		log.Errorf("Failed to send message: %v", err)
	// 		continue
	// 	}
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

	// log.Infof("Verifying Q4")
	// expectedOutputQ4 := []common.Row{
	// 	{Numerics: map[string]uint{"count": 17}, Strings: map[string]string{"actor": "Ricardo Darín"}},
	// 	{Numerics: map[string]uint{"count": 7}, Strings: map[string]string{"actor": "Alejandro Awada"}},
	// 	{Numerics: map[string]uint{"count": 7}, Strings: map[string]string{"actor": "Inés Efron"}},
	// 	{Numerics: map[string]uint{"count": 7}, Strings: map[string]string{"actor": "Leonardo Sbaraglia"}},
	// 	{Numerics: map[string]uint{"count": 7}, Strings: map[string]string{"actor": "Valeria Bertuccelli"}},
	// 	{Numerics: map[string]uint{"count": 6}, Strings: map[string]string{"actor": "Arturo Goetz"}},
	// 	{Numerics: map[string]uint{"count": 6}, Strings: map[string]string{"actor": "Diego Peretti"}},
	// 	{Numerics: map[string]uint{"count": 6}, Strings: map[string]string{"actor": "Pablo Echarri"}},
	// 	{Numerics: map[string]uint{"count": 6}, Strings: map[string]string{"actor": "Rafael Spregelburd"}},
	// 	{Numerics: map[string]uint{"count": 6}, Strings: map[string]string{"actor": "Rodrigo de la Serna"}},
	// }
	// countCredit := 0
	// timer = time.NewTimer(time.Second * 200)
	// for {
	// 	envelope, ok, err := q4Receiver.Next(timer)
	// 	if err != nil {
	// 		if err.Error() == "close channel was closed" {
	// 			log.Infof("Channel was closed")
	// 			break
	// 		}
	// 		if err.Error() == "timeout reached while waiting for message" {
	// 			log.Infof("Timeout reached while waiting for message")
	// 			break
	// 		} else {
	// 			log.Errorf("Failed to read message: %v", err)
	// 			continue
	// 		}
	// 	}
	// 	if !ok {
	// 		log.Infof("No more actors")
	// 		break
	// 	}
	// 	countCredit++
	// 	receivedActor := envelope.Msg()
	// 	log.Infof("Received country: %s %v", receivedActor.Strings["actor"], receivedActor.Numerics["count"])
	// 	log.Infof("Received country debug: %+v", receivedActor)
	// 	expectedOutputQ4 = removeQ2(expectedOutputQ4, receivedActor)
	// 	if len(expectedOutputQ4) == 0 {
	// 		log.Infof("All expected actors received")
	// 		break
	// 	}
	// 	err = envelope.Ack(true)
	// 	unwrap(err, "Failed to ack message")
	// 	timer.Reset(time.Second * 200)
	// }
	// timer.Stop()
	// if len(expectedOutputQ4) > 0 {
	// 	log.Errorf("Not all expected actors received. Missing %v", expectedOutputQ4)
	// }

	log.Infof("Verifying Q3")
	countRatings := 0
	timer := time.NewTimer(time.Minute * 1)
	for {
		envelope, ok, err := q3Receiver.Next(timer)
		if err != nil {
			if err.Error() == "close channel was closed" {
				log.Infof("Channel was closed")
				break
			}
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
		//log.Infof("Received film debug: %+v", receivedCredit)
		countRatings++
		log.Infof("Received %d ratings: %+v", countRatings, receivedRating)
		err = envelope.Ack(true)
		unwrap(err, "Failed to ack message")
		timer.Reset(time.Second * 40)
	}
	timer.Stop()
	log.Infof("Finished receiving ratings: received %d ratings", countRatings)
	// expected:
	// 10000 ratings => 5 res
	//	id: 16, avg: 3.6875
	// 	id: 1653, avg: 3.8214285714285716
	// 	id: 1956, avg: 4.25
	// 	id: 45722, avg: 3.5
	// 	id: 6636, avg: 5.0
	// 50000 ratings => 6 res
	// 	id: 16, avg: 3.787878787878788
	// 	id: 1653, avg: 3.73
	// 	id: 1956, avg: 3.75
	// 	id: 45722, avg: 3.2777777777777777
	// 	id: 6636, avg: 5.0
	// 100000 ratings => 7 res
	// 	id: 16, avg: 3.8732394366197185
	// 	id: 1653, avg: 3.7666666666666666
	// 	id: 1956, avg: 3.9038461538461537
	// 	id: 45722, avg: 3.3706896551724137
	// 	id: 48596, avg: 0.5
	// 	id: 6636, avg: 4.333333333333333
	// 	id: 69278, avg: 2.5

	// log.Infof("Verifying Q5")
	// for {
	// 	envelope, ok, err := q5Receiver.Next(timer)
	// 	if err != nil {
	// 		if err.Error() == "close channel was closed" {
	// 			log.Infof("Channel was closed")
	// 			break
	// 		}
	// 		if err.Error() == "timeout reached while waiting for message" {
	// 			log.Infof("Timeout reached while waiting for message")
	// 			break
	// 		} else {
	// 			log.Errorf("Failed to read message: %v", err)
	// 			continue
	// 		}
	// 	}
	// 	if !ok {
	// 		log.Infof("No more films")
	// 		break
	// 	}
	// 	receivedSentiment := envelope.Msg()
	// 	log.Infof("Received sentiment debug: %+v", receivedSentiment)

	// 	err = envelope.Ack(true)
	// 	unwrap(err, "Failed to ack message")
	// 	timer.Reset(time.Second * 40)
	// }
	// timer.Stop()
	// Expected:
	// NEGATIVE    5453.397595
	// POSITIVE    5668.650541
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

func Credit(data []string) common.Row {
	return common.Row{
		Strings: map[string]string{
			"ID":   data[2],
			"cast": data[0],
		},
	}
}

func Rating(data []string) common.Row {
	return common.Row{
		Strings: map[string]string{
			"movieID": data[1],
			"rating":  data[2],
		},
	}
}

func unwrap(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type ConnReader struct {
	ch                  chan middleware.Envelope[[]byte]
	lastReadNotIncluded []byte
}

func (cr *ConnReader) Read(buff []byte) (n int, err error) {
	capacity := cap(buff)
	cantCopyFromLast := min(capacity, len(cr.lastReadNotIncluded))
	copy(buff, cr.lastReadNotIncluded[:cantCopyFromLast])
	cr.lastReadNotIncluded = cr.lastReadNotIncluded[cantCopyFromLast:]
	remainingCapacity := capacity - cantCopyFromLast
	if remainingCapacity == 0 {
		return cantCopyFromLast, nil
	}

	msgEnvelope := <-cr.ch
	if msgEnvelope == nil {
		return 0, fmt.Errorf("invalid message es NIL")
	}
	msg := msgEnvelope.Msg()
	if len(msg) < 4 {
		return 0, fmt.Errorf("invalid message length")
	}
	typeMsgRaw := binary.BigEndian.Uint32(msg[0:4])
	tipo := common.TypeMsg(typeMsgRaw)
	data := msg[4:]
	log.Debugf("Received message data: %v", string(data))
	cantCopyFromData := min(remainingCapacity, len(data))
	switch tipo {
	case common.FileData:
		copy(buff[cantCopyFromLast:], data[:cantCopyFromData])
		cr.lastReadNotIncluded = append(cr.lastReadNotIncluded, data[cantCopyFromData:]...)
		n = cantCopyFromLast + cantCopyFromData
		err = nil
	case common.FinishFile:
		n = 0
		err = fmt.Errorf("EOF")
	default:
		n = 0
		err = fmt.Errorf("invalid message type: %v", tipo)
	}
	msgEnvelope.Ack(false)
	return n, err
}
