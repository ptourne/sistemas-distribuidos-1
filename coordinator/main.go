package main

import (
	"context"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"slices"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

type RatingB struct {
	Id     uint32
	Rating uint8
}

func (r *RatingB) Encode() []byte {
	buf := make([]byte, 5)
	binary.BigEndian.PutUint32(buf, r.Id)
	buf[4] = r.Rating
	return buf
}

func (r *RatingB) Decode(data []byte) {
	if len(data) < 5 {
		return
	}
	r.Id = binary.BigEndian.Uint32(data[:4])
	r.Rating = data[4]
}

type ConfigCoordinator struct {
	CoordinatorsCant          int
	CoordinatorPrefetch       int
	MiddlewareChan            *middleware.Connection[*model.Row]
	MiddlewareChanByte        *middleware.Connection[*model.FileChunk]
	MiddlewareChanPackageByte *middleware.Connection[*common.PackageFile]
	ReadFileByteQueue         string
	MoviesMetadataName        string
	CreditsName               string
	RatingsName               string
	Q1Output                  string
	Q2Output                  string
	Q3Output                  string
	Q4Output                  string
	Q5Output                  string
	AllQuerysToEndpointName   string
}

func (c *ConfigCoordinator) Close() {
	if c.MiddlewareChan != nil {
		(*c.MiddlewareChan).Close()
	}
	if c.MiddlewareChanByte != nil {
		(*c.MiddlewareChanByte).Close()
	}
	if c.MiddlewareChanPackageByte != nil {
		(*c.MiddlewareChanPackageByte).Close()
	}

}

func main() {
	var log = logger.NewConsoleLogger("coordinator", logger.Info)
	connector, err := rabbitmq.Connector()
	if err != nil {
		log.Errorf("Failed to connect middleware: %v", err)
		return
	}
	config := ConfigCoordinator{}
	middlewareChan := rabbitmq.NewMiddleware[*model.Row](connector)
	middlewareChanPackageByte := rabbitmq.NewMiddleware[*common.PackageFile](connector)
	middlewareChanByte := rabbitmq.NewMiddleware[*model.FileChunk](connector)
	config.MiddlewareChan = &middlewareChan
	config.MiddlewareChanByte = &middlewareChanByte
	config.MiddlewareChanPackageByte = &middlewareChanPackageByte
	log.Infof("Connected to middleware")

	defer config.Close()

	config.ReadFileByteQueue = "file_bytes"
	config.MoviesMetadataName = "movies_metadata"
	config.CreditsName = "credits"
	config.RatingsName = "ratings"
	config.Q1Output = "filter_release_date_l_2010_and_include_es"
	config.Q2Output = "reduce_top_5_by_budget"
	config.Q3Output = "reduce_top_bottom_avg_rating"
	config.Q4Output = "reduce_top_10_by_actor"
	config.Q5Output = "filter_avg_rate"
	config.AllQuerysToEndpointName = "all_querys_to_endpoint"

	config.CoordinatorsCant = 1
	config.CoordinatorPrefetch = 1

	receiverFileByte, err := middlewareChanPackageByte.ConsumeFrom(config.ReadFileByteQueue, config.ReadFileByteQueue, config.CoordinatorsCant, config.CoordinatorPrefetch)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	defer receiverFileByte.Close()
	wg := sync.WaitGroup{}
	inputsChannelMap := map[string]chan middleware.Envelope[*common.PackageFile]{}
	for {
		ctx := context.Background()
		envelope, ok, err := receiverFileByte.Next(ctx)
		if err != nil {
			if err.Error() == "read channel was closed" {
				log.Infof("Channel closed: %v", config.ReadFileByteQueue)
				break
			}
			log.Errorf("Error reading from middleware: %v", err)
			continue
		}
		if !ok {
			log.Infof("Channel closed: %v", config.ReadFileByteQueue)
			break
		}
		cid := envelope.Cid()
		inputCidChannel, exists := inputsChannelMap[cid]
		if !exists {
			//lint:ignore S1019 Ignoring suggestion to simplify channel creation
			inputCidChannel = make(chan middleware.Envelope[*common.PackageFile], 0)
			inputsChannelMap[cid] = inputCidChannel
			wg.Add(1)
			go handleClient(cid, inputCidChannel, config)
		}
		// log.Infof("Received message from channel: %s", cid)
		inputCidChannel <- envelope
	}

	for _, channelCid := range inputsChannelMap {
		close(channelCid)
	}

	wg.Wait()
}

func handleClient(cid string, inputChannel chan middleware.Envelope[*common.PackageFile], c ConfigCoordinator) {
	var log = logger.NewConsoleLogger(fmt.Sprintf("coordinator-%s", cid), logger.Info)
	q1Receiver, err := (*c.MiddlewareChan).ConsumeFrom(c.Q1Output, "q1", c.CoordinatorsCant, c.CoordinatorPrefetch)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	defer q1Receiver.Close()

	q2Receiver, err := (*c.MiddlewareChan).ConsumeFrom(c.Q2Output, "q2", c.CoordinatorsCant, c.CoordinatorPrefetch)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	defer q2Receiver.Close()

	q3Receiver, err := (*c.MiddlewareChan).ConsumeFrom(c.Q3Output, "q3", c.CoordinatorsCant, c.CoordinatorPrefetch)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	defer q3Receiver.Close()

	q4Receiver, err := (*c.MiddlewareChan).ConsumeFrom(c.Q4Output, "q4", c.CoordinatorsCant, c.CoordinatorPrefetch)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	defer q4Receiver.Close()

	q5Receiver, err := (*c.MiddlewareChan).ConsumeFrom(c.Q5Output, "q5", c.CoordinatorsCant, c.CoordinatorPrefetch)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	defer q5Receiver.Close()

	moviesMetadataSender, err := (*c.MiddlewareChan).WriteTo(c.MoviesMetadataName, []string{"clean_movies"})
	if err != nil {
		unwrap(err, "Failed to create write queue", log)
	}

	creditsSender, err := (*c.MiddlewareChan).WriteTo(c.CreditsName, []string{"clean_credits"})
	if err != nil {
		unwrap(err, "Failed to create write queue", log)
	}
	ratingsSender, err := (*c.MiddlewareChanByte).WriteTo(c.RatingsName, []string{"clean_ratings"})
	if err != nil {
		unwrap(err, "Failed to create write queue", log)
	}
	allQuerysToEndpointSender, err := (*c.MiddlewareChan).WriteTo(c.AllQuerysToEndpointName, []string{c.AllQuerysToEndpointName})
	if err != nil {
		unwrap(err, "Failed to create write queue", log)
	}
	defer allQuerysToEndpointSender.Close()

OuterLoop:
	for {
		msgEnvelope := <-inputChannel
		msg := msgEnvelope.Msg()
		cid := msgEnvelope.Cid()
		bytes := msg.Buf.Bytes
		t := msg.PackageType
		log.Infof("Received message type: %v", t)
		log.Infof("Received message cid: %v", cid)
		msgEnvelope.Ack(false)
		switch t {
		case common.FileName:
			fileName := string(bytes)
			var sender middleware.Sender[*model.Row]
			var amount int
			var expectedLen int
			var create func([]string) *model.Row
			switch fileName {
			case c.MoviesMetadataName:
				log.Infof("Received file: %s", fileName)
				sender = moviesMetadataSender
				amount = 10000
				expectedLen = 24
				create = Film
			case c.CreditsName:
				log.Infof("Received file: %s", fileName)
				sender = creditsSender
				amount = 10000
				expectedLen = 3
				create = Credit
			case c.RatingsName:
				log.Infof("Received file: %s", fileName)
				sender = nil
				amount = 100000
				expectedLen = 3
				create = Rating
			default:
				panic(fmt.Sprintf("Unknown file name: %s", fileName))
			}

			connReader := &ConnReader{ch: inputChannel, lastReadNotIncluded: make([]byte, 0)}
			reader := csv.NewReader(connReader)

			d, err := reader.Read()
			log.Infof("Received header file: %v", d)
			unwrap(err, "Failed to read CSV header", log)
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
						log.Infof("Last line read: %v", data)
						log.Infof("Processed %d lines from %s", line, fileName)
						log.Infof("End of file reached")
						if fileName != c.RatingsName {
							sender.SendEOF(cid)
						} else {
							ratingsSender.SendEOF(cid)
						}
						break
					}
					log.Errorf("Error reading CSV line: %v", err)
					continue
				}
				if len(data) < expectedLen {
					continue
				}

				if fileName != c.RatingsName {
					row := create(data)
					sender.Send(row, cid)
				} else {
					num, err := strconv.Atoi(data[1])
					unwrap(err, "Failed to convert string to int", log)
					digits := strings.Split(data[2], ".")
					dec, err := strconv.Atoi(digits[0])
					unwrap(err, "Failed to convert string to int", log)
					unit, err := strconv.Atoi(digits[1])
					unwrap(err, "Failed to convert string to int", log)
					val := dec*10 + unit
					rating := RatingB{
						Id:     uint32(num),
						Rating: uint8(val),
					}
					v := rating.Encode()
					ratingsSender.Send(&model.FileChunk{Bytes: v}, cid)
				}

			}
			log.Infof("CSV %s processing completed, closing", fileName)
			if fileName != c.RatingsName {
				sender.Close()
			} else {
				ratingsSender.Close()
			}

		case common.AllFilesSent:
			log.Infof("Received ALL FILES SENT")
			break OuterLoop
		}
	}
	log.Infof("CSV processing completed")

	expectedOutputQ1 := []*model.Row{
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

	// timer := time.NewTimer(time.Hour)

	log.Infof("Verifying Q1")
	err = allQuerysToEndpointSender.Send(model.RowQueryName("Q1"), cid)
	if err != nil {
		log.Errorf("Failed to send message: %v", err)
	}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		envelope, ok, err := q1Receiver.Next(ctx)
		cancel()
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
			log.Infof("No more countries")
			break
		}
		receivedCountry := envelope.Msg()
		err = allQuerysToEndpointSender.Send(model.RowQuery(*receivedCountry), cid)
		if err != nil {
			log.Errorf("Failed to send message: %v", err)
			continue
		}
		log.Infof("Received country: %s %v", receivedCountry.Strings["country"], receivedCountry.Arrays["budget_sum"])
		log.Infof("Received country debug: %+v", receivedCountry)
		expectedOutputQ1 = removeQ1(expectedOutputQ1, receivedCountry, log)
		if len(expectedOutputQ1) == 0 {
			log.Infof("All expected films received")
			break
		}
		err = envelope.Ack(true)
		unwrap(err, "Failed to ack message", log)
	}
	if len(expectedOutputQ1) > 0 {
		log.Errorf("Not all expected films received. Missing %v", expectedOutputQ1)
	}

	log.Infof("Verifying Q2")
	err = allQuerysToEndpointSender.Send(model.RowQueryName("Q2"), cid)
	if err != nil {
		log.Errorf("Failed to send message: %v", err)
	}
	// Incorrect current answer
	expectedOutputQ2 := []*model.Row{
		{Numerics: map[string]uint64{"budget_sum": 120153886644}, Strings: map[string]string{"country": "US"}},
		{Numerics: map[string]uint64{"budget_sum": 2256831838}, Strings: map[string]string{"country": "FR"}},
		{Numerics: map[string]uint64{"budget_sum": 1611604610}, Strings: map[string]string{"country": "GB"}},
		{Numerics: map[string]uint64{"budget_sum": 1169682797}, Strings: map[string]string{"country": "IN"}},
		{Numerics: map[string]uint64{"budget_sum": 832585873}, Strings: map[string]string{"country": "JP"}},
	}

	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		envelope, ok, err := q2Receiver.Next(ctx)
		cancel()
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
			log.Infof("No more countries")
			break
		}
		receivedCountry := envelope.Msg()
		err = allQuerysToEndpointSender.Send(model.RowQuery(*receivedCountry), cid)
		if err != nil {
			log.Errorf("Failed to send message: %v", err)
			continue
		}
		log.Infof("Received country: %s %v", receivedCountry.Strings["country"], receivedCountry.Arrays["budget_sum"])
		log.Infof("Received country debug: %+v", receivedCountry)
		expectedOutputQ2 = removeQ2(expectedOutputQ2, receivedCountry, log)
		if len(expectedOutputQ2) == 0 {
			log.Infof("All expected films received")
			break
		}
		err = envelope.Ack(true)
		unwrap(err, "Failed to ack message", log)
	}
	if len(expectedOutputQ2) > 0 {
		log.Errorf("Not all expected films received. Missing %v", expectedOutputQ2)
	}

	log.Infof("Verifying Q3")
	err = allQuerysToEndpointSender.Send(model.RowQueryName("Q3"), cid)
	if err != nil {
		log.Errorf("Failed to send message: %v", err)
	}
	expectedOutputQ3 := []*model.Row{
		{Floats: map[string]float64{"avg_rating": 4.0}, Strings: map[string]string{"title": "The forbidden education", "movieID": "125619"}},
		{Floats: map[string]float64{"avg_rating": 1.0}, Strings: map[string]string{"title": "Left for Dead", "movieID": "128598"}},
	}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		envelope, ok, err := q3Receiver.Next(ctx)
		cancel()
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
		err = allQuerysToEndpointSender.Send(model.RowQuery(*receivedMovie), cid)
		if err != nil {
			log.Errorf("Failed to send message: %v", err)
			continue
		}
		// log.Infof("Received film: %s %v", receivedMovie.Strings["title"], receivedMovie.Arrays["genres"])
		// log.Infof("Received film debug: %+v", receivedMovie)
		expectedOutputQ3 = removeQ3(expectedOutputQ3, receivedMovie, log)
		if len(expectedOutputQ3) == 0 {
			log.Infof("All expected films received")
			break
		}
		err = envelope.Ack(true)
		unwrap(err, "Failed to ack message", log)
	}
	if len(expectedOutputQ3) > 0 {
		log.Errorf("Not all expected films received. Missing %v", expectedOutputQ3)
	}

	log.Infof("Verifying Q4")
	expectedOutputQ4 := []*model.Row{
		{Numerics: map[string]uint64{"count": 17}, Strings: map[string]string{"actor": "Ricardo Darín"}},
		{Numerics: map[string]uint64{"count": 7}, Strings: map[string]string{"actor": "Alejandro Awada"}},
		{Numerics: map[string]uint64{"count": 7}, Strings: map[string]string{"actor": "Inés Efron"}},
		{Numerics: map[string]uint64{"count": 7}, Strings: map[string]string{"actor": "Leonardo Sbaraglia"}},
		{Numerics: map[string]uint64{"count": 7}, Strings: map[string]string{"actor": "Valeria Bertuccelli"}},
		{Numerics: map[string]uint64{"count": 6}, Strings: map[string]string{"actor": "Arturo Goetz"}},
		{Numerics: map[string]uint64{"count": 6}, Strings: map[string]string{"actor": "Diego Peretti"}},
		{Numerics: map[string]uint64{"count": 6}, Strings: map[string]string{"actor": "Pablo Echarri"}},
		{Numerics: map[string]uint64{"count": 6}, Strings: map[string]string{"actor": "Rafael Spregelburd"}},
		{Numerics: map[string]uint64{"count": 6}, Strings: map[string]string{"actor": "Rodrigo de la Serna"}},
	}
	countCredit := 0
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		envelope, ok, err := q4Receiver.Next(ctx)
		cancel()
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
			log.Infof("No more actors")
			break
		}
		countCredit++
		receivedActor := envelope.Msg()
		log.Infof("Received country: %s %v", receivedActor.Strings["actor"], receivedActor.Numerics["count"])
		log.Infof("Received country debug: %+v", receivedActor)
		expectedOutputQ4 = removeQ4(expectedOutputQ4, receivedActor, log)
		if len(expectedOutputQ4) == 0 {
			log.Infof("All expected actors received")
			break
		}
		err = envelope.Ack(true)
		unwrap(err, "Failed to ack message", log)
	}
	if len(expectedOutputQ4) > 0 {
		log.Errorf("Not all expected actors received. Missing %v", expectedOutputQ4)
	}

	// Expected:
	// NEGATIVE    5453.397595
	// POSITIVE    5668.650541
	expectedOutputQ5 := []*model.Row{
		{Strings: map[string]string{"sentiment": "NEGATIVE"}, Floats: map[string]float64{"avg_rate": 5453.397595}},
		{Strings: map[string]string{"sentiment": "POSITIVE"}, Floats: map[string]float64{"avg_rate": 5668.650541}},
	}

	log.Infof("Verifying Q5")
	err = allQuerysToEndpointSender.Send(model.RowQueryName("Q5"), cid)
	if err != nil {
		log.Errorf("Failed to send message q5: %v", err)
	}
	for {
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		envelope, ok, err := q5Receiver.Next(ctx)
		cancel()
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
			log.Infof("No more films")
			break
		}
		receivedSentiment := envelope.Msg()

		err = allQuerysToEndpointSender.Send(model.RowQuery(*receivedSentiment), cid)
		if err != nil {
			log.Errorf("Failed to send message sentiment: %v", err)
			continue
		}
		log.Infof("Received sentiment debug: %+v", receivedSentiment)
		expectedOutputQ5 = removeQ5(expectedOutputQ5, receivedSentiment, log)
		if len(expectedOutputQ5) == 0 {
			log.Infof("All expected films received")
			break
		}

		err = envelope.Ack(true)
		unwrap(err, "Failed to ack message", log)
	}
	if len(expectedOutputQ5) > 0 {
		log.Errorf("Not all expected films received. Missing %v", expectedOutputQ5)
	}
	log.Infof("All expected films received")

	// timer.Stop()
}

func removeQ1(slice []*model.Row, movie *model.Row, log *logger.ConsoleLogger) []*model.Row {

	for i, v := range slice {
		if v.Strings["title"] == movie.Strings["title"] && stringSlicesEqual(v.Arrays["genres"], movie.Arrays["genres"]) {
			log.Infof("Film matched expected")
			return slices.Delete(slice, i, i+1)
		}
	}
	log.Errorf("Film not matched expected")
	return slice
}

func removeQ2(slice []*model.Row, country *model.Row, log *logger.ConsoleLogger) []*model.Row {

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

func removeQ3(slice []*model.Row, movie *model.Row, log *logger.ConsoleLogger) []*model.Row {

	for i, v := range slice {
		if v.Strings["title"] == movie.Strings["title"] && v.Strings["movieID"] == movie.Strings["movieID"] {
			log.Infof("Film matched expected")
			return slices.Delete(slice, i, i+1)
		}
	}
	log.Errorf("Film not matched expected")
	return slice
}

func removeQ4(slice []*model.Row, actor *model.Row, log *logger.ConsoleLogger) []*model.Row {

	for i, v := range slice {
		if v.Strings["actor"] == actor.Strings["actor"] {
			if v.Numerics["count"] == actor.Numerics["count"] {
				log.Infof("Actor matched expected")
			} else {
				log.Errorf("Count not matched expected: %d != %d", actor.Numerics["count"], v.Numerics["count"])
			}
			return slices.Delete(slice, i, i+1)
		}
	}
	log.Errorf("Actor not matched expected")
	return slice
}

func removeQ5(expectedOutputQ5 []*model.Row, receivedSentiment *model.Row, log *logger.ConsoleLogger) []*model.Row {
	for i, v := range expectedOutputQ5 {
		if v.Strings["sentiment"] == receivedSentiment.Strings["sentiment"] {
			if v.Floats["avg_rate"]-receivedSentiment.Floats["avg_rate"] < 0.0001 {
				log.Infof("Sentiment matched expected")
			} else {
				log.Errorf("Avg rate not matched expected: %f != %f", receivedSentiment.Floats["avg_rate"], v.Floats["avg_rate"])
			}
			return slices.Delete(expectedOutputQ5, i, i+1)
		}
	}
	log.Errorf("Sentiment not matched expected")
	return expectedOutputQ5
}

// adult,belongs_to_collection,budget,genres,homepage,id,imdb_id,original_language,
// original_title,overview,popularity,poster_path,production_companies,
// production_countries,
// release_date,revenue,runtime,spoken_languages,status,tagline,title,video,vote_average,vote_count

func Film(data []string) *model.Row {
	budget, genres, id, overview, production_countries, release_date, revenue, title :=
		data[2], data[3], data[5], data[9], data[13], data[14], data[15], data[20]

	return &model.Row{
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

func Credit(data []string) *model.Row {
	return &model.Row{
		Strings: map[string]string{
			"ID":   data[2],
			"cast": data[0],
		},
	}
}

func Rating(data []string) *model.Row {
	return &model.Row{
		Strings: map[string]string{
			"movieID": data[1],
			"rating":  data[2],
		},
	}
}

func unwrap(err error, msg string, log *logger.ConsoleLogger) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

type ConnReader struct {
	ch                  chan middleware.Envelope[*common.PackageFile]
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
	data := msg.Buf.Bytes
	t := msg.PackageType
	cantCopyFromData := min(remainingCapacity, len(data))
	switch t {
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
		err = fmt.Errorf("invalid message type: %v", t)
	}
	msgEnvelope.Ack(false)
	return n, err
}
