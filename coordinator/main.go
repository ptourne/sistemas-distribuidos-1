package main

import (
	"context"
	"encoding/binary"
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"slices"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

func main() {
	log := logger.NewConsoleLogger("coordinator", logger.Info)
	// logMiddleware := logger.NewConsoleLogger("coordinator_mid", logger.Info)
	connector, err := rabbitmq.Connector()
	if err != nil {
		log.Errorf("Failed to connect middleware: %v", err)
		return
	}
	config := NewConfiguration(log, connector)
	defer config.Close()

	wg := sync.WaitGroup{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	inputsChannelMap := map[string]*ChannelsCid{}
	inputChannelMapLock := sync.Mutex{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			envelope, err := config.ReceiverFileByte.Next(ctx)
			if err != nil {
				if err.Error() == "read channel was closed" {
					log.Infof("Channel closed: %v", config.ReadFileByteQueue)
					break
				}
				if err.Error() == "timeout reached while waiting for message or ctx canceled" {
					log.Infof("Timeout reached while waiting for message or ctx canceled")
					break
				}
				log.Errorf("Error reading from middleware: %v", err)
				continue
			}
			cid := envelope.Cid()
			channelsCid, exists := inputsChannelMap[cid]
			if !exists {
				channelsCid = NewChannelsCid()
				inputChannelMapLock.Lock()
				inputsChannelMap[cid] = channelsCid
				inputChannelMapLock.Unlock()
				wg.Add(1)
				go handleClient(cid, channelsCid, config, &wg)
			}
			switch envelope.Type() {
			case middleware.EOF:
			case middleware.Prune:
				log.Infof("Prune arrived for cid: %s in %s", envelope.Cid(), config.ReadFileByteQueue)
				err = envelope.Ack(false)
				unwrap(err, "Failed to ack message", log)
				continue
			}
			channelsCid.input <- envelope
		}
	}()
	wg.Add(1)
	go nextQueue(ctx, config.ReceiverQueueTest, config.ReceiverTest, log, inputsChannelMap, GetTest, &inputChannelMapLock, &wg, true)
	// wg.Add(1)
	// go nextQueue(ctx, config.ReceiverQ1, config.Q1Output, log, inputsChannelMap, GetQ1, &inputChannelMapLock, &wg, true)
	// wg.Add(1)
	// go nextQueue(ctx, config.ReceiverQ2, config.Q2Output, log, inputsChannelMap, GetQ2, &inputChannelMapLock, &wg, true) //TODO deberia ser SOLO EN Q5

	wg.Wait()
	log.Infof("EXITING COORDINATOR")
	for _, channelCid := range inputsChannelMap {
		channelCid.Close()
	}
}

func nextQueue(ctx context.Context, queue middleware.Receiver[*model.Row], channelString string, log *logger.ConsoleLogger, inputsChannelMap map[string]*ChannelsCid, getFuc func(*ChannelsCid) chan middleware.Envelope[*model.Row], inputChannelMapLock *sync.Mutex, wg *sync.WaitGroup, lastQuery bool) {
	defer wg.Done()
	for {
		envelope, err := queue.Next(ctx)
		if err != nil {
			if err.Error() == "read channel was closed" {
				log.Infof("Channel closed: %v", channelString)
				break
			}
			if err.Error() == "timeout reached while waiting for message or ctx canceled" {
				log.Infof("Timeout reached while waiting for message or ctx canceled")
				break
			}
			log.Errorf("Error reading from middleware: %v", err)
			continue
		}
		cid := envelope.Cid()
		channelsCid, exists := inputsChannelMap[cid]
		if !exists {
			log.Errorf("Channel not found: %v", cid)
			panic("Channel not found")
			// break
		}
		switch envelope.Type() {
		case middleware.EOF:
			if lastQuery {
				log.Infof("cid %s finished receiving", cid)
				inputChannelMapLock.Lock()
				delete(inputsChannelMap, envelope.Cid())
				inputChannelMapLock.Unlock()
			}
		case middleware.Prune:
			log.Infof("Prune arrived for cid: %s in %s", envelope.Cid(), channelString)
			err = envelope.Ack(false)
			unwrap(err, "Failed to ack message", log)
			continue
		}

		queue := getFuc(channelsCid)
		queue <- envelope
	}
}

func handleClient(cid string, channelsCid *ChannelsCid, c *ConfigCoordinator, wg *sync.WaitGroup) {
	defer wg.Done()
	var log = logger.NewConsoleLogger(fmt.Sprintf("coordinator-%s", cid), logger.Info)
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
		msgEnvelope := <-channelsCid.input
		cid := msgEnvelope.Cid()
		msg := msgEnvelope.Msg()
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

			connReader := &ConnReader{ch: channelsCid.input, lastReadNotIncluded: make([]byte, 0)}
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

	verifyingQtest(log, allQuerysToEndpointSender, cid, channelsCid.qtest)
	// verifyingQ1(log, allQuerysToEndpointSender, cid, channelsCid.q1)
	// verifyingQ2(log, allQuerysToEndpointSender, cid, channelsCid.q2)
	// verifyingQ3(log, allQuerysToEndpointSender, cid, channelsCid.q3)
	// verifyingQ4(log, allQuerysToEndpointSender, cid, channelsCid.q4)
	// verifyingQ5(log, allQuerysToEndpointSender, cid, channelsCid.q5)

	log.Infof("finish all querys verified")
}

type ConfigCoordinator struct {
	CoordinatorsCant            uint
	CoordinatorPrefetch         int
	MiddlewareChan              *middleware.Connection[*model.Row]
	MiddlewareChanByte          *middleware.Connection[*model.FileChunk]
	MiddlewareChanPackageByte   *middleware.Connection[*common.PackageFile]
	ReadFileByteQueue           string
	MoviesMetadataName          string
	CreditsName                 string
	RatingsName                 string
	Q1Output                    string
	Q2Output                    string
	Q3Output                    string
	Q4Output                    string
	Q5Output                    string
	ReceiverTest                string
	AllQuerysToEndpointName     string
	ReceiverFileByte            middleware.Receiver[*common.PackageFile]
	ReceiverQ1                  middleware.Receiver[*model.Row]
	ReceiverQ2                  middleware.Receiver[*model.Row]
	ReceiverQ3                  middleware.Receiver[*model.Row]
	ReceiverQ4                  middleware.Receiver[*model.Row]
	ReceiverQ5                  middleware.Receiver[*model.Row]
	ReceiverQueueTest           middleware.Receiver[*model.Row]
	ReceiverAllQuerysToEndpoint middleware.Receiver[*model.Row]
}

func NewConfiguration(log *logger.ConsoleLogger, connector *rabbitmq.RabbitMQConnector) *ConfigCoordinator {
	config := ConfigCoordinator{}
	middlewareChan := rabbitmq.NewMiddleware[*model.Row](connector, log)
	middlewareChanPackageByte := rabbitmq.NewMiddleware[*common.PackageFile](connector, log)
	middlewareChanByte := rabbitmq.NewMiddleware[*model.FileChunk](connector, log)
	config.MiddlewareChan = &middlewareChan
	config.MiddlewareChanByte = &middlewareChanByte
	config.MiddlewareChanPackageByte = &middlewareChanPackageByte
	log.Infof("Connected to middleware")
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
	config.ReceiverTest = "clean_movies"

	receiverFileByte, err := middlewareChanPackageByte.ConsumeFrom(config.ReadFileByteQueue, config.ReadFileByteQueue, config.CoordinatorsCant, config.CoordinatorPrefetch)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	config.ReceiverFileByte = receiverFileByte

	q1Receiver, err := middlewareChan.ConsumeFrom(config.Q1Output, "q1", config.CoordinatorsCant, config.CoordinatorPrefetch)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	config.ReceiverQ1 = q1Receiver

	q2Receiver, err := middlewareChan.ConsumeFrom(config.Q2Output, "q2", config.CoordinatorsCant, config.CoordinatorPrefetch)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	config.ReceiverQ2 = q2Receiver

	q3Receiver, err := middlewareChan.ConsumeFrom(config.Q3Output, "q3", config.CoordinatorsCant, config.CoordinatorPrefetch)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	config.ReceiverQ3 = q3Receiver

	q4Receiver, err := middlewareChan.ConsumeFrom(config.Q4Output, "q4", config.CoordinatorsCant, config.CoordinatorPrefetch)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	config.ReceiverQ4 = q4Receiver

	q5Receiver, err := middlewareChan.ConsumeFrom(config.Q5Output, "q5", config.CoordinatorsCant, config.CoordinatorPrefetch)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	config.ReceiverQ5 = q5Receiver

	qTest, err := middlewareChan.ConsumeFrom(config.ReceiverTest, "qtest", config.CoordinatorsCant, config.CoordinatorPrefetch)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	config.ReceiverQueueTest = qTest
	return &config
}

func (c *ConfigCoordinator) Close() {
	if c.MiddlewareChan != nil {
		(*c.MiddlewareChan).Close()
		c.MiddlewareChan = nil
	}
	if c.MiddlewareChanByte != nil {
		(*c.MiddlewareChanByte).Close()
		c.MiddlewareChanByte = nil
	}
	if c.MiddlewareChanPackageByte != nil {
		(*c.MiddlewareChanPackageByte).Close()
		c.MiddlewareChanPackageByte = nil
	}
	if c.ReceiverFileByte != nil {
		(c.ReceiverFileByte).Close()
		c.ReceiverFileByte = nil
	}
	if c.ReceiverQ1 != nil {
		(c.ReceiverQ1).Close()
		c.ReceiverQ1 = nil
	}
	if c.ReceiverQ2 != nil {
		(c.ReceiverQ2).Close()
		c.ReceiverQ2 = nil
	}
	if c.ReceiverQ3 != nil {
		(c.ReceiverQ3).Close()
		c.ReceiverQ3 = nil
	}
	if c.ReceiverQ4 != nil {
		(c.ReceiverQ4).Close()
		c.ReceiverQ4 = nil
	}
	if c.ReceiverQ5 != nil {
		(c.ReceiverQ5).Close()
		c.ReceiverQ5 = nil
	}
	if c.ReceiverQueueTest != nil {
		(c.ReceiverQueueTest).Close()
		c.ReceiverQueueTest = nil
	}
}

type ChannelsCid struct {
	input chan middleware.Envelope[*common.PackageFile]
	q1    chan middleware.Envelope[*model.Row]
	q2    chan middleware.Envelope[*model.Row]
	q3    chan middleware.Envelope[*model.Row]
	q4    chan middleware.Envelope[*model.Row]
	q5    chan middleware.Envelope[*model.Row]
	qtest chan middleware.Envelope[*model.Row]
}

func NewChannelsCid() *ChannelsCid {
	return &ChannelsCid{
		//lint:ignore S1019 Ignoring suggestion to simplify channel creation
		input: make(chan middleware.Envelope[*common.PackageFile], 0),
		//lint:ignore S1019 Ignoring suggestion to simplify channel creation
		q1: make(chan middleware.Envelope[*model.Row], 0),
		//lint:ignore S1019 Ignoring suggestion to simplify channel creation
		q2: make(chan middleware.Envelope[*model.Row], 0),
		//lint:ignore S1019 Ignoring suggestion to simplify channel creation
		q3: make(chan middleware.Envelope[*model.Row], 0),
		//lint:ignore S1019 Ignoring suggestion to simplify channel creation
		q4: make(chan middleware.Envelope[*model.Row], 0),
		//lint:ignore S1019 Ignoring suggestion to simplify channel creation
		q5: make(chan middleware.Envelope[*model.Row], 0),
		//lint:ignore S1019 Ignoring suggestion to simplify channel creation
		qtest: make(chan middleware.Envelope[*model.Row], 0),
	}
}

func GetInput(c *ChannelsCid) chan middleware.Envelope[*common.PackageFile] {
	return c.input
}
func GetQ1(c *ChannelsCid) chan middleware.Envelope[*model.Row] {
	return c.q1
}
func GetQ2(c *ChannelsCid) chan middleware.Envelope[*model.Row] {
	return c.q2
}
func GetQ3(c *ChannelsCid) chan middleware.Envelope[*model.Row] {
	return c.q3
}
func GetQ4(c *ChannelsCid) chan middleware.Envelope[*model.Row] {
	return c.q4
}
func GetQ5(c *ChannelsCid) chan middleware.Envelope[*model.Row] {
	return c.q5
}
func GetTest(c *ChannelsCid) chan middleware.Envelope[*model.Row] {
	return c.qtest
}

func (c *ChannelsCid) Close() {
	if c.input != nil {
		close(c.input)
		c.input = nil
	}
	if c.q1 != nil {
		close(c.q1)
		c.q1 = nil
	}
	if c.q2 != nil {
		close(c.q2)
		c.q2 = nil
	}
	if c.q3 != nil {
		close(c.q3)
		c.q3 = nil
	}
	if c.q4 != nil {
		close(c.q4)
		c.q4 = nil
	}
	if c.q5 != nil {
		close(c.q5)
		c.q5 = nil
	}

}

func verifyingQtest(log *logger.ConsoleLogger, allQuerysToEndpointSender middleware.Sender[*model.Row], cid string, qReceiver chan middleware.Envelope[*model.Row]) {
	expectedOutputQtest := []*model.Row{}
	verifyingQuery(log, allQuerysToEndpointSender, cid, qReceiver, "Qtest", expectedOutputQtest, removeQtest, true)
}

func verifyingQ1(log *logger.ConsoleLogger, allQuerysToEndpointSender middleware.Sender[*model.Row], cid string, q1Receiver chan middleware.Envelope[*model.Row]) {
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
	verifyingQuery(log, allQuerysToEndpointSender, cid, q1Receiver, "Q1", expectedOutputQ1, removeQ1, true)
}

func verifyingQ2(log *logger.ConsoleLogger, allQuerysToEndpointSender middleware.Sender[*model.Row], cid string, q1Receiver chan middleware.Envelope[*model.Row]) {
	expectedOutputQ2 := []*model.Row{
		{Numerics: map[string]uint64{"budget_sum": 120153886644}, Strings: map[string]string{"country": "US"}},
		{Numerics: map[string]uint64{"budget_sum": 2256831838}, Strings: map[string]string{"country": "FR"}},
		{Numerics: map[string]uint64{"budget_sum": 1611604610}, Strings: map[string]string{"country": "GB"}},
		{Numerics: map[string]uint64{"budget_sum": 1169682797}, Strings: map[string]string{"country": "IN"}},
		{Numerics: map[string]uint64{"budget_sum": 832585873}, Strings: map[string]string{"country": "JP"}},
	}
	verifyingQuery(log, allQuerysToEndpointSender, cid, q1Receiver, "Q2", expectedOutputQ2, removeQ2, true) //TODO MANDARLO el send eof en q5
}

func verifyingQ3(log *logger.ConsoleLogger, allQuerysToEndpointSender middleware.Sender[*model.Row], cid string, q1Receiver chan middleware.Envelope[*model.Row]) {
	expectedOutputQ3 := []*model.Row{
		{Floats: map[string]float64{"avg_rating": 4.0}, Strings: map[string]string{"title": "The forbidden education", "movieID": "125619"}},
		{Floats: map[string]float64{"avg_rating": 1.0}, Strings: map[string]string{"title": "Left for Dead", "movieID": "128598"}},
	}
	verifyingQuery(log, allQuerysToEndpointSender, cid, q1Receiver, "Q3", expectedOutputQ3, removeQ3, false)
}

func verifyingQ4(log *logger.ConsoleLogger, allQuerysToEndpointSender middleware.Sender[*model.Row], cid string, q1Receiver chan middleware.Envelope[*model.Row]) {
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
	verifyingQuery(log, allQuerysToEndpointSender, cid, q1Receiver, "Q4", expectedOutputQ4, removeQ4, false)
}

func verifyingQ5(log *logger.ConsoleLogger, allQuerysToEndpointSender middleware.Sender[*model.Row], cid string, q1Receiver chan middleware.Envelope[*model.Row]) {
	expectedOutputQ5 := []*model.Row{
		{Strings: map[string]string{"sentiment": "NEGATIVE"}, Floats: map[string]float64{"avg_rate": 5453.397595}},
		{Strings: map[string]string{"sentiment": "POSITIVE"}, Floats: map[string]float64{"avg_rate": 5668.650541}},
	}
	verifyingQuery(log, allQuerysToEndpointSender, cid, q1Receiver, "Q5", expectedOutputQ5, removeQ5, true)
}

func verifyingQuery(log *logger.ConsoleLogger, allQuerysToEndpointSender middleware.Sender[*model.Row], cid string, qReceiver chan middleware.Envelope[*model.Row], queryNumber string, expectedOutput []*model.Row, remove func([]*model.Row, *model.Row, *logger.ConsoleLogger) []*model.Row, lastQuery bool) {
	log.Infof("Verifying %s", queryNumber)
	err := allQuerysToEndpointSender.Send(model.RowQueryName(queryNumber), cid)
	if err != nil {
		log.Errorf("Failed to send message: %v", err)
	}
OuterLoop:
	for {
		envelope := <-qReceiver
		if envelope.Cid() != cid {
			log.Errorf("Received message from wrong cid: %s", envelope.Cid())
			continue
		}
		switch envelope.Type() {
		case middleware.EOF:
			log.Infof("No more countries, finish arrived")
			if lastQuery {
				allQuerysToEndpointSender.SendEOF(cid)
			}
			err = envelope.Ack(false)
			unwrap(err, "Failed to ack message", log)
			break OuterLoop
		case middleware.Prune:
			log.Infof("Prune arrived for cid: %s in %s", envelope.Cid(), queryNumber)
			err = envelope.Ack(false)
			unwrap(err, "Failed to ack message", log)
			continue
		}
		receivedCountry := envelope.Msg()
		err = allQuerysToEndpointSender.Send(model.RowQuery(*receivedCountry), cid)
		if err != nil {
			log.Errorf("Failed to send message: %v", err)
			continue
		}
		log.Debugf("Received country debug: %v", receivedCountry)
		expectedOutput = remove(expectedOutput, receivedCountry, log)
		err = envelope.Ack(false)
		unwrap(err, "Failed to ack message", log)
	}
	if len(expectedOutput) > 0 {
		log.Errorf("Not all expected rows received. Missing %v", expectedOutput)
	}
	if len(expectedOutput) == 0 {
		log.Infof("All expected rows received")
	}
}

func removeQtest(slice []*model.Row, movie *model.Row, log *logger.ConsoleLogger) []*model.Row {
	return slice
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
