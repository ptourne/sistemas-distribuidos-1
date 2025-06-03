package main

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"

	"slices"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

func main() {

	name := os.Getenv("NAME")
	monitor_addrs := os.Getenv("MONITOR_ADDRESSES")

	log := logger.NewConsoleLogger("coordinator", logger.Info)
	ctxHeartbeat, cancelHearbeat := context.WithCancel(context.Background())
	go utils.SendHeartbeat(name, monitor_addrs, log, ctxHeartbeat)
	connector, err := rabbitmq.Connector()
	if err != nil {
		log.Errorf("Failed to connect middleware: %v", err)
		return
	}
	config := NewConfiguration(log, connector)
	defer config.Close()

	wg := sync.WaitGroup{}
	ctx, cancelHearbeat := context.WithCancel(context.Background())
	defer cancelHearbeat()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Infof("Received SIGTERM. Shutting down gracefully...")
		cancelHearbeat()
	}()
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
				if err.Error() == "timeout reached while waiting for message" {
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
				go handleClient(cid, channelsCid, config, &wg, ctx)
			}
			switch envelope.Type() {
			case middleware.EOF:
				panic("EOF arrived in coordinator")
			case middleware.Prune:
				panic("Prune arrived in coordinator")
			}
			select {
			case channelsCid.input <- envelope:
				// enviado con éxito
			case <-ctx.Done():
				log.Infof("Context cancelled before sending to input channel of cid: %s", envelope.Cid())
				return
			}

		}
	}()
	wg.Add(1)
	go nextQueue(ctx, config.ReceiverQ1, config.Q1Output, log, inputsChannelMap, GetQ1, &inputChannelMapLock, &wg, false)
	wg.Add(1)
	go nextQueue(ctx, config.ReceiverQ2, config.Q2Output, log, inputsChannelMap, GetQ2, &inputChannelMapLock, &wg, false)
	wg.Add(1)
	go nextQueue(ctx, config.ReceiverQ3, config.Q3Output, log, inputsChannelMap, GetQ3, &inputChannelMapLock, &wg, false)
	wg.Add(1)
	go nextQueue(ctx, config.ReceiverQ4, config.Q4Output, log, inputsChannelMap, GetQ4, &inputChannelMapLock, &wg, false)
	wg.Add(1)
	go nextQueue(ctx, config.ReceiverQ5, config.Q5Output, log, inputsChannelMap, GetQ5, &inputChannelMapLock, &wg, true)

	wg.Wait()
	log.Infof("All goroutines finished. Closing ChannelsCid")
	for _, channelCid := range inputsChannelMap {
		channelCid.Close()
	}
	cancelHearbeat()
	log.Infof("EXITING COORDINATOR")
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
			if err.Error() == "timeout reached while waiting for message" {
				log.Infof("Timeout reached while waiting for message or ctx canceled (nextQueue)")
				queue.Close()
				break
			}
			log.Errorf("Error reading from middleware: %v", err)
			continue
		}
		cid := envelope.Cid()
		channelsCid, exists := inputsChannelMap[cid]
		if !exists {
			log.Errorf("Channel not found: %v", cid)
			continue
		}
		finished := false
		switch envelope.Type() {
		case middleware.EOF:
			if lastQuery {
				log.Infof("cid %s finished receiving", cid)
				inputChannelMapLock.Lock()
				//close(getFuc(channelsCid))
				finished = true
				delete(inputsChannelMap, envelope.Cid())
				inputChannelMapLock.Unlock()
			}
		case middleware.Prune:
		}

		queue := getFuc(channelsCid)
		inputChannelMapLock.Lock()
		_, stillExists := inputsChannelMap[cid]
		inputChannelMapLock.Unlock()
		if !stillExists && !finished {
			continue
		}

		select {
		case queue <- envelope:
			// enviado con éxito
		case <-ctx.Done():
			log.Infof("Context cancelled before sending envelope: %s", channelString)
			return
		}

	}
}

func handleClient(cid string, channelsCid *ChannelsCid, c *ConfigCoordinator, wg *sync.WaitGroup, ctx context.Context) {
	defer wg.Done()
	var log = logger.NewConsoleLogger(fmt.Sprintf("coordinator-%s", cid), logger.Info)
	n_workers, err := strconv.Atoi(os.Getenv("N_WORKERS"))
	if err != nil {
		log.Errorf("Failed to parse n_workers: %v", err)
		panic(err)
	}
	ratingsConsumers, err := strconv.Atoi(os.Getenv("N_RATINGS_CONSUMERS"))
	if err != nil {
		log.Errorf("Failed to parse N_RATINGS_CONSUMERS: %v", err)
		panic(err)
	}

	moviesMetadataSender, err := (*c.MiddlewareChan).WriteTo(c.MoviesMetadataName, []string{"clean_movies"}, "0", uint(n_workers))
	if err != nil {
		unwrap(err, "Failed to create write queue", log)
	}

	creditsSender, err := (*c.MiddlewareChan).WriteTo(c.CreditsName, []string{"clean_credits"}, "0", uint(n_workers))
	if err != nil {
		unwrap(err, "Failed to create write queue", log)
	}
	ratingsSender, err := (*c.MiddlewareChanByte).WriteTo(c.RatingsName, []string{"reduce_by_movieId"}, "0", uint(ratingsConsumers))
	if err != nil {
		unwrap(err, "Failed to create write queue", log)
	}
	allQuerysToEndpointSender, err := (*c.MiddlewareChan).WriteTo(c.AllQuerysToEndpointName, []string{c.AllQuerysToEndpointName}, "0", uint(1))
	if err != nil {
		unwrap(err, "Failed to create write queue", log)
	}
	defer allQuerysToEndpointSender.Close()

	lastIdSent := uint64(0)

OuterLoop:
	for {
		select {
		case <-ctx.Done():
			log.Infof("Context cancelled, exiting handleClient")
			moviesMetadataSender.Close()
			creditsSender.Close()
			ratingsSender.Close()
			return
		case msgEnvelope, ok := <-channelsCid.input:
			if !ok {
				log.Infof("Channel closed: %v", channelsCid.input)
				break OuterLoop
			}
			cid := msgEnvelope.Cid()
			msg := msgEnvelope.Msg()
			bytes := msg.Buf.Bytes
			t := msg.PackageType
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

				connReader := &ConnReader{ch: channelsCid.input, lastReadNotIncluded: make([]byte, 0), ctx: ctx}
				reader := csv.NewReader(connReader)

				d, err := reader.Read()
				if err != nil && err.Error() == "read canceled by context" {
					log.Infof("Context cancelled, exiting handleClient")
					moviesMetadataSender.Close()
					creditsSender.Close()
					ratingsSender.Close()
					return
				}
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
						if err.Error() == "read canceled by context" {
							log.Infof("Context cancelled, exiting handleClient")
							moviesMetadataSender.Close()
							creditsSender.Close()
							ratingsSender.Close()
							return
						}
						if err.Error() == "EOF" {
							log.Infof("Processed %d lines from %s", line, fileName)
							log.Infof("End of file reached")
							if fileName != c.RatingsName {
								sender.SendEOF(cid)
							} else {
								err := ratingsSender.SendEOF(cid)
								if err != nil {
									log.Errorf("Error sending EOF for ratings: %v", err)
								}
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
						sender.Send(row, cid, lastIdSent)
						lastIdSent++
					} else {
						movieId := data[1]
						num, err := strconv.Atoi(movieId)
						unwrap(err, "Failed to convert string to int", log)
						digits := strings.Split(data[2], ".")
						dec, err := strconv.Atoi(digits[0])
						unwrap(err, "Failed to convert string to int", log)
						unit, err := strconv.Atoi(digits[1])
						unwrap(err, "Failed to convert string to int", log)
						val := dec*10 + unit
						rating := model.Rating{
							Id:     uint32(num),
							Rating: uint8(val),
						}
						routingKey := string(movieId[len(movieId)-1])
						ratingsSender.SendRK(&rating, cid, lastIdSent, routingKey)
						lastIdSent++
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
	}
	log.Infof("CSV processing completed")

	verifyingQ1(log, allQuerysToEndpointSender, cid, channelsCid.q1)
	verifyingQ2(log, allQuerysToEndpointSender, cid, channelsCid.q2)
	verifyingQ3(log, allQuerysToEndpointSender, cid, channelsCid.q3)
	verifyingQ4(log, allQuerysToEndpointSender, cid, channelsCid.q4)
	verifyingQ5(log, allQuerysToEndpointSender, cid, channelsCid.q5)

	log.Infof("finish all querys verified")
}

type ConfigCoordinator struct {
	CoordinatorsCant            uint
	CoordinatorPrefetch         int
	MiddlewareChan              *middleware.Connection[*model.Row]
	MiddlewareChanByte          *middleware.Connection[*model.Rating]
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
	middlewareChanByte := rabbitmq.NewMiddleware[*model.Rating](connector, log)
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
	prefetch, err := strconv.Atoi(os.Getenv("PREFETCH"))
	if err != nil {
		log.Errorf("failed to parse PREFETCH: %v", err)
		prefetch = 1
	}
	log.Debugf("Coordinator: prefetch: %d", prefetch)
	config.CoordinatorPrefetch = prefetch
	config.ReceiverTest = "clean_movies"

	receiverFileByte, err := middlewareChanPackageByte.ConsumeFrom(config.ReadFileByteQueue, config.ReadFileByteQueue, "0", config.CoordinatorPrefetch, 1)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	config.ReceiverFileByte = receiverFileByte

	q1Receiver, err := middlewareChan.ConsumeFrom(config.Q1Output, "q1", "0", config.CoordinatorPrefetch, 1)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	config.ReceiverQ1 = q1Receiver

	q2Receiver, err := middlewareChan.ConsumeFrom(config.Q2Output, "q2", "0", config.CoordinatorPrefetch, 1)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	config.ReceiverQ2 = q2Receiver

	q3Receiver, err := middlewareChan.ConsumeFrom(config.Q3Output, "q3", "0", config.CoordinatorPrefetch, 1)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	config.ReceiverQ3 = q3Receiver

	q4Receiver, err := middlewareChan.ConsumeFrom(config.Q4Output, "q4", "0", config.CoordinatorPrefetch, 1)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	config.ReceiverQ4 = q4Receiver

	q5Receiver, err := middlewareChan.ConsumeFrom(config.Q5Output, "q5", "0", config.CoordinatorPrefetch, 1)
	if err != nil {
		unwrap(err, "Failed to create read queue", log)
	}
	config.ReceiverQ5 = q5Receiver

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
	once  sync.Once
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

func (c *ChannelsCid) Close() {
	c.once.Do(func() {
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
	})
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
	verifyingQuery(log, allQuerysToEndpointSender, cid, q1Receiver, "Q1", expectedOutputQ1, removeQ1, false)
}

func verifyingQ2(log *logger.ConsoleLogger, allQuerysToEndpointSender middleware.Sender[*model.Row], cid string, q1Receiver chan middleware.Envelope[*model.Row]) {
	expectedOutputQ2 := []*model.Row{
		{Numerics: map[string]uint64{"budget_sum": 120153886644}, Strings: map[string]string{"country": "US"}},
		{Numerics: map[string]uint64{"budget_sum": 2256831838}, Strings: map[string]string{"country": "FR"}},
		{Numerics: map[string]uint64{"budget_sum": 1611604610}, Strings: map[string]string{"country": "GB"}},
		{Numerics: map[string]uint64{"budget_sum": 1169682797}, Strings: map[string]string{"country": "IN"}},
		{Numerics: map[string]uint64{"budget_sum": 832585873}, Strings: map[string]string{"country": "JP"}},
	}
	verifyingQuery(log, allQuerysToEndpointSender, cid, q1Receiver, "Q2", expectedOutputQ2, removeQ2, false)
}

func verifyingQ3(log *logger.ConsoleLogger, allQuerysToEndpointSender middleware.Sender[*model.Row], cid string, q1Receiver chan middleware.Envelope[*model.Row]) {
	//expectedOutputQ3 := []*model.Row{
	// 	{Floats: map[string]float64{"avg_rating": 4.0}, Strings: map[string]string{"title": "The forbidden education", "movieID": "125619"}},
	// 	{Floats: map[string]float64{"avg_rating": 1.0}, Strings: map[string]string{"title": "Left for Dead", "movieID": "128598"}},
	// }

	// expectedOutputQ3_6M := []*model.Row{
	// 	{Floats: map[string]float64{"avg_rating": 4.0}, Strings: map[string]string{"title": "The forbidden education", "movieID": "125619"}},
	// 	{Floats: map[string]float64{"avg_rating": 1.0}, Strings: map[string]string{"title": "Left for Dead", "movieID": "128598"}},
	// }
	expectedOutputQ3_200k := []*model.Row{
		{Floats: map[string]float64{"avg_rating": 4.4}, Strings: map[string]string{"title": "The Mugger", "movieID": "6636"}},
		{Floats: map[string]float64{"avg_rating": 0.5}, Strings: map[string]string{"title": "Ana and the Others", "movieID": "48596"}},
	}

	verifyingQuery(log, allQuerysToEndpointSender, cid, q1Receiver, "Q3", expectedOutputQ3_200k, removeQ3, false)
}

func verifyingQ4(log *logger.ConsoleLogger, allQuerysToEndpointSender middleware.Sender[*model.Row], cid string, qReceiver chan middleware.Envelope[*model.Row]) {
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
	verifyingQuery(log, allQuerysToEndpointSender, cid, qReceiver, "Q4", expectedOutputQ4, removeQ4, false)
}

func verifyingQ5(log *logger.ConsoleLogger, allQuerysToEndpointSender middleware.Sender[*model.Row], cid string, q1Receiver chan middleware.Envelope[*model.Row]) {
	expectedOutputQ5 := []*model.Row{
		{Strings: map[string]string{"sentiment": "NEGATIVE"}, Floats: map[string]float64{"avg_rate": 5453.397595}},
		{Strings: map[string]string{"sentiment": "POSITIVE"}, Floats: map[string]float64{"avg_rate": 5668.650541}},
	}
	verifyingQuery(log, allQuerysToEndpointSender, cid, q1Receiver, "Q5", expectedOutputQ5, removeQ5, true)
}

func verifyingQuery(log *logger.ConsoleLogger, allQuerysToEndpointSender middleware.Sender[*model.Row], cid string, qReceiver chan middleware.Envelope[*model.Row], queryNumber string, expectedOutput []*model.Row, remove func([]*model.Row, *model.Row, *logger.ConsoleLogger, string) []*model.Row, lastQuery bool) {
	lastIdSent := uint64(0)
	log.Infof("Verifying %s", queryNumber)
	err := allQuerysToEndpointSender.Send(model.RowQueryName(queryNumber), cid, lastIdSent)
	lastIdSent++
	if err != nil {
		log.Errorf("Failed to send message: %v", err)
	}
OuterLoop:
	for {
		envelope, ok := <-qReceiver
		// log.Infof("Received envelope id %v", envelope.Id())
		if !ok {
			log.Infof("Channel closed: %v", qReceiver)
			return
		}
		if envelope.Cid() != cid {
			log.Errorf("Received message from wrong cid: %s", envelope.Cid())
			continue
		}
		switch envelope.Type() {
		case middleware.EOF:
			log.Infof("Client %s | Query %s | No more rows , EOF arrived", cid, queryNumber)
			if lastQuery {
				allQuerysToEndpointSender.SendEOF(cid)
			}
			err = envelope.Ack(false)
			unwrap(err, "Failed to ack message", log)
			break OuterLoop
		case middleware.Prune:
			log.Infof("Prune arrived for cid: %s in %s", envelope.Cid(), queryNumber)
			// err = envelope.Ack(false)
			// unwrap(err, "Failed to ack message", log)
			// continue
			err := allQuerysToEndpointSender.Prune(cid)
			if err != nil {
				log.Errorf("cid %s | Prune failed in: %s with err:%s", cid, err, queryNumber)
				envelope.Nack(true)
			}
			err = envelope.Ack(false)
			unwrap(err, "Failed to ack message", log)
			continue
		}
		receivedRow := envelope.Msg()
		err = allQuerysToEndpointSender.Send(model.RowQuery(*receivedRow), cid, lastIdSent)
		lastIdSent++
		if err != nil {
			log.Errorf("Failed to send message: %v", err)
			continue
		}
		log.Debugf("Received row debug: %v", receivedRow)
		expectedOutput = remove(expectedOutput, receivedRow, log, cid)
		err = envelope.Ack(false)
		unwrap(err, "Failed to ack message", log)
	}
	if len(expectedOutput) > 0 {
		log.Errorf("Client %s | Query %s | Not all expected rows received 🛑. Missing %v", cid, queryNumber, expectedOutput)
	}
	if len(expectedOutput) == 0 {
		log.Infof("CLIENT %s | QUERY %s | ALL EXPECTED ROWS RECEIVED 🟢", cid, queryNumber)
	}
}

func removeQ1(slice []*model.Row, movie *model.Row, log *logger.ConsoleLogger, cid string) []*model.Row {

	for i, v := range slice {
		if v.Strings["title"] == movie.Strings["title"] && stringSlicesEqual(v.Arrays["genres"], movie.Arrays["genres"]) {
			log.Infof("Client %s | Query Q1 | Film matched expected", cid)
			return slices.Delete(slice, i, i+1)
		}
	}
	log.Errorf("Client %s | Query Q1 | Film not matched expected", cid)
	return slice
}

func removeQ2(slice []*model.Row, country *model.Row, log *logger.ConsoleLogger, cid string) []*model.Row {

	for i, v := range slice {
		if v.Strings["country"] == country.Strings["country"] {
			if v.Numerics["budget_sum"] == country.Numerics["budget_sum"] {
				log.Infof("Client %s | Query Q2 | Country matched expected", cid)
				return slices.Delete(slice, i, i+1)
			} else {
				log.Errorf("Client %s | Query Q2 | Budget sum not matched expected 😔: %d != %d", cid, country.Numerics["budget_sum"], v.Numerics["budget_sum"])
			}
		}
	}
	log.Errorf("Client %s | Query Q2 | Country not matched expected 🛑", cid)
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

func removeQ3(slice []*model.Row, movie *model.Row, log *logger.ConsoleLogger, cid string) []*model.Row {

	for i, v := range slice {
		if v.Strings["title"] == movie.Strings["title"] && v.Strings["movieID"] == movie.Strings["movieID"] {
			log.Infof("Client %s | Query Q3 |Film matched expected", cid)
			return slices.Delete(slice, i, i+1)
		}
	}
	log.Errorf("Client %s | Query Q3 | Film not matched expected 🛑", cid)
	return slice
}

func removeQ4(slice []*model.Row, actor *model.Row, log *logger.ConsoleLogger, cid string) []*model.Row {

	for i, v := range slice {
		if v.Strings["actor"] == actor.Strings["actor"] {
			if v.Numerics["count"] == actor.Numerics["count"] {
				log.Infof("Client %s | Query Q4 | Actor matched expected", cid)
				return slices.Delete(slice, i, i+1)
			} else {
				log.Errorf("Count not matched expected 😔: %d != %d", actor.Numerics["count"], v.Numerics["count"])
			}
		}
	}
	log.Errorf("Client %s | Query Q4 | Actor not matched expected 🛑", cid)
	return slice
}

func removeQ5(expectedOutputQ5 []*model.Row, receivedSentiment *model.Row, log *logger.ConsoleLogger, cid string) []*model.Row {
	for i, v := range expectedOutputQ5 {
		if v.Strings["sentiment"] == receivedSentiment.Strings["sentiment"] {
			if v.Floats["avg_rate"]-receivedSentiment.Floats["avg_rate"] < 0.0001 {
				log.Infof("Client %s | Query Q5 | Sentiment matched expected", cid)
				return slices.Delete(expectedOutputQ5, i, i+1)
			} else {
				log.Errorf("Client %s | Query Q5 | Avg rate not matched expected 😔: %f != %f", cid, receivedSentiment.Floats["avg_rate"], v.Floats["avg_rate"])
			}
		}
	}
	log.Errorf("Sentiment not matched expected 🛑")
	return expectedOutputQ5
}

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
	ctx                 context.Context
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

	select {
	case <-cr.ctx.Done():
		return 0, fmt.Errorf("read canceled by context")
	case msgEnvelope, ok := <-cr.ch:
		if !ok {
			return 0, fmt.Errorf("channel closed")
		}
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
}
