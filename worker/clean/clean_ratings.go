package clean

// import (
// 	"context"
// 	"fmt"
// 	"os"
// 	"strconv"

// 	"github.com/ptourne/sistemas-distribuidos-1/common/model"
// 	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
// 	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
// )

// type CleanRatings struct {
// 	input        string
// 	taskReceiver middleware.Receiver[*model.Rating]
// 	taskSender   middleware.Sender[*model.FileChunk]
// 	subscribers  map[string][]string
// }

// func NewCleanRatings(input task.Task[*model.Row, *model.FileChunk], subscribers map[string][]string) task.Task[*model.FileChunk, *model.FileChunk] {
// 	return &CleanRatings{input.Name(), nil, nil, subscribers}
// }

// func (f CleanRatings) Input() string {
// 	return f.input
// }

// func (f CleanRatings) Name() string {
// 	return "clean_ratings"
// }

// func (f CleanRatings) ProcessAndSend(envelope middleware.Envelope[*model.FileChunk]) error {
// 	fileChunk := envelope.Msg()
// 	cid := envelope.Cid()
// 	t := envelope.Type()
// 	switch t {
// 	case middleware.EOF:
// 		log.Infof("EOF received for cid %s in: %s", cid, f.Name())
// 		for i := range 10 {
// 			rk := fmt.Sprintf("%d", i)
// 			err := f.taskSender.SendEOFRK(rk, cid)
// 			if err != nil {
// 				return fmt.Errorf("failed to send EOFRK to %s: %w", rk, err)
// 			}
// 			log.Infof("Sent EOF to %s with cid: %s", rk, cid)
// 		}
// 		return nil
// 	case middleware.Prune:
// 		log.Infof("Prune received for cid %s in: %s", cid, f.Name())
// 		err := f.taskSender.Prune(cid)
// 		if err != nil {
// 			log.Errorf("cid %s | Prune failed in: %s with err:%s", cid, err, f.Name())
// 			envelope.Nack(true)
// 		}
// 		return nil
// 	default:
// 		log.Infof("NORMAL received for cid %s in: %s", cid, f.Name())
// 		// output := f.process(fileChunk.Bytes) // TODO: this should be a different model
// 		// if output == nil {
// 		// 	return nil
// 		// }
// 		// movieId := output.Strings["movieID"]
// 		// routingKey := string(movieId[len(movieId)-1])
// 		rating := &model.Rating{}
// 		rating.Decode(fileChunk.Bytes)
// 		movieId := fmt.Sprintf("%d", rating.Id)
// 		routingKey := string(movieId[len(movieId)-1])
// 		v := rating.Encode()
// 		return f.taskSender.SendRK(&model.FileChunk{Bytes: v}, routingKey, cid)
// 	}
// }

// func (f CleanRatings) process(row []byte) *model.Row {
// 	rating := &model.Rating{}
// 	rating.Decode(row)
// 	res := &model.Row{
// 		Strings: map[string]string{
// 			"movieID": fmt.Sprintf("%d", rating.Id),
// 		},
// 		Numerics: map[string]uint64{
// 			"rating": uint64(rating.Rating),
// 		}, //Todo use codec.Decimals
// 	}
// 	return res
// }

// func (f *CleanRatings) Connect(inputMiddleware middleware.Connection[*model.FileChunk], outputMiddleware middleware.Connection[*model.FileChunk]) ([]chan middleware.Envelope[*model.FileChunk], error) {
// 	var err error
// 	prefetch, err := strconv.Atoi(os.Getenv("PREFETCH"))
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to parse PREFETCH: %w", err)
// 	}
// 	n_workers, err := strconv.Atoi(os.Getenv("N_WORKERS"))
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to parse N_WORKERS: %w", err)
// 	}
// 	f.taskReceiver, err = inputMiddleware.ConsumeFrom(f.Input(), f.Name(), uint(n_workers), prefetch)
// 	if err != nil {
// 		return nil, fmt.Errorf("failed to create read queue for task %s", f.Name())
// 	}
// 	f.taskSender, err = outputMiddleware.WriteToRK(f.Name(), f.subscribers, "direct")
// 	//f.taskReceiver.LimitUnacked(10000)

// 	if err != nil {
// 		return nil, fmt.Errorf("failed to create write queue for task %s", f.Name())
// 	}

// 	//lint:ignore S1019 Ignorar reflect.Select en este archivo
// 	inputChannel := make(chan middleware.Envelope[*model.FileChunk], 0)
// 	go func() {
// 		for {
// 			ctx := context.Background()
// 			envelope, err := f.taskReceiver.Next(ctx)
// 			if err != nil {
// 				if err.Error() == "read channel was closed" {
// 					log.Infof("Channel closed: %v", f.Name())
// 					break
// 				}
// 				log.Errorf("Error reading from middleware: %v", err)
// 				continue
// 			}
// 			switch envelope.Type() {
// 			case middleware.EOF:
// 			case middleware.Prune:
// 			}
// 			inputChannel <- envelope
// 		}
// 		close(inputChannel)
// 	}()

// 	channels := []chan middleware.Envelope[*model.FileChunk]{inputChannel}
// 	return channels, nil
// }

// func (f *CleanRatings) Finish() error {
// 	if err := f.taskReceiver.Close(); err != nil {
// 		return fmt.Errorf("failed to close task receiver: %w", err)
// 	}
// 	if err := f.taskSender.Close(); err != nil {
// 		return fmt.Errorf("failed to close task sender: %w", err)
// 	}
// 	log.Infof("Closed task %s", f.Name())
// 	return nil
// }
