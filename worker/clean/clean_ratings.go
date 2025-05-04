package clean

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"strconv"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

type Rating struct {
	Id     uint32
	Rating uint8
}

func (r *Rating) Encode() []byte {
	buf := make([]byte, 5)
	binary.BigEndian.PutUint32(buf, r.Id)
	buf[4] = r.Rating
	return buf
}

func (r *Rating) Decode(data []byte) {
	if len(data) < 5 {
		return
	}
	r.Id = binary.BigEndian.Uint32(data[:4])
	r.Rating = data[4]
}

type CleanRatings struct {
	input        string
	taskReceiver middleware.Receiver[*model.FileChunk]
	taskSender   middleware.Sender[*model.Row]
	subscribers  map[string][]string
}

func NewCleanRatings(input task.Task[*model.Row, *model.FileChunk], subscribers map[string][]string) task.Task[*model.FileChunk, *model.Row] {
	return &CleanRatings{input.Name(), nil, nil, subscribers}
}

func (f CleanRatings) Input() string {
	return f.input
}

func (f CleanRatings) Name() string {
	return "clean_ratings"
}

func (f CleanRatings) ProcessAndSend(row *model.FileChunk, cid string) error {
	output := f.process(row.Bytes) // TODO: this should be a different model
	if output == nil {
		return nil
	}
	movieId := output.Strings["movieID"]
	routingKey := string(movieId[len(movieId)-1])
	return f.taskSender.SendRK(output, routingKey, cid)
}

func (f CleanRatings) process(row []byte) *model.Row {
	rating := &Rating{}
	rating.Decode(row)
	// requiredFields := []string{
	// 	row.Strings["movieID"],
	// 	row.Strings["rating"],
	// }

	// log.Debugf("Clean: movieID: %s, rating: %s",
	// 	row.Strings["movieID"],
	// 	row.Strings["rating"],
	// )

	// for _, field := range requiredFields {
	// 	if utils.MustDropRow(field) {
	// 		log.Debugf("warning: dropping row due to empty field: %s", field)
	// 		return nil
	// 	}
	// }
	// rating, ok := utils.ParseFloat(row.Strings["rating"])
	// rating := strings.ReplaceAll(row.Strings["rating"], ".", "")
	// ratingUint, err := strconv.ParseUint(rating, 10, 8)

	// if err != nil {
	// 	log.Warnf("could not parse rating: %s", row.Strings["rating"])
	// 	return nil
	// }

	// log.Debugf("Clean ALL: movieID: %s, rating: %v", row.Strings["movieID"], rating)

	res := &model.Row{
		Strings: map[string]string{
			"movieID": fmt.Sprintf("%d", rating.Id),
		},
		Numerics: map[string]uint64{
			"rating": uint64(rating.Rating),
		}, //Todo use codec.Decimals
	}
	// log.Debugf("rating: %v", res)
	return res
}

func (f *CleanRatings) Connect(middIn middleware.Connection[*model.FileChunk], middOut middleware.Connection[*model.Row]) ([]chan middleware.Envelope[*model.FileChunk], error) {
	var err error
	prefetch, err := strconv.Atoi(os.Getenv("PREFETCH"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse PREFETCH: %w", err)
	}
	n_workers, err := strconv.Atoi(os.Getenv("N_WORKERS"))
	if err != nil {
		return nil, fmt.Errorf("failed to parse N_WORKERS: %w", err)
	}
	f.taskReceiver, err = middIn.ConsumeFrom(f.Input(), f.Name(), prefetch, n_workers)
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue for task %s", f.Name())
	}
	f.taskSender, err = middOut.WriteToRK(f.Name(), f.subscribers, "direct")
	//f.taskReceiver.LimitUnacked(10000)

	if err != nil {
		return nil, fmt.Errorf("failed to create write queue for task %s", f.Name())
	}

	//lint:ignore S1019 Ignorar reflect.Select en este archivo
	inputChannel := make(chan middleware.Envelope[*model.FileChunk], 0)
	go func() {
		for {
			ctx := context.Background()
			envelope, ok, err := f.taskReceiver.Next(ctx)
			if err != nil {
				if err.Error() == "read channel was closed" || err.Error() == "close channel was closed" {
					log.Infof("Channel closed: %v", f.Name())
					break
				}
				log.Errorf("Error reading from middleware: %v", err)
				continue
			}
			if !ok {
				log.Infof("Channel closed: %v", f.Name())
				break
			}
			inputChannel <- envelope
		}
		close(inputChannel)
	}()
	channels := []chan middleware.Envelope[*model.FileChunk]{inputChannel}

	return channels, nil
}

func (f *CleanRatings) Finish() error {
	if err := f.taskReceiver.Close(); err != nil {
		return fmt.Errorf("failed to close task receiver: %w", err)
	}
	if err := f.taskSender.Close(); err != nil {
		return fmt.Errorf("failed to close task sender: %w", err)
	}
	log.Infof("Closed task %s", f.Name())
	return nil
}
