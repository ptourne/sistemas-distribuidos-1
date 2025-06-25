package clean

import (
	"context"
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/common/utils"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

type CleanMovies struct {
	input         task.Task[*model.Row, *model.Row]
	taskReceiver  middleware.Receiver[*model.Row]
	taskSender    middleware.Sender[*model.Row]
	subscribers   []string
	cantConsumers uint
	cantWorkers   uint
}

func NewCleanMovies(input task.Task[*model.Row, *model.Row], subscribers []string, nConsumers uint, cantWorkers uint) task.Task[*model.Row, *model.Row] {
	return &CleanMovies{input, nil, nil, subscribers, nConsumers, cantWorkers}
}

func (f CleanMovies) Input() string {
	return f.input.Name()
}

func (f CleanMovies) Name() string {
	return "clean_movies"
}

func (f CleanMovies) CantConsumers() uint {
	return f.cantConsumers
}

func (f CleanMovies) CantWorkers() uint {
	return f.cantWorkers
}

func (f CleanMovies) ProcessAndSend(envelope middleware.Envelope[*model.Row]) error {
	row := envelope.Msg()
	cid := envelope.Cid()
	id := envelope.Id()
	t := envelope.Type()

	switch t {
	case middleware.EOF:
		// log.Infof("EOF arrived for cid: %s in %s", cid, f.Name())

		err := f.taskSender.SendEOF(cid)
		if err != nil {
			return fmt.Errorf("failed to send EOF: %w", err)
		}
		return nil
	case middleware.Prune:
		err := f.taskSender.Prune(cid)
		if err != nil {
			return fmt.Errorf("cid %d | Prune failed in: %s with err:%s", cid, err, f.Name())
		}
		return nil
	default:
		// log.Infof("NORMAL arrived for cid: %s in %s movieID: %s", cid, f.Name(), row.Strings["movieID"])
		output := f.process(row)
		if output == nil {
			return nil
		}
		err := f.taskSender.Send(output, cid, id)
		if err != nil {
			return fmt.Errorf("failed to send message: %w", err)
		}
		return nil
	}
}

func (f CleanMovies) process(row *model.Row) *model.Row {
	requiredFields := []string{
		row.Strings["movieID"],
		row.Strings["title"],
		row.Strings["overview"],
		row.Strings["production_countries"],
		row.Strings["genres"],
		row.Strings["release_date"],
		row.Strings["budget"],
		row.Strings["revenue"],
	}

	log.Debugf("Clean: movieID: %s, title: %s, overview: %s, production_countries: %s, genres: %s, release_date: %s, budget: %s, revenue: %s",
		row.Strings["movieID"],
		row.Strings["title"],
		row.Strings["overview"],
		row.Strings["production_countries"],
		row.Strings["genres"],
		row.Strings["release_date"],
		row.Strings["budget"],
		row.Strings["revenue"],
	)

	for _, field := range requiredFields {
		if utils.MustDropRow(field) {
			log.Debugf("warning: dropping row due to empty field: %s", field)
			return nil
		}
	}

	productionCountries, ok := utils.DictionaryToListIso(row.Strings["production_countries"])
	if !ok {
		log.Debugf("warning: could not parse production countries: %s", row.Strings["production_countries"])
		return nil
	}

	genres, err := utils.DictionaryToListName(row.Strings["genres"])
	if err != nil {
		log.Warnf("could not parse genres: %s", row.Strings["genres"])
		return nil
	}

	releaseYear, ok := utils.ExtractYear(row.Strings["release_date"])
	if !ok {
		log.Warnf("could not parse release date: %s", row.Strings["release_date"])
		return nil
	}

	budget, ok := utils.ParseUint64(row.Strings["budget"])
	if !ok {
		log.Warnf("could not parse budget: %s", row.Strings["budget"])
		return nil
	}

	revenue, ok := utils.ParseUint64(row.Strings["revenue"])
	if !ok {
		log.Warnf("could not parse revenue: %s", row.Strings["revenue"])
		return nil
	}

	log.Debugf("Clean ALL: title: %s, production_countries: %v, release_date: %v", row.Strings["title"], productionCountries, releaseYear)

	return &model.Row{
		Strings: map[string]string{
			"movieID":  row.Strings["movieID"],
			"title":    row.Strings["title"],
			"overview": row.Strings["overview"],
		},
		Arrays: map[string][]string{
			"production_countries": productionCountries,
			"genres":               genres,
		},
		Numerics: map[string]uint64{
			"release_date": uint64(releaseYear),
			"budget":       budget,
			"revenue":      revenue,
		},
		Floats: map[string]float64{},
	}
}

func (f *CleanMovies) Connect(inputMiddleware middleware.Connection[*model.Row], outputMiddleware middleware.Connection[*model.Row]) ([]chan middleware.Envelope[*model.Row], error) {
	var err error
	// prefetch, err := strconv.Atoi(os.Getenv("PREFETCH"))
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to parse PREFETCH: %w", err)
	// }
	prefetch := 1000
	workerId := os.Getenv("WORKER_ID")
	if workerId == "" {
		return nil, fmt.Errorf("WORKER_ID environment variable is not set")
	}

	f.taskReceiver, err = inputMiddleware.ConsumeFrom(f.Input(), f.Name(), workerId, prefetch, f.CantWorkers())
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue for task %s", f.Name())
	}
	f.taskSender, err = outputMiddleware.WriteTo(f.Name(), f.subscribers, workerId, f.CantConsumers())
	if err != nil {
		return nil, fmt.Errorf("failed to create write queue for task %s", f.Name())
	}

	//lint:ignore S1019 Ignorar reflect.Select en este archivo
	inputChannel := make(chan middleware.Envelope[*model.Row], 0)
	go func() {
		for {
			ctx := context.Background()
			envelope, err := f.taskReceiver.Next(ctx)
			if err != nil {
				if err.Error() == "read channel was closed" || err.Error() == "close channel was closed" {
					log.Infof("Channel closed: %v", f.Name())
					break
				}
				log.Errorf("Error reading from middleware: %v", err)
				continue
			}
			switch envelope.Type() {
			case middleware.EOF:
				// log.Infof("finish arrived for cid: %s from clean_movies", envelope.Cid())
			case middleware.Prune:
				// log.Infof("Prune arrived for cid: %s in %s", envelope.Cid(), f.Name())
			default:
				// log.Infof("NORMAL arrived for cid: %s in %s", envelope.Cid(), f.Name())
			}
			inputChannel <- envelope
		}
		close(inputChannel)
	}()

	channels := []chan middleware.Envelope[*model.Row]{inputChannel}
	return channels, nil
}

func (f *CleanMovies) Finish() error {
	if err := f.taskReceiver.Close(); err != nil {
		return fmt.Errorf("failed to close task receiver: %w", err)
	}
	if err := f.taskSender.Close(); err != nil {
		return fmt.Errorf("failed to close task sender: %w", err)
	}
	log.Infof("Closed task %s", f.Name())
	return nil
}
