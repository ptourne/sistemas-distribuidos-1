package clean

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Debug)

type CleanMovies struct {
	input        task.Task
	taskReceiver middleware.Receiver[common.Row]
	taskSender   middleware.Sender[common.Row]
}

func NewCleanMovies(input task.Task) task.Task {
	return &CleanMovies{input, nil, nil}
}

func (f CleanMovies) Input() string {
	return f.input.Name()
}

func (f CleanMovies) Name() string {
	return "clean_movies"
}

func (f CleanMovies) ProcessAndSend(row common.Row) error {
	output := f.process(row)
	if output == nil {
		return nil
	}
	return f.taskSender.Send(output)
}

func (f CleanMovies) process(row common.Row) *common.Row {
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
		if mustDropRow(field) {
			log.Debugf("warning: dropping row due to empty field: %s", field)
			return nil
		}
	}

	productionCountries, ok := dictionaryToListIso(row.Strings["production_countries"])
	if !ok {
		log.Debugf("warning: could not parse production countries: %s", row.Strings["production_countries"])
		return nil
	}

	genres, ok := dictionaryToListName(row.Strings["genres"])
	if !ok {
		log.Warnf("could not parse genres: %s", row.Strings["genres"])
		return nil
	}

	releaseYear, ok := extractYear(row.Strings["release_date"])
	if !ok {
		log.Warnf("could not parse release date: %s", row.Strings["release_date"])
		return nil
	}

	budget, ok := parseUint(row.Strings["budget"])
	if !ok {
		log.Warnf("could not parse budget: %s", row.Strings["budget"])
		return nil
	}

	revenue, ok := parseUint(row.Strings["revenue"])
	if !ok {
		log.Warnf("could not parse revenue: %s", row.Strings["revenue"])
		return nil
	}

	log.Debugf("Clean ALL: title: %s, production_countries: %v, release_date: %v", row.Strings["title"], productionCountries, releaseYear)

	return &common.Row{
		Strings: map[string]string{
			"movieID":  row.Strings["movieID"],
			"title":    row.Strings["title"],
			"overview": row.Strings["overview"],
		},
		Arrays: map[string][]string{
			"production_countries": productionCountries,
			"genres":               genres,
		},
		Numerics: map[string]uint{
			"release_date": releaseYear,
			"budget":       budget,
			"revenue":      revenue,
		},
		Floats: map[string]float64{},
	}
}

func mustDropRow(field string) bool {
	return field == "" || field == "[]" || field == "null" // || field == "0" || field == "0.0"
}

func extractYear(dateStr string) (uint, bool) { // TODO: Handle empty strings
	if dateStr == "" {
		return 0, false
	}
	date, err := time.Parse(time.DateOnly, dateStr) // Formato Go: siempre 2006-01-02
	if err != nil {
		log.Warnf("could not parse date '%s': %v", dateStr, err)
		return 0, false
	}

	return uint(date.Year()), true
}

type NameItem struct {
	Name string `json:"name"`
}

type NameCountry struct {
	ISO string `json:"iso_3166_1"`
}

func dictionaryToListName(input string) ([]string, bool) {
	var result []string

	if input == "" || input == "[]" {
		return result, false
	}

	cleaned := strings.ReplaceAll(input, "'", "\"")
	var items []NameItem
	err := json.Unmarshal([]byte(cleaned), &items)
	if err != nil {
		var item NameItem
		err := json.Unmarshal([]byte(cleaned), &item)
		if err != nil {
			log.Errorf("failed to unmarshal input %s: %v", input, err)
			return result, false
		}
		return []string{item.Name}, true
	}

	for _, item := range items {
		result = append(result, item.Name)
	}
	return result, true
}

func dictionaryToListIso(input string) ([]string, bool) {
	var result []string

	if input == "" || input == "[]" {
		return result, false
	}

	cleaned := strings.ReplaceAll(input, "'", "\"")
	var items []NameCountry
	err := json.Unmarshal([]byte(cleaned), &items)
	if err != nil {
		var item NameCountry
		err := json.Unmarshal([]byte(cleaned), &item)
		if err != nil {
			log.Errorf("failed to unmarshal input %s: %v", input, err)
			return result, false
		}
		return []string{item.ISO}, true
	}

	for _, item := range items {
		result = append(result, item.ISO)
	}
	return result, true
}

func parseUint(s string) (uint, bool) { // TODO: Handle empty strings
	if s == "" {
		return 0, false
	}
	value, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		log.Errorf("Failed to parse uint: %s", err)
		return 0, false
	}
	return uint(value), true
}

func (f *CleanMovies) Connect(middlewareConnection middleware.MiddlewareCola[common.Row]) (chan middleware.Envelope[common.Row], error) {
	var err error
	f.taskReceiver, err = middlewareConnection.ConsumeFrom(f.Input(), f.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to create read queue for task %s", f.Name())
	}
	f.taskSender, err = middlewareConnection.WriteTo(f.Name())
	if err != nil {
		return nil, fmt.Errorf("failed to create write queue for task %s", f.Name())
	}

	//lint:ignore S1019 Ignorar reflect.Select en este archivo
	inputChannel := make(chan middleware.Envelope[common.Row], 0)
	go func() {
		for {
			envelope, ok, err := f.taskReceiver.Next(nil)
			if err != nil {
				if err.Error() == "read channel was closed" {
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
	return inputChannel, nil
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
