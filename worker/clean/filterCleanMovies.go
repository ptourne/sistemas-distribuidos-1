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
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("worker_%s", WORKER_ID), logger.Debug)

type CleanMovies struct{ input task.Task }

func NewCleanMovies(input task.Task) task.Task {
	return &CleanMovies{input}
}

func (f CleanMovies) Input() string {
	return f.input.Name()
}

func (f CleanMovies) Name() string {
	return "clean_movies"
}

func (f CleanMovies) Process(row common.Row) *common.Row {
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

	budget, ok := parseFloat(row.Strings["budget"])
	if !ok {
		log.Warnf("could not parse budget: %s", row.Strings["budget"])
		return nil
	}

	revenue, ok := parseFloat(row.Strings["revenue"])
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
		},
		Floats: map[string]float64{
			"budget":  budget,
			"revenue": revenue,
		},
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

func parseFloat(s string) (float64, bool) { // TODO: Handle empty strings
	if s == "" {
		return 0.0, false
	}
	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		log.Errorf("Failed to parse float: %s", err)
		return 0.0, false
	}
	return value, true
}
