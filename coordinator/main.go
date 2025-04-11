package main

import (
	"bufio"
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	unwrap(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	unwrap(err, "Failed to open a channel")
	defer ch.Close()

	err = ch.ExchangeDeclare(
		"films",  // name
		"fanout", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	unwrap(err, "Failed to declare an exchange")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	file, err := os.Open("../datasets/movies_metadata.csv")
	unwrap(err, "Failed to open CSV file")
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		if strings.Contains(line, "adult") {
			continue
		}
		data := strings.Split(line, ",")
		if len(data) < 24 {
			continue
		}

		film := Film(data)

		buf, err := json.Marshal(film)
		if err != nil {
			log.Printf("Failed to encode film: %v", err)
			continue
		}

		unwrap(err, "Failed to encode film")

		err = ch.PublishWithContext(ctx,
			"movies_metadata", // exchange
			"",                // routing key
			false,             // mandatory
			false,             // immediate
			amqp.Publishing{
				ContentType: "text/json",
				Body:        buf,
			})
		unwrap(err, "Failed to publish a message")

		log.Printf(" [x] Sent %s", film.Strings["title"])
		time.Sleep(1 * time.Second)
	}

	// spanish_and_argentinian_films := []common.Row{
	// 	Film("1", "Camila", []string{"AR"}, 2001, []string{"Drama", "Romance"}),
	// 	Film("2", "Alcarràs", []string{"ES"}, 2001, []string{"Drama", "Romance"}),
	// 	Film("3", "La La Land", []string{"US"}, 1999, []string{"Drama", "Romance"}),
	// 	Film("4", "El secreto de sus ojos", []string{"AR"}, 2001, []string{"Drama", "Romance"}),
	// 	Film("5", "Relatos salvajes", []string{"AR", "ES"}, 1999, []string{"Drama", "Romance"}),
	// 	Film("6", "El buen patrón", []string{"ES"}, 2001, []string{"Drama", "Romance"}),
	// 	Film("7", "Mientras Dure la Guerra", []string{"AR", "ES"}, 1999, []string{"Drama", "Romance"}),
	// 	Film("8", "Maixabel", []string{"ES"}, 1980, []string{"Drama", "Romance"}),
	// 	Film("9", "Esperando la Carroza", []string{"AR"}, 2001, []string{"Drama", "Romance"}),
	// 	Film("10", "Tapas", []string{"AR", "ES"}, 1999, []string{"Drama", "Romance"}),
	// }

	// for {
	// 	for _, film := range spanish_and_argentinian_films {
	// 		buf, err := json.Marshal(film)
	// 		unwrap(err, "Failed to encode film")

	// 		err = ch.PublishWithContext(ctx,
	// 			"movies_metadata", // exchange
	// 			"",                // routing key
	// 			false,             // mandatory
	// 			false,             // immediate
	// 			amqp.Publishing{
	// 				ContentType: "text/json",
	// 				Body:        buf,
	// 			})
	// 		unwrap(err, "Failed to publish a message")

	// 		log.Printf(" [x] Sent %s", film.Strings["title"])
	// 		time.Sleep(1 * time.Second)
	// 	}
	// }

}

// adult,belongs_to_collection,budget,genres,homepage,id,imdb_id,original_language,original_title,overview,popularity,poster_path,production_companies,production_countries,release_date,revenue,runtime,spoken_languages,status,tagline,title,video,vote_average,vote_count

func Film(data []string) common.Row {
	adult, belongs_to_collection, budget, genres, homepage, id, imdb_id, original_language,
		original_title, overview, popularity, poster_path, production_companies, production_countries,
		release_date, revenue, runtime, spoken_languages, status, tagline, title, video, vote_average, vote_count :=
		data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
		data[8], data[9], data[10], data[11], data[12], data[13], data[14], data[15],
		data[16], data[17], data[18], data[19], data[20], data[21], data[22], data[23]

		return common.Row{
		Strings: map[string]string{
			"movieID":           id,
			"title":             title,
			"imdb_id":           imdb_id,
			"original_language": original_language,
			"original_title":    original_title,
			"homepage":          homepage,
			"overview":          overview,
			"status":            status,
			"tagline":           tagline,
			"poster_path":       poster_path,
		},
		Arrays: map[string][]string{
			"production_countries":  dictionaryToList(production_countries),
			"genres":                dictionaryToList(genres),
			"production_companies":  dictionaryToList(production_companies),
			"spoken_languages":      dictionaryToList(spoken_languages),
			"belongs_to_collection": dictionaryToList(belongs_to_collection),
		},
		Numerics: map[string]uint{
			"release_date": extractYear(release_date),
			"vote_count":   uint(parseUint(vote_count)),
		},
		Booleans: map[string]bool{
			"adult": adult == "True",
			"video": video == "True",
		},
		Floats: map[string]float64{
			"budget":       parseFloat(budget),
			"revenue":      parseFloat(revenue),
			"popularity":   parseFloat(popularity),
			"runtime":      parseFloat(runtime),
			"vote_average": parseFloat(vote_average),
		},
	}
}

func extractYear(dateStr string) uint {
	date, err := time.Parse("2006-01-02", dateStr) // Formato Go: siempre 2006-01-02
	if err != nil {
		log.Printf("warning: could not parse date '%s': %v", dateStr, err)
		return 0
	}

	return uint(date.Year())
}

func parseUint(s string) uint64 { // TODO: Handle empty strings
	if s == "" {
		return 0
	}
	val, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		unwrap(err, "Failed to parse budget")
		return 0
	}
	return val
}

type NameItem struct {
	Name string `json:"name"`
}

func dictionaryToList(input string) []string {
	var items []NameItem
	var result []string

	if input == "" || input == "[]" {
		return result
	}

	err := json.Unmarshal([]byte(input), &items)
	if err != nil {
		log.Printf("failed to unmarshal input %s: %v", input, err)
		return result
	}

	for _, item := range items {
		result = append(result, item.Name)
	}
	return result
}
func parseFloat(s string) float64 { // TODO: Handle empty strings
	if s == "" {
		return 0.0
	}
	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		log.Printf("Failed to parse float: %s", err)
		return 0.0
	}
	return value
}

func unwrap(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
