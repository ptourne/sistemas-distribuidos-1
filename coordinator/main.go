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

	file, err := os.Open("datasets/movie_metadata.csv")
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
		// adult,belongs_to_collection,budget,genres,homepage,id,imdb_id,original_language,original_title,overview,popularity,poster_path,production_companies,production_countries,release_date,revenue,runtime,spoken_languages,status,tagline,title,video,vote_average,vote_count

	}

	spanish_and_argentinian_films := []common.Row{
		Film("1", "Camila", []string{"AR"}, 2001, []string{"Drama", "Romance"}),
		Film("2", "Alcarràs", []string{"ES"}, 2001, []string{"Drama", "Romance"}),
		Film("3", "La La Land", []string{"US"}, 1999, []string{"Drama", "Romance"}),
		Film("4", "El secreto de sus ojos", []string{"AR"}, 2001, []string{"Drama", "Romance"}),
		Film("5", "Relatos salvajes", []string{"AR", "ES"}, 1999, []string{"Drama", "Romance"}),
		Film("6", "El buen patrón", []string{"ES"}, 2001, []string{"Drama", "Romance"}),
		Film("7", "Mientras Dure la Guerra", []string{"AR", "ES"}, 1999, []string{"Drama", "Romance"}),
		Film("8", "Maixabel", []string{"ES"}, 1980, []string{"Drama", "Romance"}),
		Film("9", "Esperando la Carroza", []string{"AR"}, 2001, []string{"Drama", "Romance"}),
		Film("10", "Tapas", []string{"AR", "ES"}, 1999, []string{"Drama", "Romance"}),
	}

	for {
		for _, film := range spanish_and_argentinian_films {
			buf, err := json.Marshal(film)
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
	}

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
		},
		Arrays: map[string][]string{
			"production_countries": production_countries,
			"genres":               genres,
		},
		Numerics: map[string]uint{
			"release_date": release_date,
			"budget":       uint(parseUint(budget)),
			"revenue":      uint(parseUint(revenue)),
		},
		Booleans: map[string]bool{
			"adult": adult == "True",
		},
	}
}

func parseUint(budget string) uint64 {
	budgetUint, err := strconv.ParseUint(budget, 10, 64)
	unwrap(err, "Failed to parse budget")
	return budgetUint
}

func Film(movieID string, title string, production_countries []string, release_date uint, genres []string) common.Row {
	return common.Row{
		Strings: map[string]string{
			"movieID": movieID,
			"title":   title,
		},
		Arrays: map[string][]string{
			"production_countries": production_countries,
			"genres":               genres,
			"random":               {"random1", "random2"},
		},
		Numerics: map[string]uint{
			"release_date": release_date,
		},
	}
}

func unwrap(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}
