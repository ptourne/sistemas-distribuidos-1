package test

import (
	"testing"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/joiners_ratings_workers/joiner_ratings_worker/ratings"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/stretchr/testify/assert"
)

func TestJoinerRatingsOneClient(t *testing.T) {
	//t.Skip("TestJoinerRatingsOneClient: Test not updated yet")

	middlewareConnection := ConnectToRabbitMQ(t)
	defer func() {
		RunCommand(t, "docker", "stop", "rabbitmq")
		RunCommand(t, "docker", "rm", "rabbitmq")
	}()

	inputMovies, inputRatings, worker, outputJoiner := configTestJoinerRatings(t, "output_test1_ratings", middlewareConnection)

	cid := "client1"
	SendRows(t, inputMovies, cid,
		&model.Row{Strings: map[string]string{"movieID": "A", "title": "Movie A"}},
		&model.Row{Strings: map[string]string{"movieID": "B", "title": "Movie B"}},
	)

	SendRows(t, inputRatings, cid,
		&model.Row{
			Strings: map[string]string{"movieID": "A"},
			Floats:  map[string]float64{"avg_rating": 3.5},
		})

	go func() {
		worker.Run(middlewareConnection)
	}()

	// Verificar salida
	assertReceiveRatingsRows(t, outputJoiner, cid, "A", "Movie A", 3.5)
	AssertReceivedEOF(t, outputJoiner, cid)
	ExpectNoMoreRows(t, outputJoiner)
	//ToDO: expect receive prune msg

	//currentTask.Finish()
	inputMovies.Close()
	inputRatings.Close()
	outputJoiner.Close()
	middlewareConnection.Close()
}

func TestJoinerRatingsMultiClient(t *testing.T) {
	//t.Skip("TestJoinerRatingsMultiClient: Test not updated yet")

	middlewareConnection := ConnectToRabbitMQ(t)
	defer func() {
		RunCommand(t, "docker", "stop", "rabbitmq")
		RunCommand(t, "docker", "rm", "rabbitmq")
	}()

	inputMovies, inputRatings, worker, outputJoiner := configTestJoinerRatings(t, "output_test2_ratings", middlewareConnection)

	cid := "client1"
	SendRows(t, inputMovies, cid,
		&model.Row{Strings: map[string]string{"movieID": "A", "title": "Movie A"}},
		&model.Row{Strings: map[string]string{"movieID": "B", "title": "Movie B"}},
	)

	SendRows(t, inputRatings, cid,
		&model.Row{
			Strings: map[string]string{"movieID": "A"},
			Floats:  map[string]float64{"avg_rating": 3.5},
		})

	cid = "client2"
	SendRows(t, inputMovies, cid,
		&model.Row{Strings: map[string]string{"movieID": "C", "title": "Movie C"}},
		&model.Row{Strings: map[string]string{"movieID": "D", "title": "Movie D"}},
	)

	SendRows(t, inputRatings, cid,
		&model.Row{
			Strings: map[string]string{"movieID": "D"},
			Floats:  map[string]float64{"avg_rating": 1.0},
		})

	go func() {
		worker.Run(middlewareConnection)
	}()

	// Verificar salida
	assertReceiveRatingsRows(t, outputJoiner, "client1", "A", "Movie A", 3.5)
	assertReceiveRatingsRows(t, outputJoiner, "client2", "D", "Movie D", 1.0)
	AssertReceivedEOF(t, outputJoiner, "client1") // xq primero lee el prefetch, q incluye los mensajes de client2|
	AssertReceivedEOF(t, outputJoiner, "client2")
	ExpectNoMoreRows(t, outputJoiner)

	//currentTask.Finish()
	inputMovies.Close()
	inputRatings.Close()
	outputJoiner.Close()
	middlewareConnection.Close()
}

func configTestJoinerRatings(t *testing.T, output string, middlewareConnection middleware.Connection[*model.Row]) (middleware.Sender[*model.Row], middleware.Sender[*model.Row], ratings.Worker, middleware.Receiver[*model.Row]) {
	movies := ratings.NewSourceTask[*model.Row]("filter_release_date_ge_2000_and_include_ar")
	filter_ratings := ratings.NewSourceTask[*model.Row]("filter_avg_rating")
	inputMovies, err := middlewareConnection.WriteTo(movies.Name(), []string{"joiner_ratings"})
	assert.NoError(t, err)
	inputCredits, err := middlewareConnection.WriteTo(filter_ratings.Name(), []string{"joiner_1_ratings"})
	assert.NoError(t, err)
	worker := ratings.NewWorker([]string{output})
	currentTask := worker.Tasks
	outputJoiner, err := middlewareConnection.ConsumeFrom(currentTask.Name(), output, 0, 20)
	assert.NoError(t, err)
	assert.NoError(t, err)
	return inputMovies, inputCredits, worker, outputJoiner
}

func assertReceiveRatingsRows(t *testing.T, outputJoiner middleware.Receiver[*model.Row], cid string, expectedMovieID string, expectedTitle string, expectedRating float64) {
	AssertReceiveRow(t, outputJoiner, cid,
		map[string]string{"movieID": expectedMovieID, "title": expectedTitle},
		map[string]float64{"avg_rating": expectedRating},
	)
}
