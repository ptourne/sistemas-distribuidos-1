package joiner

import (
	"testing"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
	"github.com/stretchr/testify/assert"
)

func TestJoinerRatingsOneClient(t *testing.T) {

	middlewareConnection := connectToRabbitMQ(t)
	defer func() {
		runCommand(t, "docker", "stop", "rabbitmq")
		runCommand(t, "docker", "rm", "rabbitmq")
	}()

	inputMovies, inputRatings, currentTask, outputJoiner, inputChannels := configTestJoinerRatings(t, "output_test1_ratings", middlewareConnection)

	cid := "client1"
	sendRows(t, inputMovies, cid,
		&model.Row{Strings: map[string]string{"movieID": "A", "title": "Movie A"}},
		&model.Row{Strings: map[string]string{"movieID": "B", "title": "Movie B"}},
	)

	sendRows(t, inputRatings, cid,
		&model.Row{
			Strings: map[string]string{"movieID": "A"},
			Floats:  map[string]float64{"avg_rating": 3.5},
		})

	clientsFinished := map[string]int{cid: 0}
	processJoinerMessages(t, inputChannels, currentTask, clientsFinished)

	// Verificar salida
	assertReceiveRatingsRows(t, outputJoiner, cid, "A", "Movie A", 3.5)
	expectNoMoreRows(t, outputJoiner)

	currentTask.Finish()
	inputMovies.Close()
	inputRatings.Close()
	outputJoiner.Close()
	middlewareConnection.Close()
}

func TestJoinerRatingsMultiClient(t *testing.T) {

	middlewareConnection := connectToRabbitMQ(t)
	defer func() {
		runCommand(t, "docker", "stop", "rabbitmq")
		runCommand(t, "docker", "rm", "rabbitmq")
	}()

	inputMovies, inputRatings, currentTask, outputJoiner, inputChannels := configTestJoinerRatings(t, "output_test2_ratings", middlewareConnection)

	cid := "client1"
	sendRows(t, inputMovies, cid,
		&model.Row{Strings: map[string]string{"movieID": "A", "title": "Movie A"}},
		&model.Row{Strings: map[string]string{"movieID": "B", "title": "Movie B"}},
	)

	sendRows(t, inputRatings, cid,
		&model.Row{
			Strings: map[string]string{"movieID": "A"},
			Floats:  map[string]float64{"avg_rating": 3.5},
		})

	cid = "client2"
	sendRows(t, inputMovies, cid,
		&model.Row{Strings: map[string]string{"movieID": "C", "title": "Movie C"}},
		&model.Row{Strings: map[string]string{"movieID": "D", "title": "Movie D"}},
	)

	sendRows(t, inputRatings, cid,
		&model.Row{
			Strings: map[string]string{"movieID": "D"},
			Floats:  map[string]float64{"avg_rating": 1.0},
		})

	clientsFinished := map[string]int{
		"client1": 0,
		"client2": 0,
	}
	processJoinerMessages(t, inputChannels, currentTask, clientsFinished)

	// Verificar salida
	assertReceiveRatingsRows(t, outputJoiner, "client1", "A", "Movie A", 3.5)
	assertReceiveRatingsRows(t, outputJoiner, "client2", "D", "Movie D", 1.0)
	expectNoMoreRows(t, outputJoiner)

	currentTask.Finish()
	inputMovies.Close()
	inputRatings.Close()
	outputJoiner.Close()
	middlewareConnection.Close()
}

func configTestJoinerRatings(t *testing.T, output string, middlewareConnection middleware.Connection[*model.Row]) (middleware.Sender[*model.Row], middleware.Sender[*model.Row], task.JoinerTask[*model.Row, *model.Row], middleware.Receiver[*model.Row], []chan middleware.Envelope[*model.Row]) {
	movies := NewSourceTask[*model.Row]("filter_release_date_ge_2000_and_include_ar")
	ratings := NewSourceTask[*model.Row]("filter_avg_rating")
	subscribers := []string{"joiner_1_ratings"}
	inputMovies, err := middlewareConnection.WriteTo(movies.Name(), subscribers)
	assert.NoError(t, err)
	inputCredits, err := middlewareConnection.WriteTo(ratings.Name(), subscribers)
	assert.NoError(t, err)
	currentTask := NewJoinerRatings(movies, ratings, []string{output})
	outputJoiner, err := middlewareConnection.ConsumeFrom(currentTask.Name(), output, 0, 20)
	assert.NoError(t, err)

	inputChannels, err := currentTask.Connect(middlewareConnection, middlewareConnection)
	assert.NoError(t, err)
	return inputMovies, inputCredits, currentTask, outputJoiner, inputChannels
}
