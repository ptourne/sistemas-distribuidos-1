package joiner

import (
	"context"
	"testing"
	"time"

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

	err := inputMovies.Send(&model.Row{Strings: map[string]string{"movieID": "A", "title": "Movie A"}}, cid)
	assert.NoError(t, err)
	err = inputMovies.Send(&model.Row{Strings: map[string]string{"movieID": "B", "title": "Movie B"}}, cid)
	assert.NoError(t, err)
	err = inputMovies.SendEOF(cid)
	assert.NoError(t, err)
	err = inputRatings.Send(&model.Row{
		Strings: map[string]string{"movieID": "A"},
		Floats:  map[string]float64{"avg_rating": 3.5},
	}, cid)
	assert.NoError(t, err)
	err = inputRatings.SendEOF(cid)
	assert.NoError(t, err)

	clientsFinished := map[string]int{cid: 0}
	processMessages(t, inputChannels, currentTask, clientsFinished)

	// Verificar salida
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	env, ok, err := outputJoiner.Next(ctx)
	assert.NoError(t, err)
	assert.True(t, ok, "Output channel closed unexpectedly")
	row := env.Msg()
	assert.Equal(t, "A", row.Strings["movieID"])
	assert.Equal(t, "Movie A", row.Strings["title"])
	assert.Equal(t, 3.5, row.Floats["avg_rating"])

	_, _, err = outputJoiner.Next(ctx)
	assert.Equal(t, err.Error(), "timeout reached while waiting for message")

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

	err := inputMovies.Send(&model.Row{Strings: map[string]string{"movieID": "A", "title": "Movie A"}}, cid)
	assert.NoError(t, err)
	err = inputMovies.Send(&model.Row{Strings: map[string]string{"movieID": "B", "title": "Movie B"}}, cid)
	assert.NoError(t, err)
	err = inputMovies.SendEOF(cid)
	assert.NoError(t, err)
	err = inputRatings.Send(&model.Row{
		Strings: map[string]string{"movieID": "A"},
		Floats:  map[string]float64{"avg_rating": 3.5},
	}, cid)
	assert.NoError(t, err)
	err = inputRatings.SendEOF(cid)
	assert.NoError(t, err)

	cid = "client2"

	err = inputMovies.Send(&model.Row{Strings: map[string]string{"movieID": "C", "title": "Movie C"}}, cid)
	assert.NoError(t, err)
	err = inputMovies.Send(&model.Row{Strings: map[string]string{"movieID": "D", "title": "Movie D"}}, cid)
	assert.NoError(t, err)
	err = inputMovies.SendEOF(cid)
	assert.NoError(t, err)
	err = inputRatings.Send(&model.Row{
		Strings: map[string]string{"movieID": "D"},
		Floats:  map[string]float64{"avg_rating": 1.0},
	}, cid)
	assert.NoError(t, err)
	err = inputRatings.SendEOF(cid)
	assert.NoError(t, err)

	clientsFinished := map[string]int{
		"client1": 0,
		"client2": 0,
	}
	processMessages(t, inputChannels, currentTask, clientsFinished)

	// Verificar salida
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	env, ok, err := outputJoiner.Next(ctx)
	assert.NoError(t, err)
	assert.True(t, ok, "Output channel closed unexpectedly")
	row := env.Msg()
	assert.Equal(t, "A", row.Strings["movieID"])
	assert.Equal(t, "Movie A", row.Strings["title"])
	assert.Equal(t, 3.5, row.Floats["avg_rating"])
	assert.Equal(t, "client1", env.Cid())
	env, ok, err = outputJoiner.Next(ctx)
	assert.NoError(t, err)
	assert.True(t, ok, "Output channel closed unexpectedly")
	row = env.Msg()
	assert.Equal(t, "D", row.Strings["movieID"])
	assert.Equal(t, "Movie D", row.Strings["title"])
	assert.Equal(t, 1.0, row.Floats["avg_rating"])
	assert.Equal(t, "client2", env.Cid())

	_, _, err = outputJoiner.Next(ctx)
	assert.Equal(t, err.Error(), "timeout reached while waiting for message")

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
