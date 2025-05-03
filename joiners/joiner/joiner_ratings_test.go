package joiner

import (
	"context"
	"testing"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
	"github.com/stretchr/testify/assert"
)

func TestJoinerRatingsOneClient(t *testing.T) {

	runCommand(t, "docker", "run", "-d", "--name", "rabbitmq", "-p", "5672:5672", "-p", "15672:15672", "rabbitmq:management")

	defer func() {
		runCommand(t, "docker", "stop", "rabbitmq")
		runCommand(t, "docker", "rm", "rabbitmq")
	}()
	connector, err := rabbitmq.ConnectorCustom(rabbitmq.NewConfiguration("guest", "guest", "localhost", 5672))
	for {
		if err == nil {
			break
		}
		t.Logf("Error connecting to RabbitMQ. Retrying in 5 seconds...")
		time.Sleep(5 * time.Second)
		connector, err = rabbitmq.ConnectorCustom(rabbitmq.NewConfiguration("guest", "guest", "localhost", 5672))
	}
	middlewareConnection := rabbitmq.NewMiddleware[*model.Row](connector)
	movies := NewSourceTask[*model.Row]("filter_release_date_ge_2000_and_include_ar")
	ratings := NewSourceTask[*model.Row]("filter_avg_rating")
	subscribers := []string{"joiner_1_ratings"}
	inputMovies, err := middlewareConnection.WriteTo(movies.Name(), subscribers)
	assert.NoError(t, err)
	inputRatings, err := middlewareConnection.WriteTo(ratings.Name(), subscribers)
	assert.NoError(t, err)
	currentTask := NewJoinerRatings(movies, ratings, []string{"output_test1_ratings"})
	outputJoiner, err := middlewareConnection.ConsumeFrom(currentTask.Name(), "output_test1_ratings", 0, 20)
	assert.NoError(t, err)

	inputChannels, err := currentTask.Connect(middlewareConnection, middlewareConnection)
	assert.NoError(t, err)
	cid := "client1"

	err = inputMovies.Send(&model.Row{Strings: map[string]string{"movieID": "A", "title": "Movie A"}}, cid)
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

	closed := 0
	clientsFinished := make(map[string]int)
	var envelope middleware.Envelope[*model.Row]
	var ok bool
	for {
		select {
		case envelope, ok = <-inputChannels[0]:
			t.Logf("Movies: ok = %v: type = %v", ok, envelope.Type())
			if envelope.Type() == middleware.EOF {
				t.Log("EOF received from movies")
				clientsFinished[envelope.Cid()]++
			} else if !ok {
				t.Logf("Channel closed 0, exiting...")
				closed++
				inputChannels[0] = nil

			}
		case envelope, ok = <-inputChannels[1]:
			t.Logf("Ratings: ok = %v: type = %v", ok, envelope.Type())
			if envelope.Type() == middleware.EOF {
				t.Log("EOF received from ratings")
				clientsFinished[envelope.Cid()]++
				t.Logf("Client finished sending ratings")
				currentTask.ProcessPendingMovies(envelope.Cid())
			} else if !ok {
				t.Logf("Channel closed 1, exiting...")
				inputChannels[1] = nil
				closed++
			}

		}

		if closed == 2 {
			break
		}
		if !ok && envelope.Type() != middleware.EOF {
			continue
		}

		if envelope.Type() == middleware.EOF {
			count, exists := clientsFinished[envelope.Cid()]
			if !exists {
				t.Errorf("Client %s finished but not registered", envelope.Cid())
				continue
			}
			if count == 2 {
				t.Logf("Client %s finished", envelope.Cid())
				delete(clientsFinished, envelope.Cid())
				err = currentTask.FinishProcessingClient(envelope.Cid())
				assert.NoError(t, err)
				t.Logf("Finished processing client %s", envelope.Cid())
				break
			} else {
				continue
			}
		}

		row := envelope.Msg()
		row.Strings["cid"] = envelope.Cid()
		result := currentTask.ProcessAndSend(row)
		if result != nil {
			t.Errorf("Failed to process row: %v by task: %v", row, currentTask.Name())
			continue
		}
		err = envelope.Ack(false)
		assert.NoError(t, err)
	}

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
