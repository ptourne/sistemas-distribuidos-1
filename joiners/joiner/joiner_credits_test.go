package joiner

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
	"github.com/ptourne/sistemas-distribuidos-1/worker/task"
	"github.com/stretchr/testify/assert"
)

func TestJoinerCreditsOneClient(t *testing.T) {

	middlewareConnection := connectToRabbitMQ(t)
	defer func() {
		runCommand(t, "docker", "stop", "rabbitmq")
		runCommand(t, "docker", "rm", "rabbitmq")
	}()

	inputMovies, inputCredits, currentTask, outputJoiner, inputChannels := configTestJoinerCredits(t, "output_test1_credits", middlewareConnection)

	cid := "client1"
	sendRows(t, inputMovies, cid,
		&model.Row{Strings: map[string]string{"movieID": "A"}},
		&model.Row{Strings: map[string]string{"movieID": "B"}},
	)
	sendRows(t, inputCredits, cid,
		&model.Row{
			Strings: map[string]string{"ID": "A"},
			Arrays:  map[string][]string{"cast": {"Actor 1", "Actor 2"}},
		})

	clientsFinished := map[string]int{cid: 0}
	processJoinerMessages(t, inputChannels, currentTask, clientsFinished)

	// Verificar salida
	assertReceiveCastRows(t, outputJoiner, cid, "A", "Actor 1")
	assertReceiveCastRows(t, outputJoiner, cid, "A", "Actor 2")
	assertReceivedEOF(t, outputJoiner, cid)
	expectNoMoreRows(t, outputJoiner)

	currentTask.Finish()
	inputMovies.Close()
	inputCredits.Close()
	outputJoiner.Close()
	middlewareConnection.Close()
}

func TestJoinerCreditsMultipleClients(t *testing.T) {

	middlewareConnection := connectToRabbitMQ(t)
	defer func() {
		runCommand(t, "docker", "stop", "rabbitmq")
		runCommand(t, "docker", "rm", "rabbitmq")
	}()

	inputMovies, inputCredits, currentTask, outputJoiner, inputChannels := configTestJoinerCredits(t, "output_test2_credits", middlewareConnection)

	// Cliente 1
	cid := "client1"
	sendRows(t, inputMovies, cid,
		&model.Row{Strings: map[string]string{"movieID": "X"}},
	)
	sendRows(t, inputCredits, cid,
		&model.Row{
			Strings: map[string]string{"ID": "X"},
			Arrays:  map[string][]string{"cast": {"Actor X"}},
		})

	// Cliente 2: película sin créditos
	cid = "client2"
	sendRows(t, inputMovies, cid,
		&model.Row{Strings: map[string]string{"movieID": "Y"}},
		&model.Row{Strings: map[string]string{"movieID": "Z"}},
	)
	sendRows(t, inputCredits, cid, &model.Row{
		Strings: map[string]string{"ID": "A"},
		Arrays:  map[string][]string{"cast": {"Actor A"}},
	})

	clientsFinished := map[string]int{"client1": 0, "client2": 0}
	processJoinerMessages(t, inputChannels, currentTask, clientsFinished)

	// Cliente 1
	assertReceiveCastRows(t, outputJoiner, "client1", "X", "Actor X")
	assertReceivedEOF(t, outputJoiner, "client1")
	// Cliente 2: no debería producir salida (no tiene créditos)
	assertReceivedEOF(t, outputJoiner, "client2")
	expectNoMoreRows(t, outputJoiner)

	currentTask.Finish()
	inputMovies.Close()
	inputCredits.Close()
	outputJoiner.Close()
	middlewareConnection.Close()
}

type SourceTask[O codec.Serializable[O]] struct {
	name string
}

func NewSourceTask[O codec.Serializable[O]](name string) task.Task[*model.Row, O] {
	return &SourceTask[O]{name}
}

func (t *SourceTask[O]) ProcessAndSend(r *model.Row) error {
	return nil
}

func (t *SourceTask[O]) Name() string {
	return t.name
}

func (t *SourceTask[O]) Input() string {
	return ""
}

func (t *SourceTask[O]) Finish() error {
	return nil
}

func (t *SourceTask[O]) Connect(_ middleware.Connection[*model.Row], _ middleware.Connection[O]) ([]chan middleware.Envelope[*model.Row], error) {
	return nil, nil
}

func runCommand(t *testing.T, name string, args ...string) {
	cmd := exec.Command(name, args...)
	err := cmd.Run()
	if err != nil {
		t.Fatalf("error running command '%s %v': %v", name, args, err)
	}
}

func configTestJoinerCredits(t *testing.T, output string, middlewareConnection middleware.Connection[*model.Row]) (middleware.Sender[*model.Row], middleware.Sender[*model.Row], task.JoinerTask[*model.Row, *model.Row], middleware.Receiver[*model.Row], []chan middleware.Envelope[*model.Row]) {
	movies := NewSourceTask[*model.Row]("filter_release_date_ge_2000_and_include_ar")
	credits := NewSourceTask[*model.Row]("clean_credits")
	subscribers := []string{"joiner_1_credits"}
	inputMovies, err := middlewareConnection.WriteTo("filter_release_date_ge_2000_and_include_ar", subscribers)
	assert.NoError(t, err)
	inputCredits, err := middlewareConnection.WriteTo("clean_credits", subscribers)
	assert.NoError(t, err)
	currentTask := NewJoinerCredits(movies, credits, []string{output})
	outputJoiner, err := middlewareConnection.ConsumeFrom(currentTask.Name(), output, 0, 20)
	assert.NoError(t, err)

	inputChannels, err := currentTask.Connect(middlewareConnection, middlewareConnection)
	assert.NoError(t, err)
	return inputMovies, inputCredits, currentTask, outputJoiner, inputChannels
}

func connectToRabbitMQ(t *testing.T) middleware.Connection[*model.Row] {
	runCommand(t, "docker", "run", "-d", "--name", "rabbitmq", "-p", "5672:5672", "-p", "15672:15672", "rabbitmq:management")
	connector, err := rabbitmq.ConnectorCustom(rabbitmq.NewConfiguration("guest", "guest", "localhost", 5672))
	for {
		if err == nil {
			break
		}
		t.Logf("Error connecting to RabbitMQ. Retrying in 5 seconds...")
		time.Sleep(5 * time.Second)
		connector, err = rabbitmq.ConnectorCustom(rabbitmq.NewConfiguration("guest", "guest", "localhost", 5672))
	}
	return rabbitmq.NewMiddleware[*model.Row](connector)
}

func processJoinerMessages(t *testing.T, inputChannels []chan middleware.Envelope[*model.Row], currentTask task.JoinerTask[*model.Row, *model.Row], clientsFinished map[string]int) {
	closed := 0
	var envelope middleware.Envelope[*model.Row]
	var ok bool
	for {
		select {
		case envelope, ok = <-inputChannels[0]:
			t.Logf("Movies: ok = %v: type = %v", ok, envelope.Type())
			if envelope != nil && envelope.Type() == middleware.EOF {
				t.Log("EOF received from movies")
				clientsFinished[envelope.Cid()]++
			} else if !ok {
				t.Logf("Channel closed 0, exiting...")
				closed++
				inputChannels[0] = nil

			}
		case envelope, ok = <-inputChannels[1]:
			t.Logf("Credits: ok = %v: type = %v", ok, envelope.Type())
			if envelope != nil && envelope.Type() == middleware.EOF {
				t.Log("EOF received from credits")
				clientsFinished[envelope.Cid()]++
				t.Logf("Client finished sending credits")
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
		if !ok && (envelope == nil || envelope.Type() != middleware.EOF) {
			continue
		}

		if envelope != nil && envelope.Type() == middleware.EOF {
			count, exists := clientsFinished[envelope.Cid()]
			if !exists {
				t.Errorf("Client %s finished but not registered", envelope.Cid())
				continue
			}
			if count == 2 {
				t.Logf("Client %s finished", envelope.Cid())
				delete(clientsFinished, envelope.Cid())
				err := currentTask.FinishProcessingClient(envelope.Cid())
				assert.NoError(t, err)
				t.Logf("Finished processing client %s", envelope.Cid())
			}
			if len(clientsFinished) == 0 {
				t.Logf("All clients finished")
				break
			}
			continue
		}

		row := envelope.Msg()
		row.Strings["cid"] = envelope.Cid()
		t.Logf("Processing row: %+v from client %s", row, envelope.Cid())
		result := currentTask.ProcessAndSend(row)
		if result != nil {
			t.Errorf("Failed to process row: %v by task: %v", row, currentTask.Name())
			continue
		}
		err := envelope.Ack(false)
		assert.NoError(t, err)
	}
}

func sendRows(t *testing.T, sender middleware.Sender[*model.Row], cid string, rows ...*model.Row) {
	for _, row := range rows {
		err := sender.Send(row, cid)
		assert.NoError(t, err)
	}
	err := sender.SendEOF(cid)
	assert.NoError(t, err)
}

func expectNoMoreRows(t *testing.T, output middleware.Receiver[*model.Row]) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, _, err := output.Next(ctx)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "timeout reached while waiting for message")
}

func assertReceiveCastRows(t *testing.T, outputJoiner middleware.Receiver[*model.Row], cid string, expectedMovieID string, expectedActor string) {
	assertReceiveRow(t, outputJoiner, cid,
		map[string]string{"movieID": expectedMovieID, "actor": expectedActor},
		map[string]float64{},
	)
}

func assertReceiveRatingsRows(t *testing.T, outputJoiner middleware.Receiver[*model.Row], cid string, expectedMovieID string, expectedTitle string, expectedRating float64) {
	assertReceiveRow(t, outputJoiner, cid,
		map[string]string{"movieID": expectedMovieID, "title": expectedTitle},
		map[string]float64{"avg_rating": expectedRating},
	)
}

func assertReceiveRow(
	t *testing.T,
	output middleware.Receiver[*model.Row],
	cid string,
	expectedStrings map[string]string,
	expectedFloats map[string]float64,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	env, ok, err := output.Next(ctx)
	assert.NoError(t, err)
	assert.True(t, ok)

	msg := env.Msg()
	t.Logf("received envelope: %+v by %s", msg, env.Cid())

	for key, expected := range expectedStrings {
		assert.Equal(t, expected, msg.Strings[key], "String field mismatch for key '%s'", key)
	}
	for key, expected := range expectedFloats {
		assert.Equal(t, expected, msg.Floats[key], "Float field mismatch for key '%s'", key)
	}
	assert.Equal(t, cid, env.Cid())
}

func assertReceivedEOF(t *testing.T, output middleware.Receiver[*model.Row], cid string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	env, _, err := output.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, middleware.EOF, env.Type())
	assert.Equal(t, cid, env.Cid())
}
