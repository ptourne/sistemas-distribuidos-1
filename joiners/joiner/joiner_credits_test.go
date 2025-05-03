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

	runCommand(t, "docker", "run", "-d", "--name", "rabbitmq", "-p", "5672:5672", "-p", "15672:15672", "rabbitmq:management")
	t.Logf("Waiting for RabbitMQ to start...")
	time.Sleep(5 * time.Second)

	defer func() {
		runCommand(t, "docker", "stop", "rabbitmq")
		runCommand(t, "docker", "rm", "rabbitmq")
	}()
	connector, err := rabbitmq.ConnectorCustom(rabbitmq.NewConfiguration("guest", "guest", "localhost", 5672))
	assert.NoError(t, err)
	middlewareConnection := rabbitmq.NewMiddleware[*model.Row](connector)
	movies := NewSourceTask[*model.Row]("filter_release_date_ge_2000_and_include_ar")
	credits := NewSourceTask[*model.Row]("clean_credits")
	subscribers := []string{"joiner_1_credits"}
	inputMovies, err := middlewareConnection.WriteTo("filter_release_date_ge_2000_and_include_ar", subscribers)
	assert.NoError(t, err)
	inputCredits, err := middlewareConnection.WriteTo("clean_credits", subscribers)
	assert.NoError(t, err)
	currentTask := NewJoinerCredits(movies, credits, []string{"output_test"})
	outputJoiner, err := middlewareConnection.ConsumeFrom(currentTask.Name(), "output_test", 0, 20)
	assert.NoError(t, err)

	inputChannels, err := currentTask.Connect(middlewareConnection, middlewareConnection)
	assert.NoError(t, err)
	cid := "client1"

	err = inputMovies.Send(&model.Row{Strings: map[string]string{"movieID": "A"}}, cid)
	assert.NoError(t, err)
	err = inputMovies.Send(&model.Row{Strings: map[string]string{"movieID": "B"}}, cid)
	assert.NoError(t, err)
	err = inputMovies.SendEOF(cid)
	assert.NoError(t, err)
	err = inputCredits.Send(&model.Row{
		Strings: map[string]string{"ID": "A"},
		Arrays:  map[string][]string{"cast": {"Actor 1", "Actor 2"}},
	}, cid)
	assert.NoError(t, err)
	err = inputCredits.SendEOF(cid)
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
			t.Logf("Credits: ok = %v: type = %v", ok, envelope.Type())
			if envelope.Type() == middleware.EOF {
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
	assert.Equal(t, "Actor 1", row.Strings["actor"])

	env, ok, err = outputJoiner.Next(ctx)
	assert.NoError(t, err)
	assert.True(t, ok, "Output channel closed unexpectedly")
	row = env.Msg()
	assert.Equal(t, "A", row.Strings["movieID"])
	assert.Equal(t, "Actor 2", row.Strings["actor"])

	currentTask.Finish()
	inputMovies.Close()
	inputCredits.Close()
	outputJoiner.Close()
	middlewareConnection.Close()
}

func TestJoinerCreditsMultipleClients(t *testing.T) {

	runCommand(t, "docker", "run", "-d", "--name", "rabbitmq", "-p", "5672:5672", "-p", "15672:15672", "rabbitmq:management")
	t.Logf("Waiting for RabbitMQ to start...")
	time.Sleep(5 * time.Second)

	defer func() {
		runCommand(t, "docker", "stop", "rabbitmq")
		runCommand(t, "docker", "rm", "rabbitmq")
	}()

	connector, err := rabbitmq.ConnectorCustom(rabbitmq.NewConfiguration("guest", "guest", "localhost", 5672))
	assert.NoError(t, err)
	middlewareConnection := rabbitmq.NewMiddleware[*model.Row](connector)
	movies := NewSourceTask[*model.Row]("filter_release_date_ge_2000_and_include_ar")
	credits := NewSourceTask[*model.Row]("clean_credits")
	subscribers := []string{"joiner_1_credits"}
	inputMovies, err := middlewareConnection.WriteTo("filter_release_date_ge_2000_and_include_ar", subscribers)
	assert.NoError(t, err)
	inputCredits, err := middlewareConnection.WriteTo("clean_credits", subscribers)
	assert.NoError(t, err)
	currentTask := NewJoinerCredits(movies, credits, []string{"output_test2"})
	outputJoiner, err := middlewareConnection.ConsumeFrom(currentTask.Name(), "output_test2", 0, 20)
	assert.NoError(t, err)

	inputChannels, err := currentTask.Connect(middlewareConnection, middlewareConnection)
	assert.NoError(t, err)

	// Cliente 1
	err = inputMovies.Send(&model.Row{Strings: map[string]string{"movieID": "X"}}, "client1")
	assert.NoError(t, err)
	err = inputCredits.Send(&model.Row{
		Strings: map[string]string{"ID": "X"},
		Arrays:  map[string][]string{"cast": {"Actor X"}},
	}, "client1")
	assert.NoError(t, err)
	err = inputMovies.SendEOF("client1")
	assert.NoError(t, err)
	err = inputCredits.SendEOF("client1")
	assert.NoError(t, err)

	// Cliente 2: película sin créditos
	err = inputMovies.Send(&model.Row{Strings: map[string]string{"movieID": "Y"}}, "client2")
	assert.NoError(t, err)
	err = inputMovies.Send(&model.Row{Strings: map[string]string{"movieID": "Z"}}, "client2")
	assert.NoError(t, err)
	err = inputMovies.SendEOF("client2")
	assert.NoError(t, err)
	err = inputCredits.Send(&model.Row{
		Strings: map[string]string{"ID": "A"},
		Arrays:  map[string][]string{"cast": {"Actor A"}},
	}, "client2")
	assert.NoError(t, err)
	err = inputCredits.SendEOF("client2")
	assert.NoError(t, err)

	closed := 0
	clientsFinished := make(map[string]int)
	var envelope middleware.Envelope[*model.Row]
	var ok bool
	for {
		select {
		case envelope, ok = <-inputChannels[0]:
			if envelope.Type() == middleware.EOF {
				clientsFinished[envelope.Cid()]++
			} else if !ok {
				inputChannels[0] = nil
				closed++
			}
		case envelope, ok = <-inputChannels[1]:
			if envelope.Type() == middleware.EOF {
				clientsFinished[envelope.Cid()]++
				currentTask.ProcessPendingMovies(envelope.Cid())
			} else if !ok {
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
			if clientsFinished[envelope.Cid()] == 2 {
				err = currentTask.FinishProcessingClient(envelope.Cid())
				assert.NoError(t, err)
				delete(clientsFinished, envelope.Cid())
			}
			if len(clientsFinished) == 0 {
				break
			}
			continue
		}

		row := envelope.Msg()
		row.Strings["cid"] = envelope.Cid()
		err := currentTask.ProcessAndSend(row)
		assert.NoError(t, err)
		err = envelope.Ack(false)
		assert.NoError(t, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Cliente 1
	env, ok, err := outputJoiner.Next(ctx)
	assert.NoError(t, err)
	assert.True(t, ok)
	t.Logf("received envelope: %+v by %s", env.Msg(), env.Cid())
	assert.Equal(t, "X", env.Msg().Strings["movieID"])
	assert.Equal(t, "Actor X", env.Msg().Strings["actor"])

	// Cliente 2: no debería producir salida (no tiene créditos)
	_, _, err = outputJoiner.Next(ctx)
	assert.Equal(t, err.Error(), "timeout reached while waiting for message")

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
