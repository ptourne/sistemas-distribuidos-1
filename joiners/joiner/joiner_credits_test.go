package joiner

import (
	"context"
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
				count, exists := clientsFinished[envelope.Cid()]
				if !exists {
					clientsFinished[envelope.Cid()] = 1
				} else {
					clientsFinished[envelope.Cid()] = count + 1
				}
			} else if !ok {

				t.Logf("Channel closed 0, exiting...")
				closed++
				inputChannels[0] = nil

			}
		case envelope, ok = <-inputChannels[1]:
			t.Logf("Credits: ok = %v: type = %v", ok, envelope.Type())
			if envelope.Type() == middleware.EOF {
				t.Log("EOF received from credits")

				count, exists := clientsFinished[envelope.Cid()]
				if !exists {
					clientsFinished[envelope.Cid()] = 1
				} else {
					clientsFinished[envelope.Cid()] = count + 1
				}
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
