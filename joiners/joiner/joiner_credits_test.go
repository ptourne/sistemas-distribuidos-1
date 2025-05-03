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
)

func TestJoinerCreditsSingleClient(t *testing.T) {
	// Levantar RabbitMQ
	// cmd := exec.Command("rabbitmq-server")
	// err := cmd.Start()
	// if err != nil {
	// 	t.Fatalf("Failed to start RabbitMQ: %v", err)
	// }

	// // Esperar un poco para asegurarnos de que RabbitMQ esté en ejecución
	// time.Sleep(10 * time.Second)
	connector, err := rabbitmq.ConnectorCustom(rabbitmq.NewConfiguration("guest", "guest", "localhost", 5672))
	if err != nil {
		t.Fatalf("Failed to connect to middleware: %s", err)
	}
	middlewareConnection := rabbitmq.NewMiddleware[*model.Row](connector)
	movies := NewSourceTask[*model.Row]("filter_release_date_ge_2000_and_include_ar")
	credits := NewSourceTask[*model.Row]("clean_credits")
	subscribers := []string{"joiner_1_credits"}
	inputMovies, _ := middlewareConnection.WriteTo("filter_release_date_ge_2000_and_include_ar", subscribers)

	inputCredits, _ := middlewareConnection.WriteTo("clean_credits", subscribers)
	currentTask := NewJoinerCredits(movies, credits, []string{"output_test"})
	outputJoiner, _ := middlewareConnection.ConsumeFrom(currentTask.Name(), "output_test", 0, 20)

	inputChannels, err := currentTask.Connect(middlewareConnection, middlewareConnection)
	if err != nil {
		t.Errorf("Failed to create channel for task %s: %s", currentTask.Name(), err)
	}
	cid := "client1"

	inputMovies.Send(&model.Row{
		Strings: map[string]string{
			"movieID": "A",
		},
		Arrays:   map[string][]string{},
		Numerics: map[string]uint64{},
		Floats:   map[string]float64{},
	}, cid)

	inputMovies.Send(&model.Row{
		Strings: map[string]string{
			"movieID": "B",
		},
		Arrays:   map[string][]string{},
		Numerics: map[string]uint64{},
		Floats:   map[string]float64{},
	}, cid)

	inputMovies.SendEOF(cid)
	inputCredits.Send(&model.Row{
		Strings: map[string]string{
			"ID": "A",
		},
		Arrays:   map[string][]string{"cast": {"Actor 1", "Actor 2"}},
		Numerics: map[string]uint64{},
		Floats:   map[string]float64{},
	}, cid)
	inputCredits.SendEOF(cid)

	// ID, cast
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
				if err != nil {
					t.Errorf("Failed to finish processing client %s: %v", envelope.Cid(), err)
					continue
				}
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
		if err != nil {
			t.Errorf("Failed to ack message: %v", err)
		}
	}

	// Verificar salida
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	env, ok, err := outputJoiner.Next(ctx)
	if err != nil {
		t.Errorf("Failed to read from output channel: %s", err)
	}
	if !ok {
		t.Errorf("Output channel closed unexpectedly")
	}
	row := env.Msg()
	if row.Strings["movieID"] != "A" || row.Strings["actor"] != "Actor 1" {
		t.Errorf("Expected movieID A and actor Actor 1, got movieID %s and actor %s", row.Strings["movieID"], row.Strings["actor"])
	}

	env, ok, err = outputJoiner.Next(ctx)
	if err != nil {
		t.Errorf("Failed to read from output channel: %s", err)
	}
	if !ok {
		t.Errorf("Output channel closed unexpectedly")
	}
	row = env.Msg()
	if row.Strings["movieID"] != "A" || row.Strings["actor"] != "Actor 2" {
		t.Errorf("Expected movieID A and actor Actor 2, got movieID %s and actor %s", row.Strings["movieID"], row.Strings["actor"])
	}
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
