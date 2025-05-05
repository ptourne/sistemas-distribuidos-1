package test

import (
	"context"
	"os/exec"
	"testing"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/joiners_ratings_workers/joiner_credits_worker/credits"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
	"github.com/stretchr/testify/assert"
)

func TestJoinerCreditsOneClient(t *testing.T) {
	//t.Skip("TestJoinerCreditsOneClient: Test not updated yet")

	middlewareConnection := ConnectToRabbitMQ(t)
	defer func() {
		RunCommand(t, "docker", "stop", "rabbitmq")
		RunCommand(t, "docker", "rm", "rabbitmq")
	}()

	inputMovies, inputCredits, worker, outputJoiner := configTestJoinerCredits(t, "output_test1_credits", middlewareConnection)

	cid := "client1"
	SendRows(t, inputMovies, cid,
		&model.Row{Strings: map[string]string{"movieID": "A"}},
		&model.Row{Strings: map[string]string{"movieID": "B"}},
	)
	SendRows(t, inputCredits, cid,
		&model.Row{
			Strings: map[string]string{"ID": "A"},
			Arrays:  map[string][]string{"cast": {"Actor 1", "Actor 2"}},
		})
	go func() {
		worker.Run(middlewareConnection)
	}()

	// Verificar salida
	t.Logf("Esperando recibir la salida del joiner")
	assertReceiveCastRows(t, outputJoiner, cid, "A", "Actor 1")
	assertReceiveCastRows(t, outputJoiner, cid, "A", "Actor 2")
	AssertReceivedEOF(t, outputJoiner, cid)
	ExpectNoMoreRows(t, outputJoiner)
	t.Log("Asserted all rows")

	//currentTask.Finish()
	inputMovies.Close()
	inputCredits.Close()
	outputJoiner.Close()
	middlewareConnection.Close()
}

func TestJoinerCreditsMultipleClients(t *testing.T) {
	//t.Skip("TestJoinerCreditsMultipleClients: Test not updated yet")

	middlewareConnection := ConnectToRabbitMQ(t)
	defer func() {
		RunCommand(t, "docker", "stop", "rabbitmq")
		RunCommand(t, "docker", "rm", "rabbitmq")
	}()

	inputMovies, inputCredits, worker, outputJoiner := configTestJoinerCredits(t, "output_test2_credits", middlewareConnection)

	// Cliente 1
	cid := "client1"
	SendRows(t, inputMovies, cid,
		&model.Row{Strings: map[string]string{"movieID": "X"}},
	)
	SendRows(t, inputCredits, cid,
		&model.Row{
			Strings: map[string]string{"ID": "X"},
			Arrays:  map[string][]string{"cast": {"Actor X"}},
		})

	// Cliente 2: película sin créditos
	cid = "client2"
	SendRows(t, inputMovies, cid,
		&model.Row{Strings: map[string]string{"movieID": "Y"}},
		&model.Row{Strings: map[string]string{"movieID": "Z"}},
	)
	SendRows(t, inputCredits, cid, &model.Row{
		Strings: map[string]string{"ID": "A"},
		Arrays:  map[string][]string{"cast": {"Actor A"}},
	})

	go func() {
		worker.Run(middlewareConnection)
	}()

	// Cliente 1
	assertReceiveCastRows(t, outputJoiner, "client1", "X", "Actor X")
	AssertReceivedEOF(t, outputJoiner, "client1")
	// Cliente 2: no debería producir salida (no tiene créditos)
	AssertReceivedEOF(t, outputJoiner, "client2")
	ExpectNoMoreRows(t, outputJoiner)
	// ToDo: test receive prune msg

	//currentTask.Finish()
	inputMovies.Close()
	inputCredits.Close()
	outputJoiner.Close()
	middlewareConnection.Close()
}

func RunCommand(t *testing.T, name string, args ...string) {
	cmd := exec.Command(name, args...)
	err := cmd.Run()
	if err != nil {
		t.Fatalf("error running command '%s %v': %v", name, args, err)
	}
}

func configTestJoinerCredits(t *testing.T, output string, middlewareConnection middleware.Connection[*model.Row]) (middleware.Sender[*model.Row], middleware.Sender[*model.Row], credits.Worker, middleware.Receiver[*model.Row]) {
	// movies := NewSourceTask[*model.Row]("filter_release_date_ge_2000_and_include_ar")
	// credits := NewSourceTask[*model.Row]("clean_credits")
	inputMovies, err := middlewareConnection.WriteTo("filter_release_date_ge_2000_and_include_ar", []string{"joiner_credits"})
	assert.NoError(t, err)
	inputCredits, err := middlewareConnection.WriteTo("clean_credits", []string{"joiner_1_credits"})
	assert.NoError(t, err)
	worker := credits.NewWorker([]string{output})
	currentTask := worker.Tasks
	//currentTask = credits.NewJoinerCredits(movies, credits, []string{output})
	outputJoiner, err := middlewareConnection.ConsumeFrom(currentTask.Name(), output, 0, 20)
	assert.NoError(t, err)
	assert.NoError(t, err)
	return inputMovies, inputCredits, worker, outputJoiner
}

func ConnectToRabbitMQ(t *testing.T) middleware.Connection[*model.Row] {
	RunCommand(t, "docker", "run", "-d", "--name", "rabbitmq", "-p", "5672:5672", "-p", "15672:15672", "rabbitmq:management")
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

func SendRows(t *testing.T, sender middleware.Sender[*model.Row], cid string, rows ...*model.Row) {
	for _, row := range rows {
		err := sender.Send(row, cid)
		assert.NoError(t, err)
	}
	err := sender.SendEOF(cid)
	assert.NoError(t, err)
}

func ExpectNoMoreRows(t *testing.T, output middleware.Receiver[*model.Row]) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_, _, err := output.Next(ctx)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "timeout reached while waiting for message")
}

func assertReceiveCastRows(t *testing.T, outputJoiner middleware.Receiver[*model.Row], cid string, expectedMovieID string, expectedActor string) {
	AssertReceiveRow(t, outputJoiner, cid,
		map[string]string{"movieID": expectedMovieID, "actor": expectedActor},
		map[string]float64{},
	)
}

func AssertReceiveRow(
	t *testing.T,
	output middleware.Receiver[*model.Row],
	cid string,
	expectedStrings map[string]string,
	expectedFloats map[string]float64,
) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	env, _, err := output.Next(ctx)
	assert.NoError(t, err)
	//assert.True(t, ok)
	assert.Equal(t, middleware.Normal, env.Type())

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

func AssertReceivedEOF(t *testing.T, output middleware.Receiver[*model.Row], cid string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	env, _, err := output.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, middleware.EOF, env.Type())
	assert.Equal(t, cid, env.Cid())
}

func AssertReceivedPrune(t *testing.T, output middleware.Receiver[*model.Row], cid string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	env, _, err := output.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, middleware.Prune, env.Type())
	assert.Equal(t, cid, env.Cid())
}
