package test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/joiners_ratings_workers/joiner"
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
	expected := map[string][]map[string]any{
		"client1": {
			{
				"movieID": "A",
				"actor":   "Actor 1",
			},
			{
				"movieID": "A",
				"actor":   "Actor 2",
			},
		},
	}
	AssertResults(t, outputJoiner, expected)

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

	expected := map[string][]map[string]any{
		"client1": {{
			"movieID": "X",
			"actor":   "Actor X",
		}},
		"client2": {},
	}
	AssertResults(t, outputJoiner, expected)

	//currentTask.Finish()
	inputMovies.Close()
	inputCredits.Close()
	outputJoiner.Close()
	middlewareConnection.Close()
}

func TestTwoCreditsJoiner(t *testing.T) {
	//t.Skip("TestTwoCreditsJoiner: Test not updated yet")

	middlewareConnection := ConnectToRabbitMQ(t)
	defer func() {
		RunCommand(t, "docker", "stop", "rabbitmq")
		RunCommand(t, "docker", "rm", "rabbitmq")
	}()

	inputMovies, err := middlewareConnection.WriteTo("filter_release_date_ge_2000_and_include_ar", []string{"joiner_credits"})
	assert.NoError(t, err)
	inputCredits, err := middlewareConnection.WriteTo("clean_credits", []string{"joiner_1_credits", "joiner_2_credits"})
	assert.NoError(t, err)
	os.Setenv("WORKER_COUNT", "2")
	os.Setenv("WORKER_ID", "1")
	workerLogger := logger.NewConsoleLogger("joiner_1", logger.Debug)
	worker1 := joiner.NewCreditsWorker([]string{"test3"}, "1", workerLogger)
	os.Setenv("WORKER_ID", "2")
	workerLogger = logger.NewConsoleLogger("joiner_2", logger.Debug)
	worker2 := joiner.NewCreditsWorker([]string{"test3"}, "2", workerLogger)
	outputJoiner, err := middlewareConnection.ConsumeFrom(worker1.Tasks.Name(), "test3", 1, 20)
	assert.NoError(t, err)

	inputMovies.SendEOF("client1")
	inputCredits.SendEOF("client1")

	go func() {
		worker1.Run(middlewareConnection)
	}()
	go func() {
		worker2.Run(middlewareConnection)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	env, err := outputJoiner.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, env.Type(), middleware.Prune)
	env.Ack(false)
	ctx, cancel = context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	env, err = outputJoiner.Next(ctx)
	assert.NoError(t, err)
	assert.Equal(t, env.Type(), middleware.EOF)
	env.Ack(false)
	ExpectNoMoreRows(t, outputJoiner)

	inputMovies.Close()
	inputCredits.Close()
	outputJoiner.Close()
	middlewareConnection.Close()
}

func TestFiveCreditsJoinerFiveClients(t *testing.T) {
	//t.Skip("TestTwoCreditsJoiner: Test not updated yet")

	middlewareConnection := ConnectToRabbitMQ(t)
	defer func() {
		RunCommand(t, "docker", "stop", "rabbitmq")
		RunCommand(t, "docker", "rm", "rabbitmq")
	}()

	inputMovies, err := middlewareConnection.WriteTo("filter_release_date_ge_2000_and_include_ar", []string{"joiner_credits"})
	assert.NoError(t, err)
	inputCredits, err := middlewareConnection.WriteTo("clean_credits", []string{"joiner_1_credits", "joiner_2_credits"})
	assert.NoError(t, err)
	os.Setenv("WORKER_COUNT", "5")
	cantWorkers := 5
	cantClients := 5
	workers := make([]joiner.Worker, cantWorkers)
	for i := range cantWorkers {
		ID := fmt.Sprintf("%d", i+1)
		os.Setenv("WORKER_ID", ID)
		workerLogger := logger.NewConsoleLogger(fmt.Sprintf("joiner_%s", ID), logger.Debug)
		worker := joiner.NewCreditsWorker([]string{"test4"}, ID, workerLogger)
		workers[i] = worker
	}
	outputJoiner, err := middlewareConnection.ConsumeFrom(workers[0].Tasks.Name(), "test4", 1, 20)
	assert.NoError(t, err)
	clients := make([]string, cantClients)
	for i := range cantClients {
		cid := fmt.Sprintf("client%d", i+1)
		clients[i] = cid
		inputMovies.SendEOF(cid)
		inputCredits.SendEOF(cid)
	}

	for i := range cantWorkers {

		go func() {
			workers[i].Run(middlewareConnection)
		}()
	}

	AsserEOFs(t, outputJoiner, clients)

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

func configTestJoinerCredits(t *testing.T, output string, middlewareConnection middleware.Connection[*model.Row]) (middleware.Sender[*model.Row], middleware.Sender[*model.Row], joiner.Worker, middleware.Receiver[*model.Row]) {
	// movies := NewSourceTask[*model.Row]("filter_release_date_ge_2000_and_include_ar")
	// credits := NewSourceTask[*model.Row]("clean_credits")
	inputMovies, err := middlewareConnection.WriteTo("filter_release_date_ge_2000_and_include_ar", []string{"joiner_credits"})
	assert.NoError(t, err)
	inputCredits, err := middlewareConnection.WriteTo("clean_credits", []string{"joiner_1_credits"})
	assert.NoError(t, err)
	workerLogger := logger.NewConsoleLogger("joiner_1", logger.Info)

	worker := joiner.NewCreditsWorker([]string{output}, "1", workerLogger)
	currentTask := worker.Tasks
	//currentTask = credits.NewJoinerCredits(movies, credits, []string{output})
	outputJoiner, err := middlewareConnection.ConsumeFrom(currentTask.Name(), output, 1, 20)
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
	id := "1"
	middlewareLogger := logger.NewConsoleLogger(fmt.Sprintf("middleware_%s", id), logger.Info)
	return rabbitmq.NewMiddleware[*model.Row](connector, middlewareLogger)

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
	_, err := output.Next(ctx)
	assert.Error(t, err)
	assert.Equal(t, err.Error(), "timeout reached while waiting for message")
}

func AssertResults(t *testing.T, outputJoiner middleware.Receiver[*model.Row], expected map[string][]map[string]any) {
	steps := map[string]int{}
	countExpected := 0
	for client := range expected {
		if len(expected[client]) == 0 {
			steps[client] = 1
		} else {
			steps[client] = 0
			countExpected += len(expected[client])
		}
		countExpected += 2
	}
	t.Logf("Expecting %d messages", countExpected)
	for range countExpected {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		env, err := outputJoiner.Next(ctx)
		assert.NoError(t, err)
		cid := env.Cid()
		stepCid, exists := steps[cid]
		assert.True(t, exists, "Client %s not found in expected results", cid)
		switch stepCid {
		case 0:
			row := env.Msg()
			t.Logf("Received msg %+v for client %s", row, cid)
			expectedValues := expected[cid][0]
			for key, value := range expectedValues {
				switch key {
				case "movieID":
					assert.Equal(t, row.Strings["movieID"], value)
				case "title":
					assert.Equal(t, row.Strings["title"], value)
				case "avg_rating":
					assert.Equal(t, row.Floats["avg_rating"], value)
				case "actor":
					assert.Equal(t, row.Strings["actor"], value)
				}
			}
			if len(expected[cid]) > 1 {
				expected[cid] = expected[cid][1:]
				continue
			}

		case 1:
			assert.Equal(t, middleware.Prune, env.Type())
			env.Ack(false)
			t.Logf("Received prune for client %s", cid)
		case 2:
			assert.Equal(t, middleware.EOF, env.Type())
			t.Logf("Received EOF for client %s", cid)
			delete(steps, cid)
		case 3:
			assert.FailNow(t, "Unexpected message for client %s", cid)
		}
		steps[cid]++
	}
	ExpectNoMoreRows(t, outputJoiner)
}

func AsserEOFs(t *testing.T, outputJoiner middleware.Receiver[*model.Row], clients []string) {
	steps := map[string]int{}
	countExpected := 0
	for _, client := range clients {
		steps[client] = 1
		countExpected += 2
	}
	t.Logf("Expecting %d messages", countExpected)
	for range countExpected {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		defer cancel()
		env, err := outputJoiner.Next(ctx)
		assert.NoError(t, err)
		cid := env.Cid()
		stepCid, exists := steps[cid]
		assert.True(t, exists, "Client %s not found in expected results", cid)
		switch stepCid {
		case 1:
			assert.Equal(t, middleware.Prune, env.Type())
			env.Ack(false)
			t.Logf("Received prune for client %s", cid)
		case 2:
			assert.Equal(t, middleware.EOF, env.Type())
			t.Logf("Received EOF for client %s", cid)
			delete(steps, cid)
		case 3:
			assert.FailNow(t, "Unexpected message for client %s", cid)
		}
		steps[cid]++
	}
	ExpectNoMoreRows(t, outputJoiner)
}
