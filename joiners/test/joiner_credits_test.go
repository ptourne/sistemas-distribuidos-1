package test

import (
	"context"
	"os/exec"
	"testing"
	"time"

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
	worker := joiner.NewCreditsWorker([]string{output})
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
		countExpected++ // eof. ToDo: +=2 prune
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	t.Logf("Expecting %d messages", countExpected)
	for range countExpected {
		env, _, err := outputJoiner.Next(ctx)
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
			assert.Equal(t, middleware.EOF, env.Type())
			t.Logf("Received EOF for client %s", cid)
			delete(steps, cid)
			// case 1:
			// 	assert.Equal(t, middleware.Prune, env.Type())
			//  t.Logf("Received prune for client %s", cid)
			// case 2:
			// 	assert.Equal(t, middleware.EOF, env.Type())
			//  t.Logf("Received EOF for client %s", cid)
			//  delete(steps, cid)
		case 2:
			assert.FailNow(t, "Unexpected message for client %s", cid)
		}
		steps[cid]++
	}
	ExpectNoMoreRows(t, outputJoiner)
}
