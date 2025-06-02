package test

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/joiners_ratings_workers/joiner"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const RABBITMQ_EXPOSED_PORT_BASE = uint16(4000)

var baseConfig = rabbitmq.NewConfiguration("guest", "guest", "localhost", RABBITMQ_EXPOSED_PORT_BASE)

func TestJoiner(t *testing.T) {

	provider := rabbitmq.NewContainerProvider(baseConfig)

	test1 := provider.AsyncDeployRabbit()
	test2 := provider.AsyncDeployRabbit()
	test3 := provider.AsyncDeployRabbit()
	test4 := provider.AsyncDeployRabbit()
	test5 := provider.AsyncDeployRabbit()
	test6 := provider.AsyncDeployRabbit()
	test7 := provider.AsyncDeployRabbit()

	test1container := <-test1
	defer test1container.Container.Teardown()
	test2container := <-test2
	defer test2container.Container.Teardown()
	test3container := <-test3
	defer test3container.Container.Teardown()
	test4container := <-test4
	defer test4container.Container.Teardown()
	test5container := <-test5
	defer test5container.Container.Teardown()
	test6container := <-test6
	defer test6container.Container.Teardown()
	test7container := <-test7
	defer test7container.Container.Teardown()

	t.Run("Credits", func(t *testing.T) {

		t.Run("OneJoinerOneClient", func(t *testing.T) {
			init := test1container
			require.NotNil(t, init)
			require.NoError(t, init.Err)

			middlewareConnection := ConnectToRabbit(t, init, "1")

			inputMovies, inputCredits, worker, outputJoiner, outputConnection := configTestJoinerCredits(t, init, "output_test1_credits", middlewareConnection)

			cid := "client1"
			SendRows(t, inputMovies, cid,
				uint64(0),
				&model.Row{Strings: map[string]string{"movieID": "A"}},
				&model.Row{Strings: map[string]string{"movieID": "B"}},
			)
			SendRows(t, inputCredits, cid,
				uint64(2),
				&model.Row{
					Strings: map[string]string{"ID": "A"},
					Arrays:  map[string][]string{"cast": {"Actor 1", "Actor 2"}},
				})

			go func() {
				joinerConnection := ConnectToRabbit(t, init, "1")
				worker.Run(joinerConnection)
				joinerConnection.Close()
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

			worker.Tasks.Finish()
			inputMovies.Close()
			inputCredits.Close()
			outputJoiner.Close()
			middlewareConnection.Close()
			outputConnection.Close()

		})

		t.Run("OneJoinerMultipleClients", func(t *testing.T) {
			init := test2container
			require.NotNil(t, init)
			require.NoError(t, init.Err)

			middlewareConnection := ConnectToRabbit(t, init, "1")

			inputMovies, inputCredits, worker, outputJoiner, outputConnection := configTestJoinerCredits(t, init, "output_test2_credits", middlewareConnection)

			// Cliente 1
			cid := "client1"
			SendRows(t, inputMovies, cid,
				uint64(0),
				&model.Row{Strings: map[string]string{"movieID": "X"}},
			)
			SendRows(t, inputCredits, cid,
				uint64(1),
				&model.Row{
					Strings: map[string]string{"ID": "X"},
					Arrays:  map[string][]string{"cast": {"Actor X"}},
				})

			// Cliente 2: película sin créditos
			cid = "client2"
			SendRows(t, inputMovies, cid,
				uint64(0),
				&model.Row{Strings: map[string]string{"movieID": "Y"}},
				&model.Row{Strings: map[string]string{"movieID": "Z"}},
			)
			SendRows(t, inputCredits, cid,
				uint64(2),
				&model.Row{
					Strings: map[string]string{"ID": "A"},
					Arrays:  map[string][]string{"cast": {"Actor A"}},
				})

			go func() {
				joinerConnection := ConnectToRabbit(t, init, "1")
				worker.Run(joinerConnection)
				joinerConnection.Close()
			}()

			expected := map[string][]map[string]any{
				"client1": {{
					"movieID": "X",
					"actor":   "Actor X",
				}},
				"client2": {},
			}
			AssertResults(t, outputJoiner, expected)

			worker.Tasks.Finish()
			inputMovies.Close()
			inputCredits.Close()
			outputJoiner.Close()
			middlewareConnection.Close()
			outputConnection.Close()
		})

		t.Run("MultipleJoinersOneClient", func(t *testing.T) {
			init := test3container
			require.NotNil(t, init)
			require.NoError(t, init.Err)

			middlewareSenderConnection := ConnectToRabbit(t, init, "sender")

			inputMovies, err := middlewareSenderConnection.WriteTo("filter_release_date_ge_2000_and_include_ar", []string{"joiner_credits"}, "0", uint(2))
			assert.NoError(t, err)
			inputCredits, err := middlewareSenderConnection.WriteTo("clean_credits", []string{"joiner_0_credits", "joiner_1_credits"}, "0", uint(2))
			assert.NoError(t, err)
			os.Setenv("WORKER_COUNT", "2")
			os.Setenv("WORKER_ID", "0")
			workerLogger := logger.NewConsoleLogger("joiner_0", logger.Info)
			worker1 := joiner.NewCreditsWorker([]string{"test3"}, "0", workerLogger)
			os.Setenv("WORKER_ID", "1")
			workerLogger = logger.NewConsoleLogger("joiner_1", logger.Info)
			worker2 := joiner.NewCreditsWorker([]string{"test3"}, "1", workerLogger)
			outputConnection := ConnectToRabbit(t, init, "output")
			outputJoiner, err := outputConnection.ConsumeFrom(worker1.Tasks.Name(), "test3", "0", 1, uint(1))
			assert.NoError(t, err)

			var wg sync.WaitGroup
			wg.Add(2)

			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "0")
				worker1.Run(joinerConnection)
				joinerConnection.Close()
			}()
			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "1")
				worker2.Run(joinerConnection)
				joinerConnection.Close()
			}()

			inputMovies.SendEOF("client1")
			inputCredits.SendEOF("client1")

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
			middlewareSenderConnection.Close()
			outputConnection.Close()
			worker1.Tasks.Finish()
			worker2.Tasks.Finish()
			wg.Wait()

		})

		t.Run("MultipleJoinersMultipleClients", func(t *testing.T) {
			init := test4container
			require.NotNil(t, init)
			require.NoError(t, init.Err)

			middlewareSenderConnection := ConnectToRabbit(t, init, "sender")
			inputMovies, err := middlewareSenderConnection.WriteTo("filter_release_date_ge_2000_and_include_ar", []string{"joiner_credits"}, "0", uint(5))
			assert.NoError(t, err)
			cantWorkers := 5
			susbscribers := make([]string, cantWorkers)
			for i := range cantWorkers {
				susbscribers[i] = fmt.Sprintf("joiner_%d_credits", i)
			}
			inputCredits, err := middlewareSenderConnection.WriteTo("clean_credits", susbscribers, "0", uint(cantWorkers))
			assert.NoError(t, err)
			os.Setenv("WORKER_COUNT", "5")
			cantClients := 5
			workers := make([]joiner.Worker, cantWorkers)
			for i := range cantWorkers {
				ID := fmt.Sprintf("%d", i)
				os.Setenv("WORKER_ID", ID)
				workerLogger := logger.NewConsoleLogger(fmt.Sprintf("joiner_%s", ID), logger.Info)
				worker := joiner.NewCreditsWorker([]string{"test4"}, ID, workerLogger)
				workers[i] = worker
			}
			outputConnection := ConnectToRabbit(t, init, "output")
			outputJoiner, err := outputConnection.ConsumeFrom(workers[0].Tasks.Name(), "test4", "0", 1, uint(1))
			assert.NoError(t, err)

			var wg sync.WaitGroup
			wg.Add(5)

			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "4")
				workers[4].Run(joinerConnection)
				joinerConnection.Close()
			}()
			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "1")
				workers[1].Run(joinerConnection)
				joinerConnection.Close()
			}()

			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "2")
				workers[2].Run(joinerConnection)
				joinerConnection.Close()
			}()

			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "3")
				workers[3].Run(joinerConnection)
				joinerConnection.Close()
			}()

			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "0")
				workers[0].Run(joinerConnection)
				joinerConnection.Close()
			}()

			clients := make([]string, cantClients)
			for i := range cantClients {
				cid := fmt.Sprintf("client%d", i+1)
				clients[i] = cid
				inputMovies.SendEOF(cid)
				inputCredits.SendEOF(cid)
			}

			AsserEOFs(t, outputJoiner, clients)

			inputMovies.Close()
			inputCredits.Close()
			outputJoiner.Close()
			outputConnection.Close()
			middlewareSenderConnection.Close()
			for i := range cantWorkers {
				workers[i].Tasks.Finish()
			}
			wg.Wait()
		})

		t.Run("RestartProcessesPendingMovies", func(t *testing.T) {
			init := test7container
			require.NotNil(t, init)
			require.NoError(t, init.Err)

			middlewareConnection := ConnectToRabbit(t, init, "1")

			inputMovies, inputCredits, worker, outputJoiner, outputConnection := configTestJoinerCredits(t, init, "output_test7", middlewareConnection)

			cid := "client1"

			SendRows(t, inputMovies, cid,
				uint64(0),
				&model.Row{Strings: map[string]string{"movieID": "A"}},
				&model.Row{Strings: map[string]string{"movieID": "B"}},
			)

			inputCredits.Send(&model.Row{
				Strings: map[string]string{"ID": "A"},
				Arrays:  map[string][]string{"cast": {"Actor 1", "Actor 2"}},
			}, cid, uint64(2))

			joinerConnection := ConnectToRabbit(t, init, "1")
			go func() {
				worker.Run(joinerConnection)
			}()

			expected := map[string][]map[string]any{
				"client1": {
					{"movieID": "A", "actor": "Actor 1"},
					{"movieID": "A", "actor": "Actor 2"},
				},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			env, err := outputJoiner.Next(ctx)
			assert.NoError(t, err)
			env.Ack(false)
			assert.Equal(t, env.Type(), middleware.Normal)
			assert.Equal(t, env.Msg().Strings["movieID"], "A")
			assert.Equal(t, env.Msg().Strings["actor"], "Actor 1")

			env, err = outputJoiner.Next(ctx)
			assert.NoError(t, err)
			env.Ack(false)
			assert.Equal(t, env.Type(), middleware.Normal)
			assert.Equal(t, env.Msg().Strings["movieID"], "A")
			assert.Equal(t, env.Msg().Strings["actor"], "Actor 2")

			worker.Tasks.Finish()

			SendRows(t, inputCredits, cid,
				uint64(3),
				&model.Row{
					Strings: map[string]string{"ID": "B"},
					Arrays:  map[string][]string{"cast": {"Actor 3"}},
				},
			)

			workerLogger := logger.NewConsoleLogger("joiner_0", logger.Info)

			worker2 := joiner.NewCreditsWorker([]string{"output_test7"}, "0", workerLogger)
			joinerConnection = ConnectToRabbit(t, init, "1")
			go func() {
				worker2.Run(joinerConnection)
			}()

			expected = map[string][]map[string]any{
				"client1": {
					{"movieID": "B", "actor": "Actor 3"},
				},
			}
			AssertResults(t, outputJoiner, expected)

			worker.Tasks.Finish()
			joinerConnection.Close()

			inputMovies.Close()
			inputCredits.Close()
			outputJoiner.Close()
			middlewareConnection.Close()
			outputConnection.Close()
		})

	})

	os.Unsetenv("WORKER_COUNT")
	os.Unsetenv("WORKER_ID")

	// t.Run("Ratings", func(t *testing.T) {
	// 	t.Run("OneJoinerOneClient", func(t *testing.T) {
	// 		init := test5container
	// 		require.NotNil(t, init)
	// 		require.NoError(t, init.Err)

	// 		middlewareConnection := ConnectToRabbit(t, init, "1")

	// 		inputMovies, inputRatings, worker, outputJoiner, outputConnection := configTestJoinerRatings(t, init, "output_test1_ratings", middlewareConnection)

	// 		cid := "client1"
	// 		SendRows(t, inputMovies, cid,
	// 			&model.Row{Strings: map[string]string{"movieID": "A", "title": "Movie A"}},
	// 			&model.Row{Strings: map[string]string{"movieID": "B", "title": "Movie B"}},
	// 		)

	// 		SendRows(t, inputRatings, cid,
	// 			&model.Row{
	// 				Strings: map[string]string{"movieID": "A"},
	// 				Floats:  map[string]float64{"avg_rating": 3.5},
	// 			})

	// 		go func() {
	// 			joinerConnection := ConnectToRabbit(t, init, "1")
	// 			worker.Run(joinerConnection)
	// 			joinerConnection.Close()
	// 		}()

	// 		// Verificar salida
	// 		expected := map[string][]map[string]any{
	// 			"client1": {{
	// 				"movieID":    "A",
	// 				"title":      "Movie A",
	// 				"avg_rating": 3.5,
	// 			}},
	// 		}
	// 		AssertResults(t, outputJoiner, expected)

	// 		inputMovies.Close()
	// 		inputRatings.Close()
	// 		outputJoiner.Close()
	// 		middlewareConnection.Close()
	// 		outputConnection.Close()
	// 		worker.Tasks.Finish()
	// 	})

	// 	t.Run("OneJoinerMultipleClients", func(t *testing.T) {
	// 		init := test6container
	// 		require.NotNil(t, init)
	// 		require.NoError(t, init.Err)

	// 		middlewareConnection := ConnectToRabbit(t, init, "1")

	// 		inputMovies, inputRatings, worker, outputJoiner, outputConnection := configTestJoinerRatings(t, init, "output_test2_ratings", middlewareConnection)

	// 		cid := "client1"
	// 		SendRows(t, inputMovies, cid,
	// 			&model.Row{Strings: map[string]string{"movieID": "A", "title": "Movie A"}},
	// 			&model.Row{Strings: map[string]string{"movieID": "B", "title": "Movie B"}},
	// 		)

	// 		SendRows(t, inputRatings, cid,
	// 			&model.Row{
	// 				Strings: map[string]string{"movieID": "A"},
	// 				Floats:  map[string]float64{"avg_rating": 3.5},
	// 			})

	// 		cid = "client2"
	// 		SendRows(t, inputMovies, cid,
	// 			&model.Row{Strings: map[string]string{"movieID": "C", "title": "Movie C"}},
	// 			&model.Row{Strings: map[string]string{"movieID": "D", "title": "Movie D"}},
	// 		)

	// 		SendRows(t, inputRatings, cid,
	// 			&model.Row{
	// 				Strings: map[string]string{"movieID": "D"},
	// 				Floats:  map[string]float64{"avg_rating": 1.0},
	// 			})

	// 		go func() {
	// 			joinerConnection := ConnectToRabbit(t, init, "1")
	// 			worker.Run(joinerConnection)
	// 			joinerConnection.Close()
	// 		}()

	// 		// Verificar salida
	// 		expected := map[string][]map[string]any{
	// 			"client1": {{
	// 				"movieID":    "A",
	// 				"title":      "Movie A",
	// 				"avg_rating": 3.5,
	// 			}},
	// 			"client2": {{
	// 				"movieID":    "D",
	// 				"title":      "Movie D",
	// 				"avg_rating": 1.0,
	// 			}},
	// 		}
	// 		AssertResults(t, outputJoiner, expected)

	// 		worker.Tasks.Finish()
	// 		inputMovies.Close()
	// 		inputRatings.Close()
	// 		outputJoiner.Close()
	// 		middlewareConnection.Close()
	// 		outputConnection.Close()
	// 		worker.Tasks.Finish()
	// 	})
	// })
}

func RunCommand(t *testing.T, name string, args ...string) {
	cmd := exec.Command(name, args...)
	err := cmd.Run()
	if err != nil {
		t.Fatalf("error running command '%s %v': %v", name, args, err)
	}
}

func configTestJoinerCredits(t *testing.T, init rabbitmq.AsyncDeployRabbitRes, output string, middlewareConnection middleware.Connection[*model.Row]) (middleware.Sender[*model.Row], middleware.Sender[*model.Row], joiner.Worker, middleware.Receiver[*model.Row], middleware.Connection[*model.Row]) {

	inputMovies, err := middlewareConnection.WriteTo("filter_release_date_ge_2000_and_include_ar", []string{"joiner_credits"}, "0", uint(1))
	assert.NoError(t, err)
	inputCredits, err := middlewareConnection.WriteTo("clean_credits", []string{"joiner_0_credits"}, "0", uint(1))
	assert.NoError(t, err)
	workerLogger := logger.NewConsoleLogger("joiner_0", logger.Info)

	worker := joiner.NewCreditsWorker([]string{output}, "0", workerLogger)
	currentTask := worker.Tasks
	outputConnection := ConnectToRabbit(t, init, output)
	outputJoiner, err := outputConnection.ConsumeFrom(currentTask.Name(), output, "0", 1, uint(1))
	assert.NoError(t, err)
	return inputMovies, inputCredits, worker, outputJoiner, outputConnection
}

func ConnectToRabbit(t *testing.T, init rabbitmq.AsyncDeployRabbitRes, id string) middleware.Connection[*model.Row] {
	connector, err := rabbitmq.ConnectorCustom(init.Config)
	for {
		if err == nil {
			break
		}
		t.Logf("Error connecting to RabbitMQ. Retrying in 5 seconds...")
		time.Sleep(5 * time.Second)
		connector, err = rabbitmq.ConnectorCustom(init.Config)
	}
	middlewareLogger := logger.NewConsoleLogger(fmt.Sprintf("middleware_%s", id), logger.Info)
	return rabbitmq.NewMiddleware[*model.Row](connector, middlewareLogger)
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

func SendRows(t *testing.T, sender middleware.Sender[*model.Row], cid string, idBase uint64, rows ...*model.Row) {
	id := idBase
	for _, row := range rows {
		err := sender.Send(row, cid, id)
		id++
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
		env.Ack(false)

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
			//env.Ack(false)
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
		env.Ack(false)
		switch stepCid {
		case 1:
			assert.Equal(t, middleware.Prune, env.Type())
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

// func configTestJoinerRatings(t *testing.T, init rabbitmq.AsyncDeployRabbitRes, output string, middlewareConnection middleware.Connection[*model.Row]) (middleware.Sender[*model.Row], middleware.Sender[*model.Row], joiner.Worker, middleware.Receiver[*model.Row], middleware.Connection[*model.Row]) {
// 	movies := joiner.NewSourceTask[*model.Row]("filter_release_date_ge_2000_and_include_ar")
// 	filter_ratings := joiner.NewSourceTask[*model.Row]("filter_avg_rating")
// 	inputMovies, err := middlewareConnection.WriteTo(movies.Name(), []string{"joiner_ratings"})
// 	assert.NoError(t, err)
// 	inputCredits, err := middlewareConnection.WriteTo(filter_ratings.Name(), []string{"joiner_1_ratings"})
// 	assert.NoError(t, err)
// 	workerLogger := logger.NewConsoleLogger("joiner_1", logger.Debug)
// 	worker := joiner.NewRatingsWorker([]string{output}, "1", workerLogger)
// 	currentTask := worker.Tasks
// 	outputConnection := ConnectToRabbit(t, init, output)
// 	outputJoiner, err := outputConnection.ConsumeFrom(currentTask.Name(), output, 1, 20)
// 	assert.NoError(t, err)
// 	return inputMovies, inputCredits, worker, outputJoiner, outputConnection
// }
