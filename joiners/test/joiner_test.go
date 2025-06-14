package test

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
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
	test8 := provider.AsyncDeployRabbit()
	tets9 := provider.AsyncDeployRabbit()
	test10 := provider.AsyncDeployRabbit()
	test11 := provider.AsyncDeployRabbit()
	test12 := provider.AsyncDeployRabbit()
	test13 := provider.AsyncDeployRabbit()
	test14 := provider.AsyncDeployRabbit()
	test15 := provider.AsyncDeployRabbit()
	test16 := provider.AsyncDeployRabbit()
	test17 := provider.AsyncDeployRabbit()
	test18 := provider.AsyncDeployRabbit()

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
	test8container := <-test8
	defer test8container.Container.Teardown()
	test9container := <-tets9
	defer test9container.Container.Teardown()
	test10container := <-test10
	defer test10container.Container.Teardown()
	test11container := <-test11
	defer test11container.Container.Teardown()
	test12container := <-test12
	defer test12container.Container.Teardown()
	test13container := <-test13
	defer test13container.Container.Teardown()
	test14container := <-test14
	defer test14container.Container.Teardown()
	test15container := <-test15
	defer test15container.Container.Teardown()
	test16container := <-test16
	defer test16container.Container.Teardown()
	test17container := <-test17
	defer test17container.Container.Teardown()
	test18container := <-test18
	defer test18container.Container.Teardown()

	os.RemoveAll("joiner_credits")
	os.RemoveAll("joiner_ratings")

	t.Run("Credits", func(t *testing.T) {

		t.Run("OneJoinerOneClient", func(t *testing.T) {
			init := test1container
			require.NotNil(t, init)
			require.NoError(t, init.Err)

			middlewareConnection := ConnectToRabbit(t, init, "1")

			inputMovies, inputCredits, worker, outputJoiner, outputConnection := configTestJoinerCredits(t, init, "output_test1_credits", middlewareConnection)

			cid := uint64(10)
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
			expected := map[uint64][]map[string]any{
				cid: {
					{
						"movieID": "A",
						"actor":   "Actor 1",
						"msgID":   uint64(0),
					},
					{
						"movieID": "A",
						"actor":   "Actor 2",
						"msgID":   uint64(1),
					},
				},
			}
			AssertResults(t, outputJoiner, expected)

			inputMovies.Close()
			inputCredits.Close()
			outputJoiner.Close()
			middlewareConnection.Close()
			outputConnection.Close()

			worker.Tasks.Finish()
		})

		t.Run("OneJoinerMultipleClients", func(t *testing.T) {
			init := test2container
			require.NotNil(t, init)
			require.NoError(t, init.Err)

			middlewareConnection := ConnectToRabbit(t, init, "1")

			inputMovies, inputCredits, worker, outputJoiner, outputConnection := configTestJoinerCredits(t, init, "output_test2_credits", middlewareConnection)

			// Cliente 1
			cid1 := uint64(20)
			SendRows(t, inputMovies, cid1,
				uint64(0),
				&model.Row{Strings: map[string]string{"movieID": "X"}},
			)
			SendRows(t, inputCredits, cid1,
				uint64(1),
				&model.Row{
					Strings: map[string]string{"ID": "X"},
					Arrays:  map[string][]string{"cast": {"Actor X"}},
				})

			// Cliente 2: película sin créditos
			cid2 := uint64(21)
			SendRows(t, inputMovies, cid2,
				uint64(0),
				&model.Row{Strings: map[string]string{"movieID": "Y"}},
				&model.Row{Strings: map[string]string{"movieID": "Z"}},
			)
			SendRows(t, inputCredits, cid2,
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

			expected := map[uint64][]map[string]any{
				cid1: {{
					"movieID": "X",
					"actor":   "Actor X",
					"msgID":   uint64(0),
				}},
				cid2: {},
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

			cid := uint64(30)

			inputMovies.SendEOF(cid)
			inputCredits.SendEOF(cid)

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
			worker0 := workers[0]
			worker1 := workers[1]
			worker2 := workers[2]
			worker3 := workers[3]
			worker4 := workers[4]

			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "4")
				worker4.Run(joinerConnection)
				joinerConnection.Close()
			}()
			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "1")
				worker1.Run(joinerConnection)
				joinerConnection.Close()
			}()

			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "2")
				worker2.Run(joinerConnection)
				joinerConnection.Close()
			}()

			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "3")
				worker3.Run(joinerConnection)
				joinerConnection.Close()
			}()

			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "0")
				worker0.Run(joinerConnection)
				joinerConnection.Close()
			}()

			clients := make([]uint64, cantClients)
			baseCid := uint64(40)
			for i := range cantClients {
				// cid := fmt.Sprintf("client4_%d", i+1)
				cid := baseCid + uint64(i)
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

		os.Unsetenv("WORKER_COUNT")
		os.Unsetenv("WORKER_ID")

		t.Run("RestartProcessPendingMovies", func(t *testing.T) {
			init := test7container
			require.NotNil(t, init)
			require.NoError(t, init.Err)

			middlewareConnection := ConnectToRabbit(t, init, "1")

			inputMovies, inputCredits, worker, outputJoiner, outputConnection := configTestJoinerCredits(t, init, "output_test7", middlewareConnection)

			cid := uint64(70)

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
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				worker.Run(joinerConnection)
				joinerConnection.Close()
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			env, err := outputJoiner.Next(ctx)
			assert.NoError(t, err)
			env.Ack(false)
			assert.Equal(t, env.Type(), middleware.Normal)
			assert.Equal(t, env.Msg().Strings["movieID"], "A")
			assert.Equal(t, env.Msg().Strings["actor"], "Actor 1")
			assert.Equal(t, env.Id(), uint64(0))

			env, err = outputJoiner.Next(ctx)
			assert.NoError(t, err)
			env.Ack(false)
			assert.Equal(t, env.Type(), middleware.Normal)
			assert.Equal(t, env.Msg().Strings["movieID"], "A")
			assert.Equal(t, env.Msg().Strings["actor"], "Actor 2")
			assert.Equal(t, env.Id(), uint64(1))

			filepath := fmt.Sprintf("joiner_credits/joiner0/%d/movies_B.csv", cid)

			waitForFile(t, filepath, 5*time.Second)

			worker.Tasks.Finish()
			wg.Wait()

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

			expected := map[uint64][]map[string]any{
				cid: {
					{"movieID": "B", "actor": "Actor 3", "msgID": uint64(2)},
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

		t.Run("RestartProcessEOFsOneJoiner", func(t *testing.T) {
			init := test9container
			require.NotNil(t, init)
			require.NoError(t, init.Err)

			middlewareConnection := ConnectToRabbit(t, init, "0")

			inputMovies, inputCredits, worker, outputJoiner, outputConnection := configTestJoinerCredits(t, init, "output_test9", middlewareConnection)

			cid := uint64(90)

			inputMovies.SendEOF(cid)

			joinerConnection := ConnectToRabbit(t, init, "0")
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				worker.Run(joinerConnection)
				joinerConnection.Close()
			}()

			for {
				if _, exists := worker.ClientsPruneMovies[cid]; exists {
					break
				}
				time.Sleep(100 * time.Millisecond)
			}

			worker.Tasks.Finish()
			wg.Wait()

			inputCredits.SendEOF(cid)

			workerLogger := logger.NewConsoleLogger("joiner_0", logger.Info)

			worker2 := joiner.NewCreditsWorker([]string{"output_test9"}, "0", workerLogger)
			joinerConnection = ConnectToRabbit(t, init, "0")
			go func() {
				worker2.Run(joinerConnection)
			}()

			AsserEOFs(t, outputJoiner, []uint64{cid})

			worker.Tasks.Finish()
			joinerConnection.Close()

			inputMovies.Close()
			inputCredits.Close()
			outputJoiner.Close()
			middlewareConnection.Close()
			outputConnection.Close()
		})

		t.Run("RestartLeaderProcessEOFs", func(t *testing.T) {
			init := test16container
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
			worker1 := joiner.NewCreditsWorker([]string{"test16"}, "0", workerLogger)
			os.Setenv("WORKER_ID", "1")
			workerLogger = logger.NewConsoleLogger("joiner_1", logger.Info)
			worker2 := joiner.NewCreditsWorker([]string{"test16"}, "1", workerLogger)
			outputConnection := ConnectToRabbit(t, init, "output")
			outputJoiner, err := outputConnection.ConsumeFrom(worker1.Tasks.Name(), "test16", "0", 1, uint(1))
			assert.NoError(t, err)

			cid := uint64(160)

			inputMovies.SendEOF(cid)

			joinerConnection1 := ConnectToRabbit(t, init, "0")
			wg := sync.WaitGroup{}
			wg2 := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				worker1.Run(joinerConnection1)
				joinerConnection1.Close()
			}()

			joinerConnection2 := ConnectToRabbit(t, init, "1")
			wg2.Add(1)
			go func() {
				defer wg2.Done()
				worker2.Run(joinerConnection2)
				joinerConnection2.Close()
			}()

			for {
				if _, exists := worker1.ClientsPruneMovies[cid]; exists {
					if _, exists := worker2.ClientsPruneMovies[cid]; exists {
						break
					}
				}
				time.Sleep(100 * time.Millisecond)
			}

			worker1.Tasks.Finish()
			inputCredits.SendEOF(cid)
			wg.Wait()

			workerLogger = logger.NewConsoleLogger("joiner_0", logger.Info)

			worker1 = joiner.NewCreditsWorker([]string{"output_test16"}, "0", workerLogger)
			joinerConnection1 = ConnectToRabbit(t, init, "0")
			wg2.Add(1)
			go func() {
				defer wg2.Done()
				worker1.Run(joinerConnection1)
				joinerConnection1.Close()
			}()

			AsserEOFs(t, outputJoiner, []uint64{cid})

			worker2.Tasks.Finish()
			worker1.Tasks.Finish()

			wg2.Wait()

			inputMovies.Close()
			inputCredits.Close()
			outputJoiner.Close()
			middlewareSenderConnection.Close()
			outputConnection.Close()
			os.Unsetenv("WORKER_COUNT")
			os.Unsetenv("WORKER_ID")
		})

		t.Run("RestartNonLeaderProcessEOFs", func(t *testing.T) {
			init := test17container
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
			worker1 := joiner.NewCreditsWorker([]string{"test17"}, "0", workerLogger)
			os.Setenv("WORKER_ID", "1")
			workerLogger = logger.NewConsoleLogger("joiner_1", logger.Info)
			worker2 := joiner.NewCreditsWorker([]string{"test17"}, "1", workerLogger)
			outputConnection := ConnectToRabbit(t, init, "output")
			outputJoiner, err := outputConnection.ConsumeFrom(worker1.Tasks.Name(), "test17", "0", 1, uint(1))
			assert.NoError(t, err)

			cid := uint64(170)

			inputMovies.SendEOF(cid)

			joinerConnection1 := ConnectToRabbit(t, init, "0")
			wg := sync.WaitGroup{}
			wg2 := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				worker2.Run(joinerConnection1)
				joinerConnection1.Close()
			}()

			joinerConnection2 := ConnectToRabbit(t, init, "1")
			wg2.Add(1)
			go func() {
				defer wg2.Done()
				worker1.Run(joinerConnection2)
				joinerConnection2.Close()
			}()

			for {
				if _, exists := worker1.ClientsPruneMovies[cid]; exists {
					if _, exists := worker2.ClientsPruneMovies[cid]; exists {
						break
					}
				}
				time.Sleep(100 * time.Millisecond)
			}

			worker2.Tasks.Finish()
			inputCredits.SendEOF(cid)
			wg.Wait()

			workerLogger = logger.NewConsoleLogger("joiner_1", logger.Info)

			worker2 = joiner.NewCreditsWorker([]string{"test17"}, "1", workerLogger)
			joinerConnection1 = ConnectToRabbit(t, init, "1")
			wg2.Add(1)
			go func() {
				defer wg2.Done()
				worker2.Run(joinerConnection1)
				joinerConnection1.Close()
			}()

			AsserEOFs(t, outputJoiner, []uint64{cid})

			worker1.Tasks.Finish()
			worker2.Tasks.Finish()

			wg2.Wait()

			inputMovies.Close()
			inputCredits.Close()
			outputJoiner.Close()
			middlewareSenderConnection.Close()
			outputConnection.Close()
			os.Unsetenv("WORKER_COUNT")
			os.Unsetenv("WORKER_ID")
		})

		t.Run("ProcessCorruptCreditsFile", func(t *testing.T) {
			init := test10container
			require.NotNil(t, init)
			require.NoError(t, init.Err)

			cid := uint64(100)

			joinerID := "0"
			dir := fmt.Sprintf("joiner_credits/joiner%s/%d", joinerID, cid)
			require.NoError(t, os.MkdirAll(dir, 0755))

			filePath := fmt.Sprintf("%s/credits_0.csv", dir)
			file, err := os.Create(filePath)
			require.NoError(t, err)
			writer := csv.NewWriter(file)
			require.NoError(t, writer.Write([]string{"movieID", "credit"}))

			cast := `["Actor 1", "Actor 2"]`
			require.NoError(t, writer.Write([]string{"10", cast}))

			require.NoError(t, writer.Write([]string{"20"}))
			writer.Flush()
			require.NoError(t, file.Close())

			middlewareConnection := ConnectToRabbit(t, init, "0")
			inputMovies, inputCredits, worker, outputJoiner, outputConnection := configTestJoinerCredits(t, init, "output_test10", middlewareConnection)

			joinerConnection := ConnectToRabbit(t, init, "0")
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				worker.Run(joinerConnection)
				joinerConnection.Close()
			}()

			time.Sleep(1 * time.Second)

			file, err = os.Open(filePath)
			require.NoError(t, err)
			reader := csv.NewReader(file)
			_, err = reader.Read()
			require.NoError(t, err)

			data, err := reader.Read()

			if err != nil || len(data) < 2 {
				require.Fail(t, fmt.Sprintf("invalid row in file %s: %v", filePath, err))
			}
			movieID := data[0]
			if movieID != "10" {
				require.Fail(t, fmt.Sprintf("expected movieID '10', got '%s' in file %s", movieID, filePath))
			}
			if data[1] != `["Actor 1","Actor 2"]` {
				t.Fatalf("Unexpected cast value: got %q, want %q", data[1], `["Actor 1","Actor 2"]`)
			}

			_, err = reader.Read()
			require.Equal(t, io.EOF, err, fmt.Sprintf("expected EOF after reading movieID '10' in file %s", filePath))
			require.NoError(t, file.Close())

			inputMovies.SendEOF(cid)
			inputCredits.SendEOF(cid)

			AsserEOFs(t, outputJoiner, []uint64{cid})

			worker.Tasks.Finish()
			wg.Wait()

			inputMovies.Close()
			inputCredits.Close()
			outputJoiner.Close()
			middlewareConnection.Close()
			outputConnection.Close()
		})

		t.Run("ProcessCorruptPendingMoviesFile", func(t *testing.T) {
			testJoinerCreditsCorruptMoviesFile(t, uint64(110), "movies", test11container, "output_test11")
		})

		t.Run("ProcessCorruptProcessedMoviesFile", func(t *testing.T) {
			testJoinerCreditsCorruptMoviesFile(t, uint64(120), "processed_movies", test12container, "output_test12")
		})

	})

	t.Run("Ratings", func(t *testing.T) {
		t.Run("OneJoinerOneClient", func(t *testing.T) {
			init := test5container
			require.NotNil(t, init)
			require.NoError(t, init.Err)

			middlewareConnection := ConnectToRabbit(t, init, "0")

			inputMovies, inputRatings, worker, outputJoiner, outputConnection := configTestJoinerRatings(t, init, "output_test1_ratings", middlewareConnection)

			cid := uint64(50)
			SendRows(t, inputMovies, cid,
				uint64(0),
				&model.Row{Strings: map[string]string{"movieID": "A", "title": "Movie A"}},
				&model.Row{Strings: map[string]string{"movieID": "B", "title": "Movie B"}},
			)

			SendRows(t, inputRatings, cid,
				uint64(2),
				&model.Row{
					Strings: map[string]string{"movieID": "A"},
					Floats:  map[string]float64{"avg_rating": 3.5},
				})

			go func() {
				joinerConnection := ConnectToRabbit(t, init, "0")
				worker.Run(joinerConnection)
				joinerConnection.Close()
			}()

			// Verificar salida
			expected := map[uint64][]map[string]any{
				cid: {{
					"movieID":    "A",
					"title":      "Movie A",
					"avg_rating": 3.5,
					"msgID":      uint64(0),
				}},
			}
			AssertResults(t, outputJoiner, expected)

			inputMovies.Close()
			inputRatings.Close()
			outputJoiner.Close()
			middlewareConnection.Close()
			outputConnection.Close()
			worker.Tasks.Finish()
		})

		t.Run("OneJoinerMultipleClients", func(t *testing.T) {
			init := test6container
			require.NotNil(t, init)
			require.NoError(t, init.Err)

			middlewareConnection := ConnectToRabbit(t, init, "0")

			inputMovies, inputRatings, worker, outputJoiner, outputConnection := configTestJoinerRatings(t, init, "output_test2_ratings", middlewareConnection)

			cid1 := uint64(60)
			SendRows(t, inputMovies, cid1,
				uint64(0),
				&model.Row{Strings: map[string]string{"movieID": "A", "title": "Movie A"}},
				&model.Row{Strings: map[string]string{"movieID": "B", "title": "Movie B"}},
			)

			SendRows(t, inputRatings, cid1,
				uint64(2),
				&model.Row{
					Strings: map[string]string{"movieID": "A"},
					Floats:  map[string]float64{"avg_rating": 3.5},
				})

			cid2 := uint64(2)
			SendRows(t, inputMovies, cid2,
				uint64(4),
				&model.Row{Strings: map[string]string{"movieID": "C", "title": "Movie C"}},
				&model.Row{Strings: map[string]string{"movieID": "D", "title": "Movie D"}},
			)

			SendRows(t, inputRatings, cid2,
				uint64(6),
				&model.Row{
					Strings: map[string]string{"movieID": "D"},
					Floats:  map[string]float64{"avg_rating": 1.0},
				})

			go func() {
				joinerConnection := ConnectToRabbit(t, init, "0")
				worker.Run(joinerConnection)
				joinerConnection.Close()
			}()

			// Verificar salida
			expected := map[uint64][]map[string]any{
				cid1: {{
					"movieID":    "A",
					"title":      "Movie A",
					"avg_rating": 3.5,
					"msgID":      uint64(0),
				}},
				cid2: {{
					"movieID":    "D",
					"title":      "Movie D",
					"avg_rating": 1.0,
					"msgID":      uint64(0),
				}},
			}
			AssertResults(t, outputJoiner, expected)

			worker.Tasks.Finish()
			inputMovies.Close()
			inputRatings.Close()
			outputJoiner.Close()
			middlewareConnection.Close()
			outputConnection.Close()
		})

		t.Run("RestartProcessPendingMovies", func(t *testing.T) {
			init := test8container
			require.NotNil(t, init)
			require.NoError(t, init.Err)

			middlewareConnection := ConnectToRabbit(t, init, "0")

			inputMovies, inputRatings, worker, outputJoiner, outputConnection := configTestJoinerRatings(t, init, "output_test8", middlewareConnection)

			cid := uint64(80)
			SendRows(t, inputMovies, cid,
				uint64(0),
				&model.Row{Strings: map[string]string{"movieID": "A", "title": "Movie A"}},
				&model.Row{Strings: map[string]string{"movieID": "B", "title": "Movie B"}},
			)

			inputRatings.Send(&model.Row{
				Strings: map[string]string{"movieID": "A"},
				Floats:  map[string]float64{"avg_rating": 3.5}}, cid, uint64(2))

			joinerConnection := ConnectToRabbit(t, init, "0")
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				worker.Run(joinerConnection)
				joinerConnection.Close()
			}()

			ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
			defer cancel()
			env, err := outputJoiner.Next(ctx)
			assert.NoError(t, err)
			env.Ack(false)
			assert.Equal(t, env.Type(), middleware.Normal)
			assert.Equal(t, env.Msg().Strings["movieID"], "A")
			assert.Equal(t, env.Msg().Floats["avg_rating"], 3.5)
			assert.Equal(t, env.Id(), uint64(0))

			filePath := fmt.Sprintf("joiner_ratings/joiner0/%d/movies_B.csv", cid)

			waitForFile(t, filePath, 5*time.Second)

			worker.Tasks.Finish()
			wg.Wait()

			t.Log("Worker finished, sending more ratings")

			SendRows(t, inputRatings, cid,
				uint64(3),
				&model.Row{
					Strings: map[string]string{"movieID": "B"},
					Floats:  map[string]float64{"avg_rating": 1.0}},
			)

			workerLogger := logger.NewConsoleLogger("joiner_0", logger.Info)

			worker2 := joiner.NewRatingsWorker([]string{"output_test8"}, "0", workerLogger)

			joinerConnection = ConnectToRabbit(t, init, "0")
			go func() {
				worker2.Run(joinerConnection)
				joinerConnection.Close()
			}()

			expected := map[uint64][]map[string]any{
				cid: {
					{"movieID": "B", "avg_rating": 1.0, "title": "Movie B", "msgID": uint64(1)},
				},
			}
			AssertResults(t, outputJoiner, expected)

			worker.Tasks.Finish()

			inputMovies.Close()
			inputRatings.Close()
			outputJoiner.Close()
			middlewareConnection.Close()
			outputConnection.Close()
		})

		t.Run("ProcessCorruptRatingsFile", func(t *testing.T) {
			init := test13container
			require.NotNil(t, init)
			require.NoError(t, init.Err)

			cid := uint64(130)

			joinerID := "0"
			dir := fmt.Sprintf("joiner_ratings/joiner%s/%d", joinerID, cid)
			require.NoError(t, os.MkdirAll(dir, 0755))

			filePath := fmt.Sprintf("%s/ratings_0.csv", dir)
			file, err := os.Create(filePath)
			require.NoError(t, err)
			writer := csv.NewWriter(file)
			require.NoError(t, writer.Write([]string{"movieID", "rating"}))

			require.NoError(t, writer.Write([]string{"10", "1.5"}))

			require.NoError(t, writer.Write([]string{"20"}))
			writer.Flush()
			require.NoError(t, file.Close())

			middlewareConnection := ConnectToRabbit(t, init, "0")
			inputMovies, inputRatings, worker, outputJoiner, outputConnection := configTestJoinerRatings(t, init, "output_test13", middlewareConnection)

			joinerConnection := ConnectToRabbit(t, init, "0")
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				worker.Run(joinerConnection)
				joinerConnection.Close()
			}()

			time.Sleep(1 * time.Second)

			file, err = os.Open(filePath)
			require.NoError(t, err)
			reader := csv.NewReader(file)
			_, err = reader.Read()
			require.NoError(t, err)

			data, err := reader.Read()

			if err != nil || len(data) < 2 {
				require.Fail(t, fmt.Sprintf("invalid row in file %s: %v", filePath, err))
			}
			movieID := data[0]
			if movieID != "10" {
				require.Fail(t, fmt.Sprintf("expected movieID '10', got '%s' in file %s", movieID, filePath))
			}
			rating, err := strconv.ParseFloat(data[1], 64)
			require.NoError(t, err)
			require.Equal(t, 1.5, rating)

			_, err = reader.Read()
			require.Equal(t, io.EOF, err, fmt.Sprintf("expected EOF after reading movieID '10' in file %s", filePath))
			require.NoError(t, file.Close())

			inputMovies.SendEOF(cid)
			inputRatings.SendEOF(cid)

			AsserEOFs(t, outputJoiner, []uint64{cid})

			worker.Tasks.Finish()
			wg.Wait()

			inputMovies.Close()
			inputRatings.Close()
			outputJoiner.Close()
			middlewareConnection.Close()
			outputConnection.Close()
		})

		t.Run("ProcessCorruptPendingMoviesFile", func(t *testing.T) {
			testJoinerRatingsCorruptMoviesFile(t, uint64(140), "movies", test14container, "output_test14")
		})

		t.Run("ProcessCorruptProcessedMoviesFile", func(t *testing.T) {
			testJoinerRatingsCorruptMoviesFile(t, uint64(150), "processed_movies", test15container, "output_test15")
		})

		t.Run("MultipleJoinersMultipleClients", func(t *testing.T) {

			init := test18container
			require.NotNil(t, init)
			require.NoError(t, init.Err)

			cantWorkers := 5
			cantClients := 5
			middlewareSenderConnection := ConnectToRabbit(t, init, "sender")
			inputMovies, err := middlewareSenderConnection.WriteTo("filter_release_date_ge_2000_and_include_ar", []string{"joiner_ratings"}, "0", uint(cantWorkers))
			assert.NoError(t, err)
			susbscribers := make([]string, cantWorkers)
			for i := range cantWorkers {
				susbscribers[i] = fmt.Sprintf("joiner_%d_ratings", i)
			}
			inputRatings, err := middlewareSenderConnection.WriteTo("filter_avg_rating", susbscribers, "0", uint(cantWorkers))
			assert.NoError(t, err)

			baseCid := uint64(180)
			clients := make([]uint64, cantClients)
			for i := range cantClients {
				cid := baseCid + uint64(i)
				clients[i] = cid
				inputMovies.SendEOF(cid)
				inputRatings.SendEOF(cid)
			}

			os.Setenv("WORKER_COUNT", "5")
			workers := make([]joiner.Worker, cantWorkers)
			for i := range cantWorkers {
				ID := fmt.Sprintf("%d", i)
				workerLogger := logger.NewConsoleLogger(fmt.Sprintf("joiner_%s", ID), logger.Info)
				worker := joiner.NewRatingsWorker([]string{"test18"}, ID, workerLogger)
				workers[i] = worker
			}
			outputConnection := ConnectToRabbit(t, init, "output")
			outputJoiner, err := outputConnection.ConsumeFrom(workers[0].Tasks.Name(), "test18", "0", 1, uint(1))
			assert.NoError(t, err)

			var wg sync.WaitGroup
			wg.Add(5)
			worker0 := workers[0]
			worker1 := workers[1]
			worker2 := workers[2]
			worker3 := workers[3]
			worker4 := workers[4]

			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "4")
				worker4.Run(joinerConnection)
				joinerConnection.Close()
			}()
			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "1")
				worker1.Run(joinerConnection)
				joinerConnection.Close()
			}()

			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "2")
				worker2.Run(joinerConnection)
				joinerConnection.Close()
			}()

			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "3")
				worker3.Run(joinerConnection)
				joinerConnection.Close()
			}()

			go func() {
				defer wg.Done()
				joinerConnection := ConnectToRabbit(t, init, "0")
				worker0.Run(joinerConnection)
				joinerConnection.Close()
			}()

			AsserEOFs(t, outputJoiner, clients)

			os.Unsetenv("WORKER_COUNT")

			inputMovies.Close()
			inputRatings.Close()
			outputJoiner.Close()
			middlewareSenderConnection.Close()
			outputConnection.Close()
			for i := range cantWorkers {
				workers[i].Tasks.Finish()
			}

		})
	})
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

func SendRows(t *testing.T, sender middleware.Sender[*model.Row], cid uint64, idBase uint64, rows ...*model.Row) {
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

func AssertResults(t *testing.T, outputJoiner middleware.Receiver[*model.Row], expected map[uint64][]map[string]any) {
	steps := map[uint64]int{}
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
		require.NotNil(t, env, "Received nil envelope.")
		cid := env.Cid()
		env.Ack(false)

		stepCid, exists := steps[cid]
		assert.True(t, exists, "Client %d not found in expected results", cid)
		switch stepCid {
		case 0:
			row := env.Msg()
			require.NotNil(t, row, "Received nil row for client %d", cid)
			t.Logf("Received msg %+v for client %d", row, cid)
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
				case "msgID":
					assert.Equal(t, env.Id(), value, "Message ID mismatch for client %d", cid)
				}

			}
			if len(expected[cid]) > 1 {
				expected[cid] = expected[cid][1:]
				continue
			}

		case 1:
			assert.Equal(t, middleware.Prune, env.Type())
			//env.Ack(false)
			t.Logf("Received prune for client %d", cid)
		case 2:
			assert.Equal(t, middleware.EOF, env.Type())
			t.Logf("Received EOF for client %d", cid)
			delete(steps, cid)
		case 3:
			assert.FailNow(t, "Unexpected message for client %d", cid)
		}
		steps[cid]++
	}
	ExpectNoMoreRows(t, outputJoiner)
}

func AsserEOFs(t *testing.T, outputJoiner middleware.Receiver[*model.Row], clients []uint64) {
	steps := map[uint64]int{}
	countExpected := 0
	for _, client := range clients {
		steps[client] = 1
		countExpected += 2
	}
	t.Logf("Expecting %d messages", countExpected)
	for range countExpected {
		ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		env, err := outputJoiner.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.NotNil(t, env, "Received nil envelope.")
		cid := env.Cid()
		stepCid, exists := steps[cid]
		assert.True(t, exists, "Client %d not found in expected results", cid)
		env.Ack(false)
		switch stepCid {
		case 1:
			assert.Equal(t, middleware.Prune, env.Type(), "Expected prune message for client %d, got %+v", cid, env.Msg())
			t.Logf("Received prune for client %d", cid)
		case 2:
			assert.Equal(t, middleware.EOF, env.Type())
			t.Logf("Received EOF for client %d", cid)
			delete(steps, cid)
		case 3:
			assert.FailNow(t, "Unexpected message for client %d", cid)
		}
		steps[cid]++
	}
	ExpectNoMoreRows(t, outputJoiner)
}

func configTestJoinerRatings(t *testing.T, init rabbitmq.AsyncDeployRabbitRes, output string, middlewareConnection middleware.Connection[*model.Row]) (middleware.Sender[*model.Row], middleware.Sender[*model.Row], joiner.Worker, middleware.Receiver[*model.Row], middleware.Connection[*model.Row]) {
	movies := joiner.NewSourceTask[*model.Row]("filter_release_date_ge_2000_and_include_ar")
	filter_ratings := joiner.NewSourceTask[*model.Row]("filter_avg_rating")
	inputMovies, err := middlewareConnection.WriteTo(movies.Name(), []string{"joiner_ratings"}, "0", uint(1))
	assert.NoError(t, err)
	inputCredits, err := middlewareConnection.WriteTo(filter_ratings.Name(), []string{"joiner_0_ratings"}, "0", uint(1))
	assert.NoError(t, err)
	workerLogger := logger.NewConsoleLogger("joiner_0", logger.Debug)
	worker := joiner.NewRatingsWorker([]string{output}, "0", workerLogger)
	currentTask := worker.Tasks
	outputConnection := ConnectToRabbit(t, init, output)
	outputJoiner, err := outputConnection.ConsumeFrom(currentTask.Name(), output, "0", 1, uint(1))
	assert.NoError(t, err)
	return inputMovies, inputCredits, worker, outputJoiner, outputConnection
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)

	for {
		if _, err := os.Stat(path); err == nil {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("Timeout esperando el archivo: %s", path)
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func testJoinerCreditsCorruptMoviesFile(t *testing.T, cid uint64, fileName string, init rabbitmq.AsyncDeployRabbitRes, output string) {
	require.NotNil(t, init)
	require.NoError(t, init.Err)

	joinerID := "0"
	dir := fmt.Sprintf("joiner_credits/joiner%s/%d", joinerID, cid)
	require.NoError(t, os.MkdirAll(dir, 0755))

	filePath := fmt.Sprintf("%s/%s_0.csv", dir, fileName)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	writer := csv.NewWriter(file)
	require.NoError(t, writer.Write([]string{"movieID"}))

	require.NoError(t, writer.Write([]string{"10"}))

	require.NoError(t, writer.Write([]string{""}))
	writer.Flush()
	require.NoError(t, file.Close())

	middlewareConnection := ConnectToRabbit(t, init, "0")
	inputMovies, inputCredits, worker, outputJoiner, outputConnection := configTestJoinerCredits(t, init, output, middlewareConnection)

	joinerConnection := ConnectToRabbit(t, init, "0")
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		worker.Run(joinerConnection)
		joinerConnection.Close()
	}()

	time.Sleep(1 * time.Second)

	file, err = os.Open(filePath)
	require.NoError(t, err)
	reader := csv.NewReader(file)
	_, err = reader.Read()
	require.NoError(t, err)

	data, err := reader.Read()

	if err != nil || len(data) < 1 {
		require.Fail(t, fmt.Sprintf("invalid row in file %s: %v", filePath, err))
	}
	movieID := data[0]
	if movieID != "10" {
		require.Fail(t, fmt.Sprintf("expected movieID '10', got '%s' in file %s", movieID, filePath))
	}
	_, err = reader.Read()
	require.Equal(t, io.EOF, err, fmt.Sprintf("expected EOF after reading movieID '10' in file %s", filePath))
	require.NoError(t, file.Close())

	inputMovies.SendEOF(cid)
	inputCredits.SendEOF(cid)

	AsserEOFs(t, outputJoiner, []uint64{cid})

	worker.Tasks.Finish()
	wg.Wait()

	inputMovies.Close()
	inputCredits.Close()
	outputJoiner.Close()
	middlewareConnection.Close()
	outputConnection.Close()

}

func testJoinerRatingsCorruptMoviesFile(t *testing.T, cid uint64, fileName string, init rabbitmq.AsyncDeployRabbitRes, output string) {
	require.NotNil(t, init)
	require.NoError(t, init.Err)

	joinerID := "0"
	dir := fmt.Sprintf("joiner_ratings/joiner%s/%d", joinerID, cid)
	require.NoError(t, os.MkdirAll(dir, 0755))

	filePath := fmt.Sprintf("%s/%s_0.csv", dir, fileName)
	file, err := os.Create(filePath)
	require.NoError(t, err)
	writer := csv.NewWriter(file)
	require.NoError(t, writer.Write([]string{"movieID", "title"}))

	require.NoError(t, writer.Write([]string{"10", "title A"}))

	require.NoError(t, writer.Write([]string{"20"}))
	writer.Flush()
	require.NoError(t, file.Close())

	middlewareConnection := ConnectToRabbit(t, init, "0")
	inputMovies, inputRatings, worker, outputJoiner, outputConnection := configTestJoinerRatings(t, init, output, middlewareConnection)

	joinerConnection := ConnectToRabbit(t, init, "0")
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		worker.Run(joinerConnection)
		joinerConnection.Close()
	}()

	time.Sleep(1 * time.Second)

	file, err = os.Open(filePath)
	require.NoError(t, err)
	reader := csv.NewReader(file)
	_, err = reader.Read()
	require.NoError(t, err)

	data, err := reader.Read()

	if err != nil || len(data) < 2 {
		require.Fail(t, fmt.Sprintf("invalid row in file %s: %v", filePath, err))
	}
	movieID := data[0]
	if movieID != "10" {
		require.Fail(t, fmt.Sprintf("expected movieID '10', got '%s' in file %s", movieID, filePath))
	}
	if data[1] != "title A" {
		t.Fatalf("Unexpected title value: got %q, want 'title A'", data[1])
	}
	_, err = reader.Read()
	require.Equal(t, io.EOF, err, fmt.Sprintf("expected EOF after reading movieID '10' in file %s", filePath))
	require.NoError(t, file.Close())

	inputMovies.SendEOF(cid)
	inputRatings.SendEOF(cid)

	AsserEOFs(t, outputJoiner, []uint64{cid})

	worker.Tasks.Finish()
	wg.Wait()

	inputMovies.Close()
	inputRatings.Close()
	outputJoiner.Close()
	middlewareConnection.Close()
	outputConnection.Close()

}
