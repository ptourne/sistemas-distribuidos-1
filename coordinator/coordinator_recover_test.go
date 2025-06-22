package main

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/coordinator/transaction_log"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
	"github.com/stretchr/testify/assert"
)

func skipCI(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}
}

const RABBITMQ_EXPOSED_PORT_BASE = uint16(4000)

var baseConfig = rabbitmq.NewConfiguration("guest", "guest", "localhost", RABBITMQ_EXPOSED_PORT_BASE)

func startCoordinator(t *testing.T, init rabbitmq.AsyncDeployRabbitRes) (middleware.Sender[*common.PackageFile], middleware.Receiver[*model.Row]) {
	senderConnector, err := rabbitmq.ConnectorCustom(init.Config)
	assert.NoError(t, err)

	middlewareSenderLogger := logger.NewConsoleLogger("midd_send", logger.Info)
	fileBytes := "file_bytes"
	middlewareChanByte := rabbitmq.NewMiddleware[*common.PackageFile](senderConnector, middlewareSenderLogger)
	sender, err := middlewareChanByte.WriteTo(fileBytes, []string{"file_bytes"}, "0", 1)
	assert.NoError(t, err)
	receiverConnector, err := rabbitmq.ConnectorCustom(init.Config)
	assert.NoError(t, err)
	middlewareReceiverLogger := logger.NewConsoleLogger("midd_rec", logger.Info)

	middlewareChanRow := rabbitmq.NewMiddleware[*model.Row](receiverConnector, middlewareReceiverLogger)
	test_csv := "test_csv"
	prefetch := 100
	receiver, err := middlewareChanRow.ConsumeFrom(test_csv, test_csv, "0", prefetch, 1)
	assert.NoError(t, err)

	return sender, receiver
}

func startCoordinatorQuery(t *testing.T, init rabbitmq.AsyncDeployRabbitRes) (middleware.Sender[*common.PackageFile], middleware.Receiver[*model.Row], middleware.Sender[*model.Row], middleware.Sender[*model.Row], middleware.Sender[*model.Row], middleware.Sender[*model.Row], middleware.Sender[*model.Row], middleware.Sender[*model.Row]) {
	senderConnector, err := rabbitmq.ConnectorCustom(init.Config)
	assert.NoError(t, err)

	middlewareSenderLogger := logger.NewConsoleLogger("midd_send", logger.Info)
	fileBytes := "file_bytes"
	middlewareChanByte := rabbitmq.NewMiddleware[*common.PackageFile](senderConnector, middlewareSenderLogger)
	sender, err := middlewareChanByte.WriteTo(fileBytes, []string{"file_bytes"}, "0", 1)
	assert.NoError(t, err)
	receiverConnector, err := rabbitmq.ConnectorCustom(init.Config)
	assert.NoError(t, err)
	middlewareReceiverLogger := logger.NewConsoleLogger("midd_rec", logger.Info)

	middlewareChanRow := rabbitmq.NewMiddleware[*model.Row](receiverConnector, middlewareReceiverLogger)
	test_csv := "test_csv"
	prefetch := 100
	receiver, err := middlewareChanRow.ConsumeFrom(test_csv, test_csv, "0", prefetch, 1)
	assert.NoError(t, err)

	//hago senders de colas de q1, q2, q3, q4, q5
	q1Sender, err := middlewareChanRow.WriteTo("filter_release_date_l_2010_and_include_es", []string{"q1"}, "0", 1)
	assert.NoError(t, err)
	q2Sender, err := middlewareChanRow.WriteTo("reduce_top_5_by_budget", []string{"q2"}, "0", 1)
	assert.NoError(t, err)
	q3Sender, err := middlewareChanRow.WriteTo("reduce_top_bottom_avg_rating", []string{"q3"}, "0", 1)
	assert.NoError(t, err)
	q4Sender, err := middlewareChanRow.WriteTo("reduce_top_10_by_actor", []string{"q4"}, "0", 1)
	assert.NoError(t, err)
	q5Sender, err := middlewareChanRow.WriteTo("filter_avg_rate", []string{"q5"}, "0", 1)
	assert.NoError(t, err)

	allQuerysToEndpointSender, err := middlewareChanRow.WriteTo("all_querys_to_endpoint", []string{"all_querys_to_endpoint"}, "0", 1)
	assert.NoError(t, err)

	return sender, receiver, q1Sender, q2Sender, q3Sender, q4Sender, q5Sender, allQuerysToEndpointSender
}

func TestMapReducer(t *testing.T) {
	skipCI(t)
	provider := rabbitmq.NewContainerProvider(baseConfig)

	test1 := provider.AsyncDeployRabbit()
	test2 := provider.AsyncDeployRabbit()
	test3 := provider.AsyncDeployRabbit()
	test4 := provider.AsyncDeployRabbit()
	test5 := provider.AsyncDeployRabbit()
	test6 := provider.AsyncDeployRabbit()
	test7 := provider.AsyncDeployRabbit()
	// test8 := provider.AsyncDeployRabbit()

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
	// test8container := <-test8
	// defer test8container.Container.Teardown()

	t.Run("CoordinatorLogEmpty", func(t *testing.T) {
		init := test1container
		assert.NoError(t, init.Err)
		sender, receiver := startCoordinator(t, init)
		defer sender.Close()
		defer receiver.Close()
		cid := uint64(1)

		log := logger.NewConsoleLogger("coordinator", logger.Info)
		tmpDir := t.TempDir()
		ctx, ctxStop, wg := coordinatorRun(t, init, log, tmpDir, false)

		// 1. Enviar un string CSV
		fileName := "test_csv"
		sendCoordinator(t, fileName, sender, common.FileName, cid, 1)

		csvData := "id,name\n1,Alice\n2,Bob\n"

		sendCoordinator(t, csvData, sender, common.FileData, cid, 2)

		eofData := "EOF"
		sendCoordinator(t, eofData, sender, common.FinishFile, cid, 3)
		sendCoordinator(t, eofData, sender, common.AllFilesSent, cid, 4)

		log = logger.NewConsoleLogger("test", logger.Info)
		var rows []middleware.Envelope[*model.Row]
		for i := 0; i < 4; i++ {
			row, err := receiver.Next(ctx)
			assert.NoError(t, err)
			rows = append(rows, row)
			row.Ack(false)
		}

		log.Infof("CHECKING rows")
		r0 := rows[0].Msg()
		assert.Equal(t, r0.Strings["id"], "1")
		assert.Equal(t, r0.Strings["name"], "Alice")

		r1 := rows[1].Msg()
		assert.Equal(t, r1.Strings["id"], "2")
		assert.Equal(t, r1.Strings["name"], "Bob")

		r2 := rows[2]
		assert.Equal(t, r2.Type(), middleware.Prune)

		log.Infof("CHECKING EOF")
		r3 := rows[3]
		assert.Equal(t, r3.Type(), middleware.EOF)

		time.Sleep(1 * time.Second)
		ctxStop()
		wg.Wait()
		logFilePath := path.Join(tmpDir, "logs", "1", "log")
		_, err := os.Stat(logFilePath)
		assert.Error(t, err, "Expected log file to not exist, but it does exist")
	})

	t.Run("CoordinatorLogMultipleClients", func(t *testing.T) {
		init := test2container
		assert.NoError(t, init.Err)
		sender, receiver := startCoordinator(t, init)
		defer sender.Close()
		defer receiver.Close()

		log := logger.NewConsoleLogger("coordinator", logger.Info)
		tmpDir := t.TempDir()
		ctx, ctxStop, wg := coordinatorRun(t, init, log, tmpDir, false)

		// Send data from multiple clients
		clients := []uint64{1, 2, 3}
		for _, cid := range clients {
			fileName := "test_csv"
			sendCoordinator(t, fileName, sender, common.FileName, cid, 1)
			csvData := "id,name\n1,Alice\n2,Bob\n"
			sendCoordinator(t, csvData, sender, common.FileData, cid, 2)
			eofData := "EOF"
			sendCoordinator(t, eofData, sender, common.FinishFile, cid, 3)
			sendCoordinator(t, eofData, sender, common.AllFilesSent, cid, 4)
		}

		var rowsC map[uint64][]middleware.Envelope[*model.Row] = map[uint64][]middleware.Envelope[*model.Row]{}
		// Verify all clients' data is processed
		log = logger.NewConsoleLogger("test", logger.Info)
		for i := 0; i < len(clients)*4; i++ {
			row, err := receiver.Next(ctx)
			assert.NoError(t, err)
			rowsC[row.Cid()] = append(rowsC[row.Cid()], row)
			row.Ack(false)
		}

		for _, rows := range rowsC {
			log.Infof("CHECKING rows")
			r0 := rows[0].Msg()
			assert.Equal(t, r0.Strings["id"], "1")
			assert.Equal(t, r0.Strings["name"], "Alice")

			r1 := rows[1].Msg()
			assert.Equal(t, r1.Strings["id"], "2")
			assert.Equal(t, r1.Strings["name"], "Bob")

			r2 := rows[2]
			assert.Equal(t, r2.Type(), middleware.Prune)

			log.Infof("CHECKING EOF")
			r3 := rows[3]
			assert.Equal(t, r3.Type(), middleware.EOF)
		}

		log.Infof("CHECKING log files")
		time.Sleep(1 * time.Second)
		ctxStop()
		wg.Wait()
		for _, cid := range clients {
			logFilePath := path.Join(tmpDir, "logs", fmt.Sprintf("%d", cid), "log")
			_, err := os.Stat(logFilePath)
			assert.Error(t, err, "Expected log file to not exist, but it does exist")
		}
	})

	t.Run("CoordinatorLargeLine", func(t *testing.T) {
		init := test3container
		assert.NoError(t, init.Err)
		sender, receiver := startCoordinator(t, init)
		defer sender.Close()
		defer receiver.Close()
		cid := uint64(1)

		log := logger.NewConsoleLogger("coordinator", logger.Info)
		tmpDir := t.TempDir()
		ctx, ctxStop, wg := coordinatorRun(t, init, log, tmpDir, false)

		// Enviar un archivo CSV con una línea muy larga
		fileName := "test_csv"
		sendCoordinator(t, fileName, sender, common.FileName, cid, 1)

		header := "id,name\n"
		sendCoordinator(t, header, sender, common.FileData, cid, 2)

		// Crear una línea con una descripción muy larga
		longDescription1 := make([]byte, 2100)
		for i := range longDescription1 {
			longDescription1[i] = 'x'
		}
		longLine := fmt.Sprintf("1,%s", string(longDescription1))
		sendCoordinator(t, longLine, sender, common.FileData, cid, 3)

		longDescription2 := make([]byte, 2100)
		for i := range longDescription2 {
			longDescription2[i] = 'y'
		}
		longLine = string(longDescription2)
		sendCoordinator(t, longLine, sender, common.FileData, cid, 4)

		longDescription3 := make([]byte, 10)
		for i := range longDescription3 {
			longDescription3[i] = 'k'
		}
		longLine = fmt.Sprintf("%s\n", string(longDescription3))
		sendCoordinator(t, longLine, sender, common.FileData, cid, 5)

		// Enviar EOF
		eofData := "EOF"
		sendCoordinator(t, eofData, sender, common.FinishFile, cid, 4)
		sendCoordinator(t, eofData, sender, common.AllFilesSent, cid, 5)

		msgCompleto := make([]byte, len(longDescription1)+len(longDescription2)+len(longDescription3))
		copy(msgCompleto, longDescription1)
		copy(msgCompleto[len(longDescription1):], longDescription2)
		copy(msgCompleto[len(longDescription1)+len(longDescription2):], longDescription3)

		// Verificar que se procesó correctamente
		var rows []middleware.Envelope[*model.Row]
		for i := 0; i < 3; i++ {
			row, err := receiver.Next(ctx)
			assert.NoError(t, err)
			rows = append(rows, row)
			row.Ack(false)
		}

		// Verificar el contenido
		r0 := rows[0].Msg()
		assert.Equal(t, "1", r0.Strings["id"])
		assert.Equal(t, string(msgCompleto), r0.Strings["name"])

		r1 := rows[1]
		assert.Equal(t, middleware.Prune, r1.Type())

		r2 := rows[2]
		assert.Equal(t, middleware.EOF, r2.Type())

		// Detener el coordinador y verificar el log
		time.Sleep(3 * time.Second)
		ctxStop()
		wg.Wait()

		// Verificar que el log se limpió correctamente
		logFilePath := path.Join(tmpDir, "logs", "1", "log")
		_, err := os.Stat(logFilePath)
		assert.Error(t, err, "Expected log file to not exist, but it does exist")
	})

	t.Run("CoordinatorLogRecoveryFirstMsg", func(t *testing.T) {
		init := test4container
		assert.NoError(t, init.Err)
		sender, receiver := startCoordinator(t, init)
		defer sender.Close()
		defer receiver.Close()
		cid := uint64(1)

		log := logger.NewConsoleLogger("coordinator", logger.Info)
		tmpDir := t.TempDir()
		_, ctxStop, wg := coordinatorRun(t, init, log, tmpDir, false)

		log.Infof("envio filename")
		fileName := "test_csv"
		sendCoordinator(t, fileName, sender, common.FileName, cid, 1)

		log.Infof("envio header")
		csvData := "id,name\n"
		sendCoordinator(t, csvData, sender, common.FileData, cid, 2)
		gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK := stopCoordinatorAndReadLog(t, ctxStop, wg, tmpDir, cid)
		assert.Equal(t, fileName, gotFilename)
		assert.Equal(t, uint64(0), gotCounter)
		assert.Equal(t, []string{"id", "name"}, gotRead)
		assert.Equal(t, []byte{}, gotLastReadNotIncluided)
		assert.Equal(t, uint64(2), gotLastIdACK)

		log = logger.NewConsoleLogger("test", logger.Info)

		log.Infof("envio primer paquete")
		_, ctxStop, wg = coordinatorRun(t, init, log, tmpDir, false)
		csvData = "1,Alice"
		sendCoordinator(t, csvData, sender, common.FileData, cid, 3)
		gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK = stopCoordinatorAndReadLog(t, ctxStop, wg, tmpDir, cid)
		assert.Equal(t, fileName, gotFilename)
		assert.Equal(t, uint64(0), gotCounter)
		assert.Equal(t, []string{"id", "name"}, gotRead)
		assert.Equal(t, []byte{}, gotLastReadNotIncluided)
		assert.Equal(t, gotLastIdACK, uint64(2))

		log.Infof("envio segundo paquete")
		_, ctxStop, wg = coordinatorRun(t, init, log, tmpDir, false)
		csvData = "\n2,Don"
		sendCoordinator(t, csvData, sender, common.FileData, cid, 4)
		gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK = stopCoordinatorAndReadLog(t, ctxStop, wg, tmpDir, cid)
		assert.Equal(t, gotFilename, fileName)
		assert.Equal(t, gotCounter, uint64(1))
		assert.Equal(t, gotRead, []string{"1", "Alice"})
		assert.Equal(t, string(gotLastReadNotIncluided), "2,Don")
		assert.Equal(t, gotLastIdACK, uint64(4))

		log.Infof("envio paquete final y eofs")
		csvData = "\n"
		sendCoordinator(t, csvData, sender, common.FileData, cid, 5)
		sendCoordinator(t, "EOF", sender, common.FinishFile, cid, 6)
		sendCoordinator(t, "EOF", sender, common.AllFilesSent, cid, 7)
		ctx, ctxStop, wg := coordinatorRun(t, init, log, tmpDir, false)

		//verifico que lleguen los paquetes
		var rows []middleware.Envelope[*model.Row]
		for i := 0; i < 5; i++ {
			row, err := receiver.Next(ctx)
			log.Infof("row: %v", row.Msg())
			assert.NoError(t, err)
			rows = append(rows, row)
			row.Ack(false)
		}

		log.Infof("verifico que lleguen los paquetes")
		r0 := rows[0].Msg()
		assert.Equal(t, r0.Strings["id"], "1")
		assert.Equal(t, r0.Strings["name"], "Alice")
		assert.Equal(t, rows[0].Cid(), uint64(1))
		assert.Equal(t, rows[0].Id(), uint64(1))

		r1 := rows[1].Msg()
		assert.Equal(t, r1.Strings["id"], "1")
		assert.Equal(t, r1.Strings["name"], "Alice")
		assert.Equal(t, rows[1].Cid(), uint64(1))
		assert.Equal(t, rows[1].Id(), uint64(1))

		r2 := rows[2].Msg()
		assert.Equal(t, r2.Strings["id"], "2")
		assert.Equal(t, r2.Strings["name"], "Don")
		assert.Equal(t, rows[2].Cid(), uint64(1))
		assert.Equal(t, rows[2].Id(), uint64(3))

		r3 := rows[3]
		assert.Equal(t, r3.Type(), middleware.Prune)

		r4 := rows[4]
		assert.Equal(t, r4.Type(), middleware.EOF)

		log.Infof("verifico que el log se limpió correctamente")
		time.Sleep(1 * time.Second)
		ctxStop()
		wg.Wait()
	})

	t.Run("CoordinatorLargeLineLogRecovery", func(t *testing.T) {
		init := test5container
		assert.NoError(t, init.Err)
		sender, receiver := startCoordinator(t, init)
		defer sender.Close()
		defer receiver.Close()
		cid := uint64(1)

		log := logger.NewConsoleLogger("coordinator", logger.Info)
		tmpDir := t.TempDir()
		_, ctxStop, wg := coordinatorRun(t, init, log, tmpDir, false)

		log2 := logger.NewConsoleLogger("test", logger.Info)

		log.Infof("envio filename")
		// Enviar un archivo CSV con una línea muy larga
		fileName := "test_csv"
		sendCoordinator(t, fileName, sender, common.FileName, cid, 1)

		log2.Infof("envio header")
		header := "id,name\n"
		sendCoordinator(t, header, sender, common.FileData, cid, 2)

		log2.Infof("envio primer paquete")
		// Crear una línea con una descripción muy larga
		longDescription1 := make([]byte, 2100)
		for i := range longDescription1 {
			longDescription1[i] = 'x'
		}
		longLine := fmt.Sprintf("1,%s", string(longDescription1))
		sendCoordinator(t, longLine, sender, common.FileData, cid, 3)

		gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK := stopCoordinatorAndReadLog(t, ctxStop, wg, tmpDir, cid)
		assert.Equal(t, fileName, gotFilename)
		assert.Equal(t, uint64(0), gotCounter)
		assert.Equal(t, []string{"id", "name"}, gotRead)
		assert.Equal(t, []byte{}, gotLastReadNotIncluided)
		assert.Equal(t, uint64(2), gotLastIdACK)

		log2.Infof("envio segundo paquete")
		_, ctxStop, wg = coordinatorRun(t, init, log, tmpDir, false)
		longDescription2 := make([]byte, 2100)
		for i := range longDescription2 {
			longDescription2[i] = 'y'
		}
		longLine = string(longDescription2)
		sendCoordinator(t, longLine, sender, common.FileData, cid, 4)

		gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK = stopCoordinatorAndReadLog(t, ctxStop, wg, tmpDir, cid)
		assert.Equal(t, fileName, gotFilename)
		assert.Equal(t, uint64(0), gotCounter)
		assert.Equal(t, []string{"id", "name"}, gotRead)
		assert.Equal(t, []byte{}, gotLastReadNotIncluided)
		assert.Equal(t, uint64(2), gotLastIdACK)

		log2.Infof("envio tercer paquete")
		_, ctxStop, wg = coordinatorRun(t, init, log, tmpDir, false)
		longDescription3 := make([]byte, 10)
		for i := range longDescription3 {
			longDescription3[i] = 'k'
		}
		longLine = fmt.Sprintf("%s\n", string(longDescription3))
		sendCoordinator(t, longLine, sender, common.FileData, cid, 5)

		msgCompleto := make([]byte, len(longDescription1)+len(longDescription2)+len(longDescription3))
		copy(msgCompleto, longDescription1)
		copy(msgCompleto[len(longDescription1):], longDescription2)
		copy(msgCompleto[len(longDescription1)+len(longDescription2):], longDescription3)

		gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK = stopCoordinatorAndReadLog(t, ctxStop, wg, tmpDir, cid)
		assert.Equal(t, fileName, gotFilename)
		assert.Equal(t, uint64(1), gotCounter)
		assert.Equal(t, []string{"1", string(msgCompleto)}, gotRead)
		assert.Equal(t, []byte{}, gotLastReadNotIncluided)
		assert.Equal(t, uint64(5), gotLastIdACK)

		// Enviar EOF
		log2.Infof("envio EOF")
		ctx, ctxStop, wg := coordinatorRun(t, init, log, tmpDir, false)
		eofData := "EOF"
		sendCoordinator(t, eofData, sender, common.FinishFile, cid, 6)
		sendCoordinator(t, eofData, sender, common.AllFilesSent, cid, 7)

		// Verificar que se procesó correctamente
		log2.Infof("verifico que lleguen los paquetes")
		var rows []middleware.Envelope[*model.Row]
		for i := 0; i < 4; i++ {
			row, err := receiver.Next(ctx)
			assert.NoError(t, err)
			rows = append(rows, row)
			row.Ack(false)
		}

		// Verificar el contenido
		r0 := rows[0].Msg()
		assert.Equal(t, "1", r0.Strings["id"])
		assert.Equal(t, string(msgCompleto), r0.Strings["name"])

		r1 := rows[1].Msg()
		assert.Equal(t, "1", r1.Strings["id"])
		assert.Equal(t, string(msgCompleto), r1.Strings["name"])

		r2 := rows[2]
		assert.Equal(t, middleware.Prune, r2.Type())

		r3 := rows[3]
		assert.Equal(t, middleware.EOF, r3.Type())

		// Detener el coordinador y verificar el log
		time.Sleep(3 * time.Second)
		ctxStop()
		wg.Wait()

		// Verificar que el log se limpió correctamente
		logFilePath := path.Join(tmpDir, "logs", "1", "log")
		_, err := os.Stat(logFilePath)
		assert.Error(t, err, "Expected log file to not exist, but it does exist")
	})

	t.Run("CoordinatorLogRecoveryEOFS", func(t *testing.T) {
		init := test6container
		assert.NoError(t, init.Err)
		sender, receiver := startCoordinator(t, init)
		defer sender.Close()
		defer receiver.Close()
		cid := uint64(1)

		log := logger.NewConsoleLogger("coordinator", logger.Info)
		tmpDir := t.TempDir()
		_, ctxStop, wg := coordinatorRun(t, init, log, tmpDir, false)

		log.Infof("envio filename")
		fileName := "test_csv"
		sendCoordinator(t, fileName, sender, common.FileName, cid, 1)

		log.Infof("envio header y primer paquete")
		csvData := "id,name\n1,Alice\n"
		sendCoordinator(t, csvData, sender, common.FileData, cid, 2)
		gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK := stopCoordinatorAndReadLog(t, ctxStop, wg, tmpDir, cid)
		assert.Equal(t, fileName, gotFilename)
		assert.Equal(t, uint64(1), gotCounter)
		assert.Equal(t, []string{"1", "Alice"}, gotRead)
		assert.Equal(t, []byte{}, gotLastReadNotIncluided)
		assert.Equal(t, uint64(2), gotLastIdACK)

		log = logger.NewConsoleLogger("test", logger.Info)

		log.Infof("envio paquete final y eofs")
		csvData = "\n"
		sendCoordinator(t, csvData, sender, common.FileData, cid, 5)
		sendCoordinator(t, "EOF", sender, common.FinishFile, cid, 6)
		sendCoordinator(t, "EOF", sender, common.AllFilesSent, cid, 7)
		ctx, ctxStop, wg := coordinatorRun(t, init, log, tmpDir, false)

		//verifico que lleguen los paquetes
		var rows []middleware.Envelope[*model.Row]
		for i := 0; i < 4; i++ {
			log.Infof("i: %d", i)
			row, err := receiver.Next(ctx)
			log.Infof("row1: %v", row.Msg())
			assert.NoError(t, err)
			rows = append(rows, row)
			row.Ack(false)
		}

		log.Infof("verifico que lleguen los paquetes")
		r0 := rows[0].Msg()
		assert.Equal(t, "1", r0.Strings["id"])
		assert.Equal(t, "Alice", r0.Strings["name"])
		assert.Equal(t, uint64(1), rows[0].Cid())
		assert.Equal(t, uint64(1), rows[0].Id())

		r1 := rows[1].Msg()
		assert.Equal(t, "1", r1.Strings["id"])
		assert.Equal(t, "Alice", r1.Strings["name"])
		assert.Equal(t, uint64(1), rows[1].Cid())
		assert.Equal(t, uint64(1), rows[1].Id())

		r2 := rows[2]
		assert.Equal(t, middleware.Prune, r2.Type())

		r3 := rows[3]
		assert.Equal(t, middleware.EOF, r3.Type())

		log.Infof("verifico que el log se limpió correctamente")
		time.Sleep(1 * time.Second)
		logFilePath := path.Join(tmpDir, "logs", "1", "log")
		_, err := os.Stat(logFilePath)
		assert.Error(t, err, "Expected log file to not exist, but it does exist")
		ctxStop()
		wg.Wait()
	})

	t.Run("CoordinatorLogRecoveryEOFSeparated", func(t *testing.T) {
		init := test7container
		assert.NoError(t, init.Err)
		sender, receiver := startCoordinator(t, init)
		defer sender.Close()
		defer receiver.Close()
		cid := uint64(1)

		log := logger.NewConsoleLogger("coordinator", logger.Info)
		tmpDir := t.TempDir()
		_, ctxStop, wg := coordinatorRun(t, init, log, tmpDir, false)

		log.Infof("envio filename")
		fileName := "test_csv"
		sendCoordinator(t, fileName, sender, common.FileName, cid, 1)

		log.Infof("envio header y primer paquete")
		csvData := "id,name\n1,Alice\n"
		sendCoordinator(t, csvData, sender, common.FileData, cid, 2)
		gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK := stopCoordinatorAndReadLog(t, ctxStop, wg, tmpDir, cid)
		assert.Equal(t, fileName, gotFilename)
		assert.Equal(t, uint64(1), gotCounter)
		assert.Equal(t, []string{"1", "Alice"}, gotRead)
		assert.Equal(t, []byte{}, gotLastReadNotIncluided)
		assert.Equal(t, uint64(2), gotLastIdACK)

		log = logger.NewConsoleLogger("test", logger.Info)

		log.Infof("envio paquete final y primer eof")
		_, ctxStop, wg = coordinatorRun(t, init, log, tmpDir, false)
		csvData = "\n"
		sendCoordinator(t, csvData, sender, common.FileData, cid, 5)
		sendCoordinator(t, "EOF", sender, common.FinishFile, cid, 6)
		stopCoordinatorAndReadLogError(t, ctxStop, wg, tmpDir, cid)

		log.Infof("envio segundo eof")
		sendCoordinator(t, "EOF", sender, common.AllFilesSent, cid, 7)
		ctx, ctxStop, wg := coordinatorRun(t, init, log, tmpDir, false)

		//verifico que lleguen los paquetes
		var rows []middleware.Envelope[*model.Row]
		for i := 0; i < 4; i++ {
			log.Infof("i: %d", i)
			row, err := receiver.Next(ctx)
			log.Infof("row1: %v", row.Msg())
			assert.NoError(t, err)
			rows = append(rows, row)
			row.Ack(false)
		}

		log.Infof("verifico que lleguen los paquetes")
		r0 := rows[0].Msg()
		assert.Equal(t, "1", r0.Strings["id"])
		assert.Equal(t, "Alice", r0.Strings["name"])
		assert.Equal(t, uint64(1), rows[0].Cid())
		assert.Equal(t, uint64(1), rows[0].Id())

		r1 := rows[1].Msg()
		assert.Equal(t, "1", r1.Strings["id"])
		assert.Equal(t, "Alice", r1.Strings["name"])
		assert.Equal(t, uint64(1), rows[1].Cid())
		assert.Equal(t, uint64(1), rows[1].Id())

		r2 := rows[2]
		assert.Equal(t, middleware.Prune, r2.Type())

		r3 := rows[3]
		assert.Equal(t, middleware.EOF, r3.Type())

		log.Infof("verifico que el log se limpió correctamente")
		time.Sleep(1 * time.Second)
		logFilePath := path.Join(tmpDir, "logs", "1", "log")
		_, err := os.Stat(logFilePath)
		assert.Error(t, err, "Expected log file to not exist, but it does exist")
		ctxStop()
		wg.Wait()
	})

	t.Run("CoordinatorLogRecoveryMsgError", func(t *testing.T) {
		init := test4container
		assert.NoError(t, init.Err)
		sender, receiver := startCoordinator(t, init)
		defer sender.Close()
		defer receiver.Close()
		cid := uint64(1)

		log := logger.NewConsoleLogger("coordinator", logger.Info)
		tmpDir := t.TempDir()
		_, ctxStop, wg := coordinatorRun(t, init, log, tmpDir, false)

		log.Infof("envio filename")
		fileName := "test_csv"
		sendCoordinator(t, fileName, sender, common.FileName, cid, 1)

		log.Infof("envio header")
		csvData := "id,name\n1\n"
		sendCoordinator(t, csvData, sender, common.FileData, cid, 2)
		gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK := stopCoordinatorAndReadLog(t, ctxStop, wg, tmpDir, cid)
		assert.Equal(t, fileName, gotFilename)
		assert.Equal(t, uint64(1), gotCounter)
		assert.Equal(t, []string{}, gotRead)
		assert.Equal(t, string(""), string(gotLastReadNotIncluided))
		assert.Equal(t, uint64(2), gotLastIdACK)

		log = logger.NewConsoleLogger("test", logger.Info)

		log.Infof("envio primer paquete")
		_, ctxStop, wg = coordinatorRun(t, init, log, tmpDir, false)
		csvData = "1,Alice"
		sendCoordinator(t, csvData, sender, common.FileData, cid, 3)
		gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK = stopCoordinatorAndReadLog(t, ctxStop, wg, tmpDir, cid)
		assert.Equal(t, fileName, gotFilename)
		assert.Equal(t, uint64(1), gotCounter)
		assert.Equal(t, []string{}, gotRead)
		assert.Equal(t, "", string(gotLastReadNotIncluided))
		assert.Equal(t, uint64(2), gotLastIdACK)

		log.Infof("envio segundo paquete")
		_, ctxStop, wg = coordinatorRun(t, init, log, tmpDir, false)
		csvData = "\n2\n"
		sendCoordinator(t, csvData, sender, common.FileData, cid, 4)
		gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK = stopCoordinatorAndReadLog(t, ctxStop, wg, tmpDir, cid)
		assert.Equal(t, fileName, gotFilename)
		assert.Equal(t, uint64(3), gotCounter)
		assert.Equal(t, []string{}, gotRead)
		assert.Equal(t, "", string(gotLastReadNotIncluided))
		assert.Equal(t, uint64(4), gotLastIdACK)

		log.Infof("envio eofs")
		sendCoordinator(t, "EOF", sender, common.FinishFile, cid, 6)
		sendCoordinator(t, "EOF", sender, common.AllFilesSent, cid, 7)
		ctx, ctxStop, wg := coordinatorRun(t, init, log, tmpDir, false)

		//verifico que lleguen los paquetes
		var rows []middleware.Envelope[*model.Row]
		for i := 0; i < 3; i++ {
			row, err := receiver.Next(ctx)
			log.Infof("row: %v", row.Msg())
			assert.NoError(t, err)
			rows = append(rows, row)
			row.Ack(false)
		}

		log.Infof("verifico que lleguen los paquetes")
		r0 := rows[0].Msg()
		assert.Equal(t, r0.Strings["id"], "1")
		assert.Equal(t, r0.Strings["name"], "Alice")
		assert.Equal(t, uint64(1), rows[0].Cid())
		assert.Equal(t, uint64(2), rows[0].Id())

		r1 := rows[1]
		assert.Equal(t, r1.Type(), middleware.Prune)

		r2 := rows[2]
		assert.Equal(t, r2.Type(), middleware.EOF)

		log.Infof("verifico que el log se limpió correctamente")
		time.Sleep(1 * time.Second)
		ctxStop()
		wg.Wait()
	})

	t.Run("CoordinatorLogRecoveryQueryPhaseQ1", func(t *testing.T) {
		init := test1container
		assert.NoError(t, init.Err)
		sender, receiver, q1Sender, q2Sender, q3Sender, q4Sender, q5Sender, allQuerysToEndpointSender := startCoordinatorQuery(t, init)
		defer sender.Close()
		defer receiver.Close()
		defer q1Sender.Close()
		defer q2Sender.Close()
		defer q3Sender.Close()
		defer q4Sender.Close()
		defer q5Sender.Close()
		defer allQuerysToEndpointSender.Close()
		cid := uint64(1)

		log := logger.NewConsoleLogger("coordinator", logger.Info)
		tmpDir := t.TempDir()
		_, ctxStop, wg := coordinatorRun(t, init, log, tmpDir, true)

		// Send file data and process it
		fileName := "test_csv"
		sendCoordinator(t, fileName, sender, common.FileName, cid, 1)
		csvData := "id,name\n1,Alice\n2,Bob\n"
		sendCoordinator(t, csvData, sender, common.FileData, cid, 2)
		sendCoordinator(t, "EOF", sender, common.FinishFile, cid, 3)
		sendCoordinator(t, "EOF", sender, common.AllFilesSent, cid, 4)

		time.Sleep(1 * time.Second)
		rowQ1 := model.Row{
			Strings: map[string]string{"title": "Alice"},
			Arrays:  map[string][]string{"genres": {"Action", "Adventure", "Sci-Fi"}},
		}
		sendQ(t, q1Sender, cid, 1, &rowQ1)
		sendQEOF(t, q1Sender, cid)

		qNumber, rows, ended := stopCoordinatorAndReadLogQuery(t, ctxStop, wg, tmpDir, cid)

		assert.Equal(t, uint8(1), qNumber)
		assert.True(t, model.EqualsRows(&rowQ1, rows[1][0]))
		assert.True(t, ended)

		// Process the data to get to query phase
		// ctx, ctxStop, wg := coordinatorRun(t, init, log, tmpDir)
		// var rows []middleware.Envelope[*model.Row]
		// for i := 0; i < 4; i++ {
		// 	row, err := receiver.Next(ctx)
		// 	assert.NoError(t, err)
		// 	rows = append(rows, row)
		// 	row.Ack(false)
		// }

		// // Stop coordinator during query phase (Q1)
		// time.Sleep(1 * time.Second)
		// ctxStop()
		// wg.Wait()

		// // Verify query phase log exists
		// queryLogPath := path.Join(tmpDir, "logs", fmt.Sprintf("%d", cid), "querys")
		// _, err := os.Stat(queryLogPath)
		// assert.NoError(t, err, "Query log file should exist")

		// // Restart coordinator and verify recovery
		// log = logger.NewConsoleLogger("test", logger.Info)
		// ctx, ctxStop, wg = coordinatorRun(t, init, log, tmpDir)

		// // Verify that the coordinator recovers from query phase
		// // The coordinator should continue processing queries
		// time.Sleep(2 * time.Second)
		// ctxStop()
		// wg.Wait()

		// // Verify that the log was cleaned up after completion
		// _, err = os.Stat(queryLogPath)
		// assert.Error(t, err, "Query log file should be cleaned up after completion")
	})

}

func stopCoordinatorAndReadLog(t *testing.T, ctxStop context.CancelFunc, wg *sync.WaitGroup, tmpDir string, cid uint64) (string, uint64, []string, []byte, uint64) {
	time.Sleep(1 * time.Second)
	ctxStop()
	wg.Wait()

	//leo el log
	logFilePath := path.Join(tmpDir, "logs", fmt.Sprintf("%d", cid), "log")
	logFile, err := os.Open(logFilePath)
	assert.NoError(t, err)
	gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK, err := transaction_log.ReadLogFile(logFile)
	logFile.Close()
	assert.NoError(t, err)
	return gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK
}

func stopCoordinatorAndReadLogError(t *testing.T, ctxStop context.CancelFunc, wg *sync.WaitGroup, tmpDir string, cid uint64) {
	time.Sleep(1 * time.Second)
	ctxStop()
	wg.Wait()

	//verifico que exita la carpeta del cid
	logDir := path.Join(tmpDir, "logs", fmt.Sprintf("%d", cid))
	_, err := os.Stat(logDir)
	assert.NoError(t, err)

	//verifico que no exista el log
	logFilePath := path.Join(tmpDir, "logs", fmt.Sprintf("%d", cid), "log")
	_, err = os.Stat(logFilePath)
	assert.Error(t, err)
}

func sendCoordinator(t *testing.T, data string, sender middleware.Sender[*common.PackageFile], packageType common.TypePackage, cid uint64, id uint64) {
	packageFilename := &common.PackageFile{
		PackageType: packageType,
		Buf: model.FileChunk{
			Bytes: []byte(data),
		},
	}
	err := sender.Send(packageFilename, cid, id)
	assert.NoError(t, err)
}

func coordinatorRun(t *testing.T, init rabbitmq.AsyncDeployRabbitRes, log *logger.ConsoleLogger, tmpDir string, queriesPhaseIncluded bool) (context.Context, context.CancelFunc, *sync.WaitGroup) {
	cConnector, err := rabbitmq.ConnectorCustom(init.Config)
	assert.NoError(t, err)
	wg := sync.WaitGroup{}
	wg.Add(1)
	ctx, ctxStop := context.WithCancel(context.Background())
	go runCoordinator(ctx, log, cConnector, tmpDir, 1, 1, &wg, queriesPhaseIncluded)
	return ctx, ctxStop, &wg
}

func sendQ(t *testing.T, sender middleware.Sender[*model.Row], cid uint64, id uint64, row *model.Row) {
	err := sender.Send(row, cid, id)
	assert.NoError(t, err)
}

func sendQEOF(t *testing.T, sender middleware.Sender[*model.Row], cid uint64) {
	err := sender.SendEOF(cid)
	assert.NoError(t, err)
}

func stopCoordinatorAndReadLogQuery(t *testing.T, ctxStop context.CancelFunc, wg *sync.WaitGroup, tmpDir string, cid uint64) (uint8, map[uint8][]*model.Row, bool) {
	time.Sleep(1 * time.Second)
	ctxStop()
	wg.Wait()

	//leo el log
	logFilePath := path.Join(tmpDir, "logs", fmt.Sprintf("%d", cid), "querys")
	logFile, err := os.Open(logFilePath)
	assert.NoError(t, err)
	qNumber, rows, ended, err := transaction_log.ReadQueriesRows(logFile)
	logFile.Close()
	assert.NoError(t, err)
	return qNumber, rows, ended
}
