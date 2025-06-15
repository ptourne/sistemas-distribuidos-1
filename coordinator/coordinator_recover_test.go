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

func startCoordinator(t *testing.T, init rabbitmq.AsyncDeployRabbitRes) (middleware.Sender[*common.PackageFile], middleware.Receiver[*model.Row], context.Context, context.CancelFunc) {
	senderConnector, err := rabbitmq.ConnectorCustom(init.Config)
	assert.NoError(t, err)

	middlewareSenderLogger := logger.NewConsoleLogger("midd_send", logger.Debug)
	fileBytes := "file_bytes"
	middlewareChanByte := rabbitmq.NewMiddleware[*common.PackageFile](senderConnector, middlewareSenderLogger)
	sender, err := middlewareChanByte.WriteTo(fileBytes, []string{"file_bytes"}, "0", 1)
	assert.NoError(t, err)
	receiverConnector, err := rabbitmq.ConnectorCustom(init.Config)
	assert.NoError(t, err)
	middlewareReceiverLogger := logger.NewConsoleLogger("midd_rec", logger.Debug)

	middlewareChanRow := rabbitmq.NewMiddleware[*model.Row](receiverConnector, middlewareReceiverLogger)
	test_csv := "test_csv"
	prefetch := 100
	receiver, err := middlewareChanRow.ConsumeFrom(test_csv, test_csv, "0", prefetch, 1)
	assert.NoError(t, err)

	ctx, ctxStop := context.WithCancel(context.Background())

	return sender, receiver, ctx, ctxStop
}

func TestMapReducer(t *testing.T) {
	skipCI(t)
	provider := rabbitmq.NewContainerProvider(baseConfig)

	test1 := provider.AsyncDeployRabbit()
	test2 := provider.AsyncDeployRabbit()
	test3 := provider.AsyncDeployRabbit()
	test4 := provider.AsyncDeployRabbit()
	// test5 := provider.AsyncDeployRabbit()
	// test6 := provider.AsyncDeployRabbit()
	// test7 := provider.AsyncDeployRabbit()
	// test8 := provider.AsyncDeployRabbit()

	test1container := <-test1
	defer test1container.Container.Teardown()
	test2container := <-test2
	defer test2container.Container.Teardown()
	test3container := <-test3
	defer test3container.Container.Teardown()
	test4container := <-test4
	defer test4container.Container.Teardown()
	// test5container := <-test5
	// defer test5container.Container.Teardown()
	// test6container := <-test6
	// defer test6container.Container.Teardown()
	// test7container := <-test7
	// defer test7container.Container.Teardown()
	// test8container := <-test8
	// defer test8container.Container.Teardown()

	t.Run("CoordinatorLogEmpty", func(t *testing.T) {
		init := test1container
		assert.NoError(t, init.Err)
		sender, receiver, ctx, ctxStop := startCoordinator(t, init)
		defer sender.Close()
		defer receiver.Close()
		defer ctxStop()
		cid := uint64(1)

		log := logger.NewConsoleLogger("coordinator", logger.Info)
		cConnector, err := rabbitmq.ConnectorCustom(init.Config)
		assert.NoError(t, err)

		tmpDir := t.TempDir()
		wg := sync.WaitGroup{}
		wg.Add(1)
		go runCoordinator(ctx, log, cConnector, tmpDir, 1, 1, &wg, true)

		// 1. Enviar un string CSV
		fileName := "test_csv"
		packageFilename := &common.PackageFile{
			PackageType: common.FileName,
			Buf: model.FileChunk{
				Bytes: []byte(fileName),
			},
		}
		err = sender.Send(packageFilename, cid, 1)
		assert.NoError(t, err)

		csvData := "id,name\n1,Alice\n2,Bob\n"

		packageFile := &common.PackageFile{
			PackageType: common.FileData,
			Buf: model.FileChunk{
				Bytes: []byte(csvData),
			},
		}
		err = sender.Send(packageFile, cid, 1)
		assert.NoError(t, err)

		packageFileEOF := &common.PackageFile{
			PackageType: common.FinishFile,
			Buf: model.FileChunk{
				Bytes: []byte("EOF"),
			},
		}
		err = sender.Send(packageFileEOF, cid, 1)
		assert.NoError(t, err)

		packageEOF := &common.PackageFile{
			PackageType: common.AllFilesSent,
			Buf: model.FileChunk{
				Bytes: []byte("EOF"),
			},
		}
		err = sender.Send(packageEOF, cid, 1)
		assert.NoError(t, err)

		log = logger.NewConsoleLogger("test", logger.Info)
		// 2. Recibir los registros como model.Row
		var rows []middleware.Envelope[*model.Row]
		for i := 0; i < 4; i++ {
			if i < 2 {
				row, err := receiver.Next(ctx)
				log.Infof("Received row %s", row)
				assert.NoError(t, err)
				rows = append(rows, row)
				row.Ack(false)
			} else if i == 2 {
				log.Infof("Received prune")
				msg, err := receiver.Next(ctx)
				assert.NoError(t, err)
				assert.Equal(t, msg.Type(), middleware.Prune)
				msg.Ack(false)
			} else {
				log.Infof("Received EOF")
				msg, err := receiver.Next(ctx)
				assert.NoError(t, err)
				assert.Equal(t, msg.Type(), middleware.EOF)
				msg.Ack(false)
			}
		}

		r0 := rows[0].Msg()
		assert.Equal(t, r0.Strings["id"], "1")
		assert.Equal(t, r0.Strings["name"], "Alice")

		r1 := rows[1].Msg()
		assert.Equal(t, r1.Strings["id"], "2")
		assert.Equal(t, r1.Strings["name"], "Bob")

		time.Sleep(1 * time.Second)
		ctxStop()
		wg.Wait()
		logFilePath := path.Join(tmpDir, "logs", "1", "log")
		_, err = os.Stat(logFilePath)
		assert.Error(t, err, "Expected log file to not exist, but it does exist")
	})

	t.Run("CoordinatorLogMultipleClients", func(t *testing.T) {
		init := test2container
		assert.NoError(t, init.Err)
		sender, receiver, ctx, ctxStop := startCoordinator(t, init)
		defer sender.Close()
		defer receiver.Close()
		defer ctxStop()

		log := logger.NewConsoleLogger("coordinator", logger.Info)
		cConnector, err := rabbitmq.ConnectorCustom(init.Config)
		assert.NoError(t, err)

		tmpDir := t.TempDir()
		wg := sync.WaitGroup{}
		wg.Add(1)
		go runCoordinator(ctx, log, cConnector, tmpDir, 1, 1, &wg, true)

		// Send data from multiple clients
		clients := []uint64{1, 2, 3}
		for _, cid := range clients {
			fileName := "test_csv"
			packageFilename := &common.PackageFile{
				PackageType: common.FileName,
				Buf: model.FileChunk{
					Bytes: []byte(fileName),
				},
			}
			err = sender.Send(packageFilename, cid, 1)
			assert.NoError(t, err)

			csvData := "id,name\n1,Alice\n2,Bob\n"
			packageFile := &common.PackageFile{
				PackageType: common.FileData,
				Buf: model.FileChunk{
					Bytes: []byte(csvData),
				},
			}
			err = sender.Send(packageFile, cid, 1)
			assert.NoError(t, err)

			packageFileEOF := &common.PackageFile{
				PackageType: common.FinishFile,
				Buf: model.FileChunk{
					Bytes: []byte("EOF"),
				},
			}
			err = sender.Send(packageFileEOF, cid, 1)
			assert.NoError(t, err)

			packageEOF := &common.PackageFile{
				PackageType: common.AllFilesSent,
				Buf: model.FileChunk{
					Bytes: []byte("EOF"),
				},
			}
			err = sender.Send(packageEOF, cid, 1)
			assert.NoError(t, err)
		}

		var rowsC map[uint64][]middleware.Envelope[*model.Row] = map[uint64][]middleware.Envelope[*model.Row]{}
		// Verify all clients' data is processed
		log = logger.NewConsoleLogger("test", logger.Info)
		for i := 0; i < len(clients)*4; i++ {
			row, err := receiver.Next(ctx)
			assert.NoError(t, err)
			log.Infof("Received cid %d type %d", row.Cid(), row.Type())
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
			_, err = os.Stat(logFilePath)
			assert.Error(t, err, "Expected log file to not exist, but it does exist")
		}
	})

	t.Run("CoordinatorLogRecoveryNOlogs", func(t *testing.T) {
		init := test3container
		assert.NoError(t, init.Err)
		sender, receiver, ctx, ctxStop := startCoordinator(t, init)
		defer sender.Close()
		defer receiver.Close()
		defer ctxStop()

		log := logger.NewConsoleLogger("coordinator", logger.Info)
		cConnector, err := rabbitmq.ConnectorCustom(init.Config)
		assert.NoError(t, err)

		tmpDir := t.TempDir()
		wg := sync.WaitGroup{}
		wg.Add(1)
		go runCoordinator(ctx, log, cConnector, tmpDir, 1, 1, &wg, true)

		// Send initial data
		fileName := "test_csv"
		packageFilename := &common.PackageFile{
			PackageType: common.FileName,
			Buf: model.FileChunk{
				Bytes: []byte(fileName),
			},
		}
		err = sender.Send(packageFilename, 1, 1)
		assert.NoError(t, err)

		csvData := "id,name\n1,Alice\n2,Bob\n"
		packageFile := &common.PackageFile{
			PackageType: common.FileData,
			Buf: model.FileChunk{
				Bytes: []byte(csvData),
			},
		}
		err = sender.Send(packageFile, 1, 1)
		assert.NoError(t, err)

		// packageFileEOF := &common.PackageFile{
		// 	PackageType: common.FinishFile,
		// 	Buf: model.FileChunk{
		// 		Bytes: []byte("EOF"),
		// 	},
		// }
		// err = sender.Send(packageFileEOF, 1, 1)
		// assert.NoError(t, err)

		// Stop coordinator
		ctxStop()
		wg.Wait()

		//chequeo que no exista el log
		logFilePath := path.Join(tmpDir, "logs", "1", "log")
		_, err = os.Stat(logFilePath)
		assert.NoError(t, err, "Expected log file to exist, but it does not exist")

		// Start new coordinator with same tmpDir
		// ctx2, ctxStop2 := context.WithCancel(context.Background())
		// defer ctxStop2()

		// wg2 := sync.WaitGroup{}
		// wg2.Add(1)
		// go runCoordinator(ctx2, log, cConnector, tmpDir, 1, 1, &wg2)

		// // Verify all data is processed
		// var rows []middleware.Envelope[*model.Row] = []middleware.Envelope[*model.Row]{}
		// for i := 0; i < 2; i++ {
		// 	row, err := receiver.Next(ctx2)
		// 	assert.NoError(t, err)
		// 	assert.NotNil(t, row)
		// 	rows = append(rows, row)
		// }

		// r0 := rows[0].Msg()
		// assert.Equal(t, r0.Strings["id"], "1")
		// assert.Equal(t, r0.Strings["name"], "Alice")

		// r1 := rows[1].Msg()
		// assert.Equal(t, r1.Strings["id"], "2")
		// assert.Equal(t, r1.Strings["name"], "Bob")

		// ctxStop2()
		// wg2.Wait()
	})

	t.Run("CoordinatorLogCheckpoint", func(t *testing.T) {
		init := test1container
		assert.NoError(t, init.Err)
		sender, receiver, ctx, ctxStop := startCoordinator(t, init)
		defer sender.Close()
		defer receiver.Close()
		defer ctxStop()

		log := logger.NewConsoleLogger("coordinator", logger.Info)
		cConnector, err := rabbitmq.ConnectorCustom(init.Config)
		assert.NoError(t, err)

		tmpDir := t.TempDir()
		wg := sync.WaitGroup{}
		wg.Add(1)
		go runCoordinator(ctx, log, cConnector, tmpDir, 1, 1, &wg, true)

		// Send data before checkpoint
		fileName := "test_csv"
		packageFilename := &common.PackageFile{
			PackageType: common.FileName,
			Buf: model.FileChunk{
				Bytes: []byte(fileName),
			},
		}
		err = sender.Send(packageFilename, 1, 1)
		assert.NoError(t, err)

		csvData := "id,name\n1,Alice\n2,Bob\n"
		packageFile := &common.PackageFile{
			PackageType: common.FileData,
			Buf: model.FileChunk{
				Bytes: []byte(csvData),
			},
		}
		err = sender.Send(packageFile, 1, 1)
		assert.NoError(t, err)

		// Verify initial data
		for i := 0; i < 2; i++ {
			row, err := receiver.Next(ctx)
			assert.NoError(t, err)
			assert.NotNil(t, row)
		}

		// Stop coordinator to force checkpoint
		ctxStop()
		wg.Wait()

		// Start new coordinator
		ctx2, ctxStop2 := context.WithCancel(context.Background())
		defer ctxStop2()

		wg2 := sync.WaitGroup{}
		wg2.Add(1)
		go runCoordinator(ctx2, log, cConnector, tmpDir, 1, 1, &wg2, true)

		// Send more data after checkpoint
		packageFile2 := &common.PackageFile{
			PackageType: common.FileData,
			Buf: model.FileChunk{
				Bytes: []byte("3,Charlie\n"),
			},
		}
		err = sender.Send(packageFile2, 1, 1)
		assert.NoError(t, err)

		packageFileEOF := &common.PackageFile{
			PackageType: common.FinishFile,
			Buf: model.FileChunk{
				Bytes: []byte("EOF"),
			},
		}
		err = sender.Send(packageFileEOF, 1, 1)
		assert.NoError(t, err)

		// Verify all data is processed
		row, err := receiver.Next(ctx2)
		assert.NoError(t, err)
		assert.NotNil(t, row)
		assert.Equal(t, "3", row.Msg().Strings["id"])
		assert.Equal(t, "Charlie", row.Msg().Strings["name"])

		ctxStop2()
		wg2.Wait()
	})
}
