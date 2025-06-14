package main

import (
	"context"
	"os"
	"sync"
	"testing"

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
	// test2 := provider.AsyncDeployRabbit()
	// test3 := provider.AsyncDeployRabbit()
	// test4 := provider.AsyncDeployRabbit()
	// test5 := provider.AsyncDeployRabbit()
	// test6 := provider.AsyncDeployRabbit()
	// test7 := provider.AsyncDeployRabbit()
	// test8 := provider.AsyncDeployRabbit()

	test1container := <-test1
	defer test1container.Container.Teardown()
	// test2container := <-test2
	// defer test2container.Container.Teardown()
	// test3container := <-test3
	// defer test3container.Container.Teardown()
	// test4container := <-test4
	// defer test4container.Container.Teardown()
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
		go runCoordinator(ctx, log, cConnector, tmpDir, 1, 1, &wg)

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

		// 2. Recibir los registros como model.Row
		var rows []middleware.Envelope[*model.Row]
		for i := 0; i < 2; i++ {
			row, err := receiver.Next(ctx)
			log.Infof("Received row %s", row)
			assert.NoError(t, err)
			rows = append(rows, row)
		}

		r0 := rows[0].Msg()
		assert.Equal(t, r0.Strings["id"], "1")
		assert.Equal(t, r0.Strings["name"], "Alice")

		r1 := rows[1].Msg()
		assert.Equal(t, r1.Strings["id"], "2")
		assert.Equal(t, r1.Strings["name"], "Bob")

		ctxStop()
		wg.Wait()
	})

}
