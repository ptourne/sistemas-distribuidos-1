package rabbitmq

import (
	"bytes"
	"os"
	"testing"
	"time"

	"context"
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/stretchr/testify/assert"
)

type Ball struct {
	ID uint64
}

func (b Ball) Encode() ([]byte, error) {
	return codec.Uint64Encode(b.ID)
}

func (b *Ball) Decode(data []byte) (*Ball, error) {
	r := bytes.NewReader(data)
	id, err := codec.Uint64Decode(r)
	if err != nil {
		return nil, err
	}
	return &Ball{ID: id}, nil
}

func newTimer() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

const RABBITMQ_EXPOSED_PORT_BASE = uint16(3000)

var baseConfig = NewConfiguration("guest", "guest", "localhost", RABBITMQ_EXPOSED_PORT_BASE)

var log *logger.ConsoleLogger = logger.NewConsoleLogger("test", logger.Debug)
var middlewareLogger *logger.ConsoleLogger = logger.NewConsoleLogger("middleware", logger.Debug)

func skipCI(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}
}

func TestRabbitMQMiddleware(t *testing.T) {
	skipCI(t)
	provider := NewContainerProvider(baseConfig)

	test1 := provider.AsyncDeployRabbit()
	test2 := provider.AsyncDeployRabbit()
	test3 := provider.AsyncDeployRabbit()
	test4 := provider.AsyncDeployRabbit()
	test5 := provider.AsyncDeployRabbit()
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

	t.Run("OneMessage", func(t *testing.T) {
		init := test1container
		assert.NoError(t, init.Err)

		senderConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector, middlewareLogger)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"})
		assert.NoError(t, err)
		cid := "1"

		receiverConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiverMiddleware := NewMiddleware[*Ball](receiverConnector, middlewareLogger)
		receiver, err := receiverMiddleware.ConsumeFrom("output", "receiver", 1, 1)
		assert.NoError(t, err)

		timer, cancel := newTimer()
		received, err := receiver.Next(timer)
		cancel()
		if !assert.Error(t, err) {
			fmt.Println("Received message:", received.Msg())
			return
		}

		sentMsg := &Ball{1}
		err = sender.Send(sentMsg, cid)
		assert.NoError(t, err)

		timer, cancel = newTimer()
		received, err = receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, sentMsg, received.Msg())
		assert.NoError(t, received.Ack(true))

		timer, cancel = newTimer()
		_, err = receiver.Next(timer)
		cancel()
		assert.Error(t, err)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		timer, cancel = newTimer()
		received, err = receiver.Next(timer)
		cancel()
		log.Debugf("received message in close notification: %+v", received)
		assert.NoError(t, err)
		assert.Equal(t, cid, received.Cid())
		assert.Equal(t, middleware.Prune, received.Type())
		assert.NoError(t, received.Ack(true))

		timer, cancel = newTimer()
		received, err = receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, cid, received.Cid())
		assert.Equal(t, middleware.EOF, received.Type())
		assert.NoError(t, received.Ack(true))
	})

	t.Run("TwoMessages", func(t *testing.T) {
		init := test2container
		assert.NoError(t, init.Err)

		senderConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector, middlewareLogger)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"})
		assert.NoError(t, err)
		cid := "1"

		receiverConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiverMiddleware := NewMiddleware[*Ball](receiverConnector, middlewareLogger)
		receiver, err := receiverMiddleware.ConsumeFrom("output", "receiver", 1, 1)
		assert.NoError(t, err)

		timer, cancel := newTimer()
		received, err := receiver.Next(timer)
		if !assert.Error(t, err) {
			fmt.Println("Received message:", received.Msg())
			return
		}
		cancel()

		sentMsg1 := &Ball{1}
		err = sender.Send(sentMsg1, cid)
		assert.NoError(t, err)

		timer, cancel = newTimer()
		received, err = receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, sentMsg1, received.Msg())
		assert.NoError(t, received.Ack(true))

		sentMsg2 := &Ball{2}
		err = sender.Send(sentMsg2, cid)
		assert.NoError(t, err)

		timer, cancel = newTimer()
		received, err = receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, sentMsg2, received.Msg())
		assert.NoError(t, received.Ack(true))

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		timer, cancel = newTimer()
		received, err = receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, cid, received.Cid())
		assert.Equal(t, middleware.Prune, received.Type())
		assert.NoError(t, received.Ack(true))

		timer, cancel = newTimer()
		received, err = receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, cid, received.Cid())
		assert.Equal(t, middleware.EOF, received.Type())
		assert.NoError(t, received.Ack(true))
	})

	t.Run("ReceiverArrivesLate", func(t *testing.T) {
		init := test3container
		assert.NoError(t, init.Err)

		senderConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector, middlewareLogger)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"})
		assert.NoError(t, err)
		cid := "1"

		sentMsg := &Ball{1}
		err = sender.Send(sentMsg, cid)
		assert.NoError(t, err)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		receiverConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiverMiddleware := NewMiddleware[*Ball](receiverConnector, middlewareLogger)
		receiver, err := receiverMiddleware.ConsumeFrom("output", "receiver", 1, 1)
		assert.NoError(t, err)

		timer, cancel := newTimer()
		received, err := receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, sentMsg, received.Msg())
		assert.NoError(t, received.Ack(true))

		timer, cancel = newTimer()
		received, err = receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, cid, received.Cid())
		assert.Equal(t, middleware.Prune, received.Type())
		assert.NoError(t, received.Ack(true))

		timer, cancel = newTimer()
		received, err = receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, cid, received.Cid())
		assert.Equal(t, middleware.EOF, received.Type())
		assert.NoError(t, received.Ack(true))
	})

	t.Run("TwoReceiversFinishCidAfterTimeout", func(t *testing.T) {
		init := test4container
		assert.NoError(t, init.Err)

		senderConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector, middlewareLogger)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"})
		assert.NoError(t, err)
		cid := "1"

		receiver1Connector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiver1Middleware := NewMiddleware[*Ball](receiver1Connector, middlewareLogger)
		receiver1, err := receiver1Middleware.ConsumeFrom("output", "receiver", 2, 1)
		assert.NoError(t, err)

		receiver2Connector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiver2Middleware := NewMiddleware[*Ball](receiver2Connector, middlewareLogger)
		receiver2, err := receiver2Middleware.ConsumeFrom("output", "receiver", 2, 1)
		assert.NoError(t, err)

		sentMsg1 := &Ball{1}
		err = sender.Send(sentMsg1, cid)
		assert.NoError(t, err)

		handle1 := make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimer()
			received, err := receiver1.Next(ctx)
			cancel()
			handle1 <- NextAsyncRes{received, err}
		}()
		handle2 := make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimer()
			received, err := receiver2.Next(ctx)
			cancel()
			handle2 <- NextAsyncRes{received, err}
		}()

		select {
		case res := <-handle1:
			assert.NoError(t, res.err)
			assert.Equal(t, sentMsg1, res.received.Msg())
			assert.NoError(t, res.received.Ack(true))
			resOther := <-handle2
			assert.Error(t, resOther.err)
		case res := <-handle2:
			assert.NoError(t, res.err)
			assert.Equal(t, sentMsg1, res.received.Msg())
			assert.NoError(t, res.received.Ack(true))
			resOther := <-handle1
			assert.Error(t, resOther.err)
		}

		sentMsg2 := &Ball{2}
		err = sender.Send(sentMsg2, cid)
		assert.NoError(t, err)

		handle1 = make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimer()
			received, err := receiver1.Next(ctx)
			cancel()
			handle1 <- NextAsyncRes{received, err}
		}()
		handle2 = make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimer()
			received, err := receiver2.Next(ctx)
			cancel()
			handle2 <- NextAsyncRes{received, err}
		}()

		select {
		case res := <-handle1:
			assert.NoError(t, res.err)
			assert.Equal(t, sentMsg2, res.received.Msg())
			assert.NoError(t, res.received.Ack(true))
			resOther := <-handle2
			assert.Error(t, resOther.err)
		case res := <-handle2:
			assert.NoError(t, res.err)
			assert.Equal(t, sentMsg2, res.received.Msg())
			assert.NoError(t, res.received.Ack(true))
			resOther := <-handle1
			assert.Error(t, resOther.err)
		}

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		handle1 = make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimer()
			received, err := receiver1.Next(ctx)
			cancel()
			handle1 <- NextAsyncRes{received, err}
		}()
		handle2 = make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimer()
			received, err := receiver2.Next(ctx)
			cancel()
			handle2 <- NextAsyncRes{received, err}
		}()

		res1 := <-handle1
		res2 := <-handle2

		assert.Equal(t, cid, res1.received.Cid())
		assert.Equal(t, middleware.Prune, res1.received.Type())
		assert.NoError(t, res1.received.Ack(true))

		assert.Equal(t, cid, res2.received.Cid())
		assert.Equal(t, middleware.Prune, res2.received.Type())
		assert.NoError(t, res2.received.Ack(true))

		handle1 = make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimer()
			received, err := receiver1.Next(ctx)
			cancel()
			handle1 <- NextAsyncRes{received, err}
		}()
		handle2 = make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimer()
			received, err := receiver2.Next(ctx)
			cancel()
			handle2 <- NextAsyncRes{received, err}
		}()

		res1 = <-handle1
		res2 = <-handle2

		if res1.err == nil {
			assert.Equal(t, cid, res1.received.Cid())
			assert.Equal(t, middleware.EOF, res1.received.Type())
			assert.NoError(t, res1.received.Ack(true))
			assert.Error(t, res2.err)
		} else if assert.NoError(t, res2.err) {
			assert.Equal(t, cid, res2.received.Cid())
			assert.Equal(t, middleware.EOF, res2.received.Type())
			assert.NoError(t, res2.received.Ack(true))
			assert.Error(t, res1.err)
		}
	})

	t.Run("TwoReceiversFinishCidAfterMsgOfDiffCid", func(t *testing.T) {
		init := test5container
		assert.NoError(t, init.Err)
		senderConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector, middlewareLogger)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"})
		assert.NoError(t, err)
		cid1 := "1"
		cid2 := "2"

		receiver1Connector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiver1Middleware := NewMiddleware[*Ball](receiver1Connector, middlewareLogger)
		receiver1, err := receiver1Middleware.ConsumeFrom("output", "receiver", 2, 1)
		assert.NoError(t, err)

		receiver2Connector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiver2Middleware := NewMiddleware[*Ball](receiver2Connector, middlewareLogger)
		receiver2, err := receiver2Middleware.ConsumeFrom("output", "receiver", 2, 1)
		assert.NoError(t, err)

		sentMsg1 := &Ball{1}
		err = sender.Send(sentMsg1, cid1)
		assert.NoError(t, err)

		handle1 := make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimer()
			received, err := receiver1.Next(ctx)
			cancel()
			handle1 <- NextAsyncRes{received, err}
		}()
		handle2 := make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimer()
			received, err := receiver2.Next(ctx)
			cancel()
			handle2 <- NextAsyncRes{received, err}
		}()

		select {
		case res := <-handle1:
			assert.NoError(t, res.err)
			assert.Equal(t, sentMsg1, res.received.Msg())
			assert.NoError(t, res.received.Ack(true))
			resOther := <-handle2
			assert.Error(t, resOther.err)
		case res := <-handle2:
			assert.NoError(t, res.err)
			assert.Equal(t, sentMsg1, res.received.Msg())
			assert.NoError(t, res.received.Ack(true))
			resOther := <-handle1
			assert.Error(t, resOther.err)
		}

		sentMsg2 := &Ball{2}
		err = sender.Send(sentMsg2, cid1)
		assert.NoError(t, err)

		handle1 = make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimer()
			received, err := receiver1.Next(ctx)
			cancel()
			handle1 <- NextAsyncRes{received, err}
		}()

		handle2 = make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimer()
			received, err := receiver2.Next(ctx)
			cancel()
			handle2 <- NextAsyncRes{received, err}
		}()

		select {
		case res := <-handle1:
			assert.NoError(t, res.err)
			assert.Equal(t, sentMsg2, res.received.Msg())
			assert.NoError(t, res.received.Ack(true))
			resOther := <-handle2
			assert.Error(t, resOther.err)
		case res := <-handle2:
			assert.NoError(t, res.err)
			assert.Equal(t, sentMsg2, res.received.Msg())
			assert.NoError(t, res.received.Ack(true))
			resOther := <-handle1
			assert.Error(t, resOther.err)
		}

		err = sender.SendEOF(cid1)
		assert.NoError(t, err)

		for i := range 20 {
			ball := &Ball{uint64(i + 3)}
			err = sender.Send(ball, cid2)
			assert.NoError(t, err)
		}

		exit := false
		handle1 = make(chan NextAsyncRes)
		ctx1, cancel1 := context.WithCancel(context.Background())
		go func() {
			for {
				received, err := receiver1.Next(ctx1)
				if exit {
					break
				}
				handle1 <- NextAsyncRes{received, err}
				if received.Type() == middleware.Normal {
					time.Sleep(100 * time.Millisecond)
				}
			}
			log.Infof("go routine1 finished")
		}()
		handle2 = make(chan NextAsyncRes)
		ctx2, cancel2 := context.WithCancel(context.Background())
		go func() {
			for {
				received, err := receiver2.Next(ctx2)
				if exit {
					break
				}
				handle2 <- NextAsyncRes{received, err}
				if received.Type() == middleware.Normal {
					time.Sleep(100 * time.Millisecond)
				}
			}
			log.Infof("go routine2 finished")
		}()

		shouldContinue := false
		numberOfPrunes := 0
		expectsError := false
		gotError := false
		receivedPrunes := make(map[uint]bool)
		var pedack middleware.Envelope[*Ball] = nil
		a := func(id uint, res NextAsyncRes) bool {
			log.Infof("Receiver %d received message", id)
			if expectsError {
				if err != nil {
					log.Infof("Got an expected error")
					assert.NoError(t, pedack.Ack(false))
					gotError = true
					return true
				}
			} else {
				assert.NoError(t, err, "Unexpected error")
			}
			switch res.received.Type() {
			case middleware.Prune:
				log.Infof("Received prune")
				assert.Falsef(t, receivedPrunes[id], "More than one prune received by %d", id)
				receivedPrunes[id] = true
				numberOfPrunes++
				switch numberOfPrunes {
				case 1:
					log.Infof("Received first prune")
					pedack = res.received
					expectsError = true
				case 2:
					log.Infof("Received second prune")
					assert.True(t, gotError)
				default:
					t.Fatalf("Should not get more than two prunes")
				}
			case middleware.EOF:
				log.Infof("Received EOF")
				assert.True(t, gotError)
				assert.Equal(t, 2, numberOfPrunes)
				return false
			}
			return true
		}
		for shouldContinue {
			select {
			case res := <-handle1:
				shouldContinue = a(1, res)
			case res := <-handle2:
				shouldContinue = a(2, res)
			}
		}
		log.Debugf("Exiting loop")
		exit = true
		cancel1()
		cancel2()
	})

}

type NextAsyncRes struct {
	received middleware.Envelope[*Ball]
	err      error
}
