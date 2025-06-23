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
	return context.WithTimeout(context.Background(), 10*time.Second)
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
	test6 := provider.AsyncDeployRabbit()
	test7 := provider.AsyncDeployRabbit()
	test8 := provider.AsyncDeployRabbit()
	test9 := provider.AsyncDeployRabbit()
	test10 := provider.AsyncDeployRabbit()
	test11 := provider.AsyncDeployRabbit()
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
	test9container := <-test9
	defer test9container.Container.Teardown()
	test10container := <-test10
	defer test10container.Container.Teardown()
	test11container := <-test11
	defer test11container.Container.Teardown()

	t.Run("OneMessage", func(t *testing.T) {
		init := test1container
		assert.NoError(t, init.Err)
		senderId := "1"
		consumerCount := 1

		senderConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector, middlewareLogger)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"}, senderId, uint(consumerCount))
		assert.NoError(t, err)
		cid := uint64(1)

		receiverConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiverMiddleware := NewMiddleware[*Ball](receiverConnector, middlewareLogger)
		receiver, err := receiverMiddleware.ConsumeFrom("output", "receiver", "0", 1, uint(consumerCount))
		assert.NoError(t, err)

		timer, cancel := newTimer()
		received, err := receiver.Next(timer)
		cancel()
		if !assert.Error(t, err) {
			fmt.Println("Received message:", received.Msg())
			return
		}

		sentMsg := &Ball{1}
		err = sender.Send(sentMsg, cid, 1)
		assert.NoError(t, err)

		timer, cancel = newTimer()
		received, err = receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, sentMsg, received.Msg())
		assert.Equal(t, uint64(1), received.Id())
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
		assert.NoError(t, received.Ack(false))
	})

	t.Run("OneMessageID", func(t *testing.T) {
		init := test2container
		assert.NoError(t, init.Err)
		senderId := "1"
		consumerCount := 5
		senderConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector, middlewareLogger)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"}, senderId, uint(consumerCount))
		assert.NoError(t, err)
		cid := uint64(1)

		receiverConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiverMiddleware := NewMiddleware[*Ball](receiverConnector, middlewareLogger)
		receivers := []middleware.Receiver[*Ball]{}
		for i := 0; i < consumerCount; i++ {
			rk := fmt.Sprintf("%d", i)
			receiver, err := receiverMiddleware.ConsumeFrom("output", "receiver", rk, 1, uint(consumerCount))
			assert.NoError(t, err)
			assert.NotNil(t, receiver)
			receivers = append(receivers, receiver)
		}

		sentMsg := &Ball{1}
		err = sender.Send(sentMsg, cid, 1)
		assert.NoError(t, err)

		err = sender.Prune(cid)
		assert.NoError(t, err)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		cant_msg_received := 0
		cant_msg_not_received := 0

		for i := 0; i < consumerCount; i++ {
			log.Debugf("Waiting for message")
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
			e, err := receivers[i].Next(ctx)
			assert.NoError(t, err)
			log.Debugf("Received message type %s with i:%d", e.Type(), i)
			switch e.Type() {
			case middleware.Normal:
				cant_msg_received++
				assert.Equal(t, sentMsg, e.Msg())
				assert.Equal(t, uint64(1), e.Id())
				e.Ack(false)

				e, err = receivers[i].Next(ctx)
				assert.NoError(t, err)
				assert.Equal(t, middleware.Prune, e.Type())
				assert.Equal(t, uint64(0), e.Id())
				e.Ack(false)
				cancel()
			case middleware.Prune:
				cant_msg_not_received++
				assert.Equal(t, uint64(0), e.Id())
				e.Ack(false)
				cancel()
			default:
				assert.Fail(t, "should not be here")
				cancel()
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
		e, err := receivers[0].Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, middleware.EOF, e.Type())
		assert.Equal(t, uint64(0), e.Id())
		e.Ack(false)
		cancel()

		for i := 0; i < consumerCount; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err = receivers[i].Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		assert.Equal(t, 1, cant_msg_received)
		assert.Equal(t, consumerCount-1, cant_msg_not_received)

	})

	t.Run("OneMessageIDReverseEOF", func(t *testing.T) {
		init := test3container
		assert.NoError(t, init.Err)
		senderId := "1"
		consumerCount := 5
		senderConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector, middlewareLogger)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"}, senderId, uint(consumerCount))
		assert.NoError(t, err)
		cid := uint64(1)

		receiverConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiverMiddleware := NewMiddleware[*Ball](receiverConnector, middlewareLogger)
		receivers := []middleware.Receiver[*Ball]{}
		for i := 0; i < consumerCount; i++ {
			rk := fmt.Sprintf("%d", i)
			receiver, err := receiverMiddleware.ConsumeFrom("output", "receiver", rk, 1, uint(consumerCount))
			assert.NoError(t, err)
			assert.NotNil(t, receiver)
			receivers = append(receivers, receiver)
		}

		sentMsg := &Ball{1}
		err = sender.Send(sentMsg, cid, 0)
		assert.NoError(t, err)

		err = sender.Prune(cid)
		assert.NoError(t, err)

		for i := 0; i < consumerCount; i++ {
			err = sender.SendEOFONE(cid, fmt.Sprintf("%d", i))
			assert.NoError(t, err)
			time.Sleep(100 * time.Millisecond)
		}

		cant_msg_received := 0
		cant_msg_not_received := 0

		for i := consumerCount - 1; i >= 0; i-- {
			log.Debugf("Waiting for message")
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
			e, err := receivers[i].Next(ctx)
			assert.NoError(t, err)
			log.Debugf("Received message type %s with i:%d", e.Type(), i)
			switch e.Type() {
			case middleware.Normal:
				cant_msg_received++
				assert.Equal(t, sentMsg, e.Msg())
				e.Ack(true)

				e, err = receivers[i].Next(ctx)
				assert.NoError(t, err)
				assert.Equal(t, middleware.Prune, e.Type())
				e.Ack(true)
				cancel()
			case middleware.Prune:
				cant_msg_not_received++
				e.Ack(true)
				cancel()
			default:
				assert.Fail(t, "should not be here")
				cancel()
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
		e, err := receivers[0].Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, middleware.EOF, e.Type())
		e.Ack(true)
		cancel()

		for i := 0; i < consumerCount; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err = receivers[i].Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		assert.Equal(t, 1, cant_msg_received)
		assert.Equal(t, consumerCount-1, cant_msg_not_received)

	})

	t.Run("TwoMessages", func(t *testing.T) {
		init := test4container
		assert.NoError(t, init.Err)
		senderId := "1"
		consumerCount := 1

		senderConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector, middlewareLogger)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"}, senderId, uint(consumerCount))
		assert.NoError(t, err)
		cid := uint64(1)

		receiverConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiverMiddleware := NewMiddleware[*Ball](receiverConnector, middlewareLogger)
		receiver, err := receiverMiddleware.ConsumeFrom("output", "receiver", "0", 1, uint(consumerCount))
		assert.NoError(t, err)

		timer, cancel := newTimer()
		received, err := receiver.Next(timer)
		if !assert.Error(t, err) {
			fmt.Println("Received message:", received.Msg())
			return
		}
		cancel()

		sentMsg1 := &Ball{1}
		err = sender.Send(sentMsg1, cid, 1)
		assert.NoError(t, err)

		timer, cancel = newTimer()
		received, err = receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, sentMsg1, received.Msg())
		assert.Equal(t, uint64(1), received.Id())
		assert.NoError(t, received.Ack(true))

		sentMsg2 := &Ball{2}
		err = sender.Send(sentMsg2, cid, 2)
		assert.NoError(t, err)

		timer, cancel = newTimer()
		received, err = receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, sentMsg2, received.Msg())
		assert.Equal(t, uint64(2), received.Id())
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
		init := test5container
		assert.NoError(t, init.Err)
		senderId := "1"
		consumerCount := 1

		senderConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector, middlewareLogger)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"}, senderId, uint(consumerCount))
		assert.NoError(t, err)
		cid := uint64(1)

		sentMsg := &Ball{1}
		err = sender.Send(sentMsg, cid, 0)
		assert.NoError(t, err)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		receiverConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiverMiddleware := NewMiddleware[*Ball](receiverConnector, middlewareLogger)
		receiver, err := receiverMiddleware.ConsumeFrom("output", "receiver", "0", 1, uint(consumerCount))
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
		init := test6container
		assert.NoError(t, init.Err)
		senderId := "1"
		consumerCount := 2

		senderConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector, middlewareLogger)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"}, senderId, uint(consumerCount))
		assert.NoError(t, err)
		cid := uint64(1)

		receiver1Connector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiver1Middleware := NewMiddleware[*Ball](receiver1Connector, middlewareLogger)
		receiver1, err := receiver1Middleware.ConsumeFrom("output", "receiver", "0", 1, uint(consumerCount))
		assert.NoError(t, err)

		receiver2Connector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiver2Middleware := NewMiddleware[*Ball](receiver2Connector, middlewareLogger)
		receiver2, err := receiver2Middleware.ConsumeFrom("output", "receiver", "1", 1, uint(consumerCount))
		assert.NoError(t, err)

		sentMsg1 := &Ball{1}
		err = sender.Send(sentMsg1, cid, 0)
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
		err = sender.Send(sentMsg2, cid, 1)
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
		init := test7container
		assert.NoError(t, init.Err)
		senderId := "1"
		consumerCount := 2
		senderConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector, middlewareLogger)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"}, senderId, uint(consumerCount))
		assert.NoError(t, err)
		cid1 := uint64(1)
		cid2 := uint64(2)

		receiver1Connector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiver1Middleware := NewMiddleware[*Ball](receiver1Connector, middlewareLogger)
		receiver1, err := receiver1Middleware.ConsumeFrom("output", "receiver", "0", 1, uint(consumerCount))
		assert.NoError(t, err)

		receiver2Connector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiver2Middleware := NewMiddleware[*Ball](receiver2Connector, middlewareLogger)
		receiver2, err := receiver2Middleware.ConsumeFrom("output", "receiver", "1", 1, uint(consumerCount))
		assert.NoError(t, err)

		sentMsg1 := &Ball{1}
		err = sender.Send(sentMsg1, cid1, 0)
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
		err = sender.Send(sentMsg2, cid1, 1)
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
			err = sender.Send(ball, cid2, uint64(i+2))
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

	t.Run("TwoConsumerGroups", func(t *testing.T) {
		// This test wants to ensure that an prune.Ack() on one group doesn't reach the seccond one.
		init := test8container
		assert.NoError(t, init.Err)
		senderId := "1"
		consumerCount := 1 //ES LA CANTIDAD DE SHARDS

		senderConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		var middlewareLogger_sender *logger.ConsoleLogger = logger.NewConsoleLogger("sender", logger.Debug)
		senderMiddleware := NewMiddleware[*Ball](senderConnector, middlewareLogger_sender)
		sender, err := senderMiddleware.WriteTo("output", []string{"group_a", "group_b"}, senderId, uint(consumerCount))
		assert.NoError(t, err)
		cid := uint64(1)

		receiver1Connector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		var middlewareLogger_a_0 *logger.ConsoleLogger = logger.NewConsoleLogger("cons_a_0", logger.Debug)
		receiver1Middleware := NewMiddleware[*Ball](receiver1Connector, middlewareLogger_a_0)
		consumer_group_a, err := receiver1Middleware.ConsumeFrom("output", "group_a", "0", 1, uint(consumerCount))
		assert.NoError(t, err)

		receiver3Connector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		var middlewareLogger_b *logger.ConsoleLogger = logger.NewConsoleLogger("cons_b", logger.Debug)
		receiver3Middleware := NewMiddleware[*Ball](receiver3Connector, middlewareLogger_b)
		consumer_group_b, err := receiver3Middleware.ConsumeFrom("output", "group_b", "0", 1, uint(consumerCount))
		assert.NoError(t, err)

		sentMsg1 := &Ball{1}
		err = sender.Send(sentMsg1, cid, 0)
		assert.NoError(t, err)

		ctx, cancel := newTimer()
		res, err := consumer_group_a.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, sentMsg1, res.Msg())
		assert.Equal(t, middleware.Normal, res.Type())
		assert.NoError(t, res.Ack(true))

		ctx, cancel = newTimer()
		res, err = consumer_group_b.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, sentMsg1, res.Msg())
		assert.Equal(t, middleware.Normal, res.Type())
		assert.NoError(t, res.Ack(true))

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		ctx, cancel = newTimer()
		res, err = consumer_group_a.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, middleware.Prune, res.Type())
		assert.NoError(t, res.Ack(true))

		ctx, cancel = newTimer()
		res, err = consumer_group_b.Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, middleware.Prune, res.Type())
		assert.NoError(t, res.Ack(true))
		cancel()

		ctx, cancel = newTimer()
		res, err = consumer_group_b.Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, middleware.EOF, res.Type())
		assert.NoError(t, res.Ack(true))
		cancel()

		ctx, cancel = newTimer()
		res, err = consumer_group_a.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, middleware.EOF, res.Type())
		assert.NoError(t, res.Ack(true))

		ctx, cancel = newTimer()
		_, err = consumer_group_a.Next(ctx)
		cancel()
		assert.Error(t, err)
	})

	t.Run("TwoProducers", func(t *testing.T) {
		init := test9container
		assert.NoError(t, init.Err)

		consumerCount := 1

		sender1Connector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		var middlewareLoggerSender1 *logger.ConsoleLogger = logger.NewConsoleLogger("sender_1", logger.Debug)
		sender1Middleware := NewMiddleware[*Ball](sender1Connector, middlewareLoggerSender1)
		sender1, err := sender1Middleware.WriteTo("output", []string{"receiver"}, "0", uint(consumerCount))
		assert.NoError(t, err)

		sender2Connector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		var middlewareLoggerSender2 *logger.ConsoleLogger = logger.NewConsoleLogger("sender_2", logger.Debug)
		sender2Middleware := NewMiddleware[*Ball](sender2Connector, middlewareLoggerSender2)
		sender2, err := sender2Middleware.WriteTo("output", []string{"receiver"}, "1", uint(consumerCount))
		assert.NoError(t, err)
		cid := uint64(1)

		receiverConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		var middlewareLoggerReceiver *logger.ConsoleLogger = logger.NewConsoleLogger("receiver", logger.Info)
		receiverMiddleware := NewMiddleware[*Ball](receiverConnector, middlewareLoggerReceiver)
		receiver, err := receiverMiddleware.ConsumeFrom("output", "receiver", "0", 30, uint(consumerCount))
		assert.NoError(t, err)

		const countSender2 = uint64(100000)
		for i := range countSender2 {
			sentMsg := &Ball{i}
			err = sender2.Send(sentMsg, cid, uint64(i))
			assert.NoError(t, err)
		}
		ref := time.Now()
		emptyDuration := time.Since(ref)
		err = sender2.Prune(cid)
		fin := time.Since(ref) - emptyDuration
		assert.NoError(t, err)
		log.Infof("Prune duration: %s", fin)
		err = sender1.SendEOF(cid) //EL QUE TIENE ID = 0 MANDA EL EOF
		assert.NoError(t, err)

		for i := range countSender2 {
			timer, cancel := newTimer()
			received, err := receiver.Next(timer)
			cancel()
			assert.NoError(t, err)
			if assert.Equalf(t, middleware.Normal, received.Type(), "Received message of type %s instead of Ball{%d}", received.Type(), i) {
				assert.Equal(t, i, received.Msg().ID)
				assert.Equal(t, uint64(i), received.Id())
				assert.NoError(t, received.Ack(true))
			}
		}

		timer, cancel := newTimer()
		received, err := receiver.Next(timer)
		cancel()
		log.Debugf("received message in close notification: %+v", received)
		assert.NoError(t, err)
		assert.Equal(t, cid, received.Cid())
		assert.Equal(t, middleware.Prune, received.Type())
		assert.Equal(t, uint64(0), received.Id())
		assert.NoError(t, received.Ack(true))

		timer, cancel = newTimer()
		received, err = receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, cid, received.Cid())
		assert.Equal(t, middleware.EOF, received.Type())
		assert.Equal(t, uint64(0), received.Id())
		assert.NoError(t, received.Ack(true))
	})

	t.Run("OnePubOneRec", func(t *testing.T) {
		init := test10container
		assert.NoError(t, init.Err)
		senderId := "1"
		consumerCount := 1
		senderConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector, middlewareLogger)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"}, senderId, uint(consumerCount))
		assert.NoError(t, err)
		cid := uint64(1)

		countSender1 := uint64(200000)
		for i := range countSender1 {
			sentMsg := &Ball{i}
			err := sender.Send(sentMsg, cid, uint64(i))
			assert.NoError(t, err)
		}
		ref := time.Now()
		emptyDuration := time.Since(ref)
		err = sender.Prune(cid)
		fin := time.Since(ref) - emptyDuration
		assert.NoError(t, err)
		log.Infof("Prune duration: %s", fin)
		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		receiverConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiverMiddleware := NewMiddleware[*Ball](receiverConnector, middlewareLogger)
		receiver, err := receiverMiddleware.ConsumeFrom("output", "receiver", "0", 30, uint(consumerCount))
		assert.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
		defer cancel()
		handle1 := make(chan NextAsyncRes)
		go func() {
			for {
				received, err := receiver.Next(ctx)
				handle1 <- NextAsyncRes{received, err}
			}
		}()

		for i := range countSender1 {
			res := <-handle1
			assert.NoError(t, res.err)
			assert.Equal(t, middleware.Normal, res.received.Type(), "Received message of type %s instead NORMAL in round: %d", res.received.Type(), i)
			assert.NoError(t, res.received.Ack(false))
		}

		res := <-handle1
		assert.NoError(t, res.err)
		assert.Equal(t, middleware.Prune, res.received.Type())
		assert.NoError(t, res.received.Ack(false))

		res = <-handle1
		assert.NoError(t, res.err)
		assert.Equal(t, middleware.EOF, res.received.Type())
		assert.NoError(t, res.received.Ack(false))
	})

	t.Run("SenderIdVerification", func(t *testing.T) {
		init := test11container
		assert.NoError(t, init.Err)
		consumerCount := 1

		// Create three different senders with different IDs
		senders := make([]middleware.Sender[*Ball], 3)

		sender1Connector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		var middlewareLoggerSender1 *logger.ConsoleLogger = logger.NewConsoleLogger("sender_1", logger.Debug)
		sender1Middleware := NewMiddleware[*Ball](sender1Connector, middlewareLoggerSender1)
		senders[0], err = sender1Middleware.WriteTo("output", []string{"receiver"}, "1", uint(consumerCount))
		assert.NoError(t, err)

		sender2Connector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		var middlewareLoggerSender2 *logger.ConsoleLogger = logger.NewConsoleLogger("sender_2", logger.Debug)
		sender2Middleware := NewMiddleware[*Ball](sender2Connector, middlewareLoggerSender2)
		senders[1], err = sender2Middleware.WriteTo("output", []string{"receiver"}, "2", uint(consumerCount))
		assert.NoError(t, err)

		sender3Connector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		var middlewareLoggerSender3 *logger.ConsoleLogger = logger.NewConsoleLogger("sender_3", logger.Debug)
		sender3Middleware := NewMiddleware[*Ball](sender3Connector, middlewareLoggerSender3)
		senders[2], err = sender3Middleware.WriteTo("output", []string{"receiver"}, "3", uint(consumerCount))
		assert.NoError(t, err)

		receiverConnector, err := ConnectorCustom(init.Config)
		assert.NoError(t, err)
		var middlewareLoggerReceiver *logger.ConsoleLogger = logger.NewConsoleLogger("receiver", logger.Debug)
		receiverMiddleware := NewMiddleware[*Ball](receiverConnector, middlewareLoggerReceiver)
		receiver, err := receiverMiddleware.ConsumeFrom("output", "receiver", "0", 10, uint(consumerCount))
		assert.NoError(t, err)

		cid := uint64(1)

		// Send messages from each sender
		for i := range 3 {
			sentMsg := &Ball{uint64(i + 1)}
			err = senders[i].Send(sentMsg, cid, uint64(i+1))
			assert.NoError(t, err)
			err = senders[i].Prune(cid)
			assert.NoError(t, err)
		}

		// Collect received messages
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Receive the three messages
		for i := range 3 {
			received, err := receiver.Next(ctx)
			assert.NoError(t, err)
			assert.Equal(t, middleware.Normal, received.Type())
			assert.NotNil(t, received.Msg())
			assert.Equal(t, uint64(i+1), received.SenderId(), fmt.Sprintf("Message %d should have sender ID %d", i+1, i+1))
			assert.Equal(t, uint64(i+1), received.Id(), fmt.Sprintf("Message %d should have ID %d", i+1, i+1))
			assert.NoError(t, received.Ack(false))

		}

		// Send EOF from sender1 and verify sender ID in EOF/Prune messages
		err = senders[0].SendEOF(cid)
		assert.NoError(t, err)

		// Receive Prune message
		received, err := receiver.Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, middleware.Prune, received.Type())
		assert.Equal(t, uint64(0), received.SenderId(), "Prune message should have sender ID 0")
		assert.NoError(t, received.Ack(false))

		// Receive EOF message
		received, err = receiver.Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, middleware.EOF, received.Type())
		// EOF messages should have sender ID 0 since they are generated by the system
		assert.Equal(t, uint64(0), received.SenderId(), "EOF message should have sender ID 0")
		assert.NoError(t, received.Ack(false))

		log.Infof("SenderIdVerification test completed successfully")
	})

}

type NextAsyncRes struct {
	received middleware.Envelope[*Ball]
	err      error
}
