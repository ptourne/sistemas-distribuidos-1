package rabbitmq

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"context"
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
)

const RABBITMQ_EXPOSED_PORT_BASE = uint16(3000)

type ContainerProvider struct {
	config  configuration
	lock    sync.Mutex
	counter uint16
}

func NewContainerProvider(config configuration) *ContainerProvider {
	return &ContainerProvider{config: config, lock: sync.Mutex{}, counter: 0}
}

var baseConfig = NewConfiguration("guest", "guest", "localhost", RABBITMQ_EXPOSED_PORT_BASE)

// Container wraps the testcontainers-go container
type Container struct {
	container testcontainers.Container
}

// Teardown stops and removes the container
func (c *Container) Teardown() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	_ = c.container.Terminate(ctx) // Cleanup container
	cancel()
}

// StartContainer runs a container and returns a Container struct
func (cp *ContainerProvider) DeployRabbitmq() (*Container, configuration, error) {
	cp.lock.Lock()
	counter := cp.counter
	cp.counter++
	cp.lock.Unlock()
	fmt.Println("Starting RabbitMQ container", counter)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	config := cp.config
	config.Port = RABBITMQ_EXPOSED_PORT_BASE + counter
	req := testcontainers.ContainerRequest{
		Name:         fmt.Sprintf("rabbitmq-testing-%d", counter),
		Image:        "rabbitmq:4.1.0",
		ExposedPorts: []string{fmt.Sprintf("%d:5672", config.Port)},
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, config, fmt.Errorf("failed to start container: %w", err)
	}

	return &Container{
		container: container,
	}, config, nil
}

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

func newTimmer() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

type AsyncDeployRabbitRes struct {
	container *Container
	config    configuration
	err       error
}

func TestRabbitMQMiddleware(t *testing.T) {
	provider := NewContainerProvider(baseConfig)
	asyncDeployRabbit := func() chan AsyncDeployRabbitRes {
		ch := make(chan AsyncDeployRabbitRes)
		go func() {
			container, config, err := provider.DeployRabbitmq()
			ch <- AsyncDeployRabbitRes{container, config, err}
		}()
		return ch
	}
	test1 := asyncDeployRabbit()
	test2 := asyncDeployRabbit()
	test3 := asyncDeployRabbit()
	test4 := asyncDeployRabbit()
	test5 := asyncDeployRabbit()
	test1container := <-test1
	defer test1container.container.Teardown()
	test2container := <-test2
	defer test2container.container.Teardown()
	test3container := <-test3
	defer test3container.container.Teardown()
	test4container := <-test4
	defer test4container.container.Teardown()
	test5container := <-test5
	defer test5container.container.Teardown()

	t.Run("OneMessage", func(t *testing.T) {
		init := test1container
		assert.NoError(t, init.err)

		senderConnector, err := ConnectorCustom(init.config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"})
		assert.NoError(t, err)
		cid := "1"

		receiverConnector, err := ConnectorCustom(init.config)
		assert.NoError(t, err)
		receiverMiddleware := NewMiddleware[*Ball](receiverConnector)
		receiver, err := receiverMiddleware.ConsumeFrom("output", "receiver", 1, 1)
		assert.NoError(t, err)

		timer, cancel := newTimmer()
		received, _, err := receiver.Next(timer)
		cancel()
		if !assert.Error(t, err) {
			fmt.Println("Received message:", received.Msg())
			return
		}

		sentMsg := &Ball{1}
		err = sender.Send(sentMsg, cid)
		assert.NoError(t, err)

		timer, cancel = newTimmer()
		received, ok, err := receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, sentMsg, received.Msg())
		assert.NoError(t, received.Ack(true))

		timer, cancel = newTimmer()
		_, ok, err = receiver.Next(timer)
		cancel()
		assert.Error(t, err)
		assert.False(t, ok)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		timer, cancel = newTimmer()
		received, ok, err = receiver.Next(timer)
		cancel()
		log.Debugf("Received message, going to check")
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, cid, received.Cid())
		assert.Equal(t, middleware.EOF, received.Type())
	})

	t.Run("TwoMessages", func(t *testing.T) {
		init := test2container
		assert.NoError(t, init.err)

		senderConnector, err := ConnectorCustom(init.config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"})
		assert.NoError(t, err)
		cid := "1"

		receiverConnector, err := ConnectorCustom(init.config)
		assert.NoError(t, err)
		receiverMiddleware := NewMiddleware[*Ball](receiverConnector)
		receiver, err := receiverMiddleware.ConsumeFrom("output", "receiver", 1, 1)
		assert.NoError(t, err)

		timer, cancel := newTimmer()
		received, _, err := receiver.Next(timer)
		if !assert.Error(t, err) {
			fmt.Println("Received message:", received.Msg())
			return
		}
		cancel()

		sentMsg1 := &Ball{1}
		err = sender.Send(sentMsg1, cid)
		assert.NoError(t, err)

		timer, cancel = newTimmer()
		received, ok, err := receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, sentMsg1, received.Msg())
		assert.NoError(t, received.Ack(true))

		sentMsg2 := &Ball{2}
		err = sender.Send(sentMsg2, cid)
		assert.NoError(t, err)

		timer, cancel = newTimmer()
		received, ok, err = receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, sentMsg2, received.Msg())
		assert.NoError(t, received.Ack(true))

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		timer, cancel = newTimmer()
		received, ok, err = receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, cid, received.Cid())
		assert.Equal(t, middleware.EOF, received.Type())
	})

	t.Run("ReceiverArrivesLate", func(t *testing.T) {
		init := test3container
		assert.NoError(t, init.err)

		senderConnector, err := ConnectorCustom(init.config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"})
		assert.NoError(t, err)
		cid := "1"

		sentMsg := &Ball{1}
		err = sender.Send(sentMsg, cid)
		assert.NoError(t, err)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		receiverConnector, err := ConnectorCustom(init.config)
		assert.NoError(t, err)
		receiverMiddleware := NewMiddleware[*Ball](receiverConnector)
		receiver, err := receiverMiddleware.ConsumeFrom("output", "receiver", 1, 1)
		assert.NoError(t, err)

		timer, cancel := newTimmer()
		received, ok, err := receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, sentMsg, received.Msg())
		assert.NoError(t, received.Ack(true))

		timer, cancel = newTimmer()
		received, ok, err = receiver.Next(timer)
		cancel()
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, cid, received.Cid())
		assert.Equal(t, middleware.EOF, received.Type())
	})

	t.Run("TwoReceiversFinishCidAfterTimeout", func(t *testing.T) {
		init := test4container
		assert.NoError(t, init.err)

		senderConnector, err := ConnectorCustom(init.config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"})
		assert.NoError(t, err)
		cid := "1"

		receiver1Connector, err := ConnectorCustom(init.config)
		assert.NoError(t, err)
		receiver1Middleware := NewMiddleware[*Ball](receiver1Connector)
		receiver1, err := receiver1Middleware.ConsumeFrom("output", "receiver", 2, 1)
		assert.NoError(t, err)

		receiver2Connector, err := ConnectorCustom(init.config)
		assert.NoError(t, err)
		receiver2Middleware := NewMiddleware[*Ball](receiver2Connector)
		receiver2, err := receiver2Middleware.ConsumeFrom("output", "receiver", 2, 1)
		assert.NoError(t, err)

		sentMsg1 := &Ball{1}
		err = sender.Send(sentMsg1, cid)
		assert.NoError(t, err)

		handle1 := make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimmer()
			received, ok, err := receiver1.Next(ctx)
			cancel()
			handle1 <- NextAsyncRes{received, ok, err}
		}()
		handle2 := make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimmer()
			received, ok, err := receiver2.Next(ctx)
			cancel()
			handle2 <- NextAsyncRes{received, ok, err}
		}()

		select {
		case res := <-handle1:
			assert.NoError(t, res.err)
			assert.True(t, res.ok)
			assert.Equal(t, sentMsg1, res.received.Msg())
			assert.NoError(t, res.received.Ack(true))
			resOther := <-handle2
			assert.Error(t, resOther.err)
		case res := <-handle2:
			assert.NoError(t, res.err)
			assert.True(t, res.ok)
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
			ctx, cancel := newTimmer()
			received, ok, err := receiver1.Next(ctx)
			cancel()
			handle1 <- NextAsyncRes{received, ok, err}
		}()
		handle2 = make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimmer()
			received, ok, err := receiver2.Next(ctx)
			cancel()
			handle2 <- NextAsyncRes{received, ok, err}
		}()

		select {
		case res := <-handle1:
			assert.NoError(t, res.err)
			assert.True(t, res.ok)
			assert.Equal(t, sentMsg2, res.received.Msg())
			assert.NoError(t, res.received.Ack(true))
			resOther := <-handle2
			assert.Error(t, resOther.err)
		case res := <-handle2:
			assert.NoError(t, res.err)
			assert.True(t, res.ok)
			assert.Equal(t, sentMsg2, res.received.Msg())
			assert.NoError(t, res.received.Ack(true))
			resOther := <-handle1
			assert.Error(t, resOther.err)
		}

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		handle1 = make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimmer()
			received, ok, err := receiver1.Next(ctx)
			cancel()
			handle1 <- NextAsyncRes{received, ok, err}
		}()
		handle2 = make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimmer()
			received, ok, err := receiver2.Next(ctx)
			cancel()
			handle2 <- NextAsyncRes{received, ok, err}
		}()

		select {
		case res := <-handle1:
			assert.NoError(t, res.err)
			assert.False(t, res.ok)
			assert.Equal(t, cid, res.received.Cid())
			assert.Equal(t, middleware.EOF, res.received.Type())
			resOther := <-handle2
			assert.Error(t, resOther.err)
		case res := <-handle2:
			assert.NoError(t, res.err)
			assert.False(t, res.ok)
			assert.Equal(t, cid, res.received.Cid())
			assert.Equal(t, middleware.EOF, res.received.Type())
			resOther := <-handle1
			assert.Error(t, resOther.err)
		}
	})

	t.Run("TwoReceiversFinishCidAfterMsgOfDiffCid", func(t *testing.T) {
		init := test5container
		assert.NoError(t, init.err)
		senderConnector, err := ConnectorCustom(init.config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"})
		assert.NoError(t, err)
		cid1 := "1"
		cid2 := "2"

		receiver1Connector, err := ConnectorCustom(init.config)
		assert.NoError(t, err)
		receiver1Middleware := NewMiddleware[*Ball](receiver1Connector)
		receiver1, err := receiver1Middleware.ConsumeFrom("output", "receiver", 2, 1)
		assert.NoError(t, err)

		receiver2Connector, err := ConnectorCustom(init.config)
		assert.NoError(t, err)
		receiver2Middleware := NewMiddleware[*Ball](receiver2Connector)
		receiver2, err := receiver2Middleware.ConsumeFrom("output", "receiver", 2, 1)
		assert.NoError(t, err)

		sentMsg1 := &Ball{1}
		err = sender.Send(sentMsg1, cid1)
		assert.NoError(t, err)

		handle1 := make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimmer()
			received, ok, err := receiver1.Next(ctx)
			cancel()
			handle1 <- NextAsyncRes{received, ok, err}
		}()
		handle2 := make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimmer()
			received, ok, err := receiver2.Next(ctx)
			cancel()
			handle2 <- NextAsyncRes{received, ok, err}
		}()

		select {
		case res := <-handle1:
			assert.NoError(t, res.err)
			assert.True(t, res.ok)
			assert.Equal(t, sentMsg1, res.received.Msg())
			assert.NoError(t, res.received.Ack(true))
			resOther := <-handle2
			assert.Error(t, resOther.err)
		case res := <-handle2:
			assert.NoError(t, res.err)
			assert.True(t, res.ok)
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
			ctx, cancel := newTimmer()
			received, ok, err := receiver1.Next(ctx)
			cancel()
			handle1 <- NextAsyncRes{received, ok, err}
		}()

		handle2 = make(chan NextAsyncRes)
		go func() {
			ctx, cancel := newTimmer()
			received, ok, err := receiver2.Next(ctx)
			cancel()
			handle2 <- NextAsyncRes{received, ok, err}
		}()

		select {
		case res := <-handle1:
			assert.NoError(t, res.err)
			assert.True(t, res.ok)
			assert.Equal(t, sentMsg2, res.received.Msg())
			assert.NoError(t, res.received.Ack(true))
			resOther := <-handle2
			assert.Error(t, resOther.err)
		case res := <-handle2:
			assert.NoError(t, res.err)
			assert.True(t, res.ok)
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
				received, ok, err := receiver1.Next(ctx1)
				if exit {
					break
				}
				handle1 <- NextAsyncRes{received, ok, err}
				if ok {
					time.Sleep(100 * time.Millisecond)
				}
				if err != nil {
					log.Errorf("Error in receiver1: %v", err)
					break
				}
			}
			log.Infof("go routine1 finished")
		}()
		handle2 = make(chan NextAsyncRes)
		ctx2, cancel2 := context.WithCancel(context.Background())
		go func() {
			for {
				received, ok, err := receiver2.Next(ctx2)
				if exit {
					break
				}
				handle2 <- NextAsyncRes{received, ok, err}
				if ok {
					time.Sleep(100 * time.Millisecond)
				}
				if err != nil {
					log.Errorf("Error in receiver1: %v", err)
					break
				}
			}
			log.Infof("go routine2 finished")
		}()

		shouldBreak := false
		for !shouldBreak {
			select {
			case res := <-handle1:
				if !res.ok {
					assert.NoError(t, res.err)
					assert.False(t, res.ok)
					assert.Equal(t, cid1, res.received.Cid())
					assert.Equal(t, middleware.EOF, res.received.Type())
					log.Infof("Receiver cid1 finished")
					shouldBreak = true
				} else {
					assert.NoError(t, res.err)
					assert.Equal(t, cid2, res.received.Cid())
					assert.Equal(t, middleware.Normal, res.received.Type())
					assert.NoError(t, res.received.Ack(true))
				}
			case res := <-handle2:
				if !res.ok {
					assert.NoError(t, res.err)
					assert.False(t, res.ok)
					assert.Equal(t, cid1, res.received.Cid())
					assert.Equal(t, middleware.EOF, res.received.Type())
					log.Infof("Receiver cid1 finished")
					shouldBreak = true
				} else {
					assert.NoError(t, res.err)
					assert.Equal(t, cid2, res.received.Cid())
					assert.Equal(t, middleware.Normal, res.received.Type())
					assert.NoError(t, res.received.Ack(true))
				}
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
	ok       bool
	err      error
}
