package rabbitmq

import (
	"bytes"
	"testing"
	"time"

	"context"
	"fmt"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/stretchr/testify/assert"
	"github.com/testcontainers/testcontainers-go"
)

const RABBITMQ_EXPOSED_PORT_BASE = uint16(3000)

type ContainerProvider struct {
	config  configuration
	counter uint16
}

func NewContainerProvider(config configuration) *ContainerProvider {
	return &ContainerProvider{config: config, counter: 0}
}

var baseConfig = NewConfiguration("guest", "guest", "localhost", RABBITMQ_EXPOSED_PORT_BASE)

// Container wraps the testcontainers-go container
type Container struct {
	container testcontainers.Container
	host      string
	port      string
}

// Teardown stops and removes the container
func (c *Container) Teardown() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	_ = c.container.Terminate(ctx) // Cleanup container
	cancel()
}

// StartContainer runs a container and returns a Container struct
func (cp *ContainerProvider) DeployRabbitmq() (*Container, configuration, error) {
	fmt.Println("Starting RabbitMQ container", cp.counter)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	config := cp.config
	config.Port = RABBITMQ_EXPOSED_PORT_BASE + cp.counter
	cp.counter++
	req := testcontainers.ContainerRequest{
		Name:         fmt.Sprintf("rabbitmq-testing-%d", cp.counter),
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

func newTimmer() *time.Timer {
	return time.NewTimer(time.Second * 5)
}

func TestRabbitMQMiddleware(t *testing.T) {
	provider := NewContainerProvider(baseConfig)

	t.Run("OneMessage", func(t *testing.T) {
		container, config, err := provider.DeployRabbitmq()
		assert.NoError(t, err)
		defer container.Teardown()

		senderConnector, err := ConnectorCustom(config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"})
		assert.NoError(t, err)

		receiverConnector, err := ConnectorCustom(config)
		assert.NoError(t, err)
		receiverMiddleware := NewMiddleware[*Ball](receiverConnector)
		receiver, err := receiverMiddleware.ConsumeFrom("output", "receiver")
		assert.NoError(t, err)

		received, ok, err := receiver.Next(newTimmer())
		if !assert.Error(t, err) {
			fmt.Println("Received message:", received.Msg())
			return
		}

		sentMsg := &Ball{1}
		err = sender.Send(sentMsg)
		assert.NoError(t, err)

		received, ok, err = receiver.Next(newTimmer())
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, sentMsg, received.Msg())
		assert.NoError(t, received.Ack(true))

		err = sender.Close()
		assert.NoError(t, err)

		received, ok, err = receiver.Next(newTimmer())
		assert.NoError(t, err)
		assert.False(t, ok)
	})

	t.Run("TwoMessages", func(t *testing.T) {
		container, config, err := provider.DeployRabbitmq()
		assert.NoError(t, err)
		defer container.Teardown()

		senderConnector, err := ConnectorCustom(config)
		assert.NoError(t, err)
		senderMiddleware := NewMiddleware[*Ball](senderConnector)
		sender, err := senderMiddleware.WriteTo("output", []string{"receiver"})
		assert.NoError(t, err)

		receiverConnector, err := ConnectorCustom(config)
		assert.NoError(t, err)
		receiverMiddleware := NewMiddleware[*Ball](receiverConnector)
		receiver, err := receiverMiddleware.ConsumeFrom("output", "receiver")
		assert.NoError(t, err)

		received, ok, err := receiver.Next(newTimmer())
		if !assert.Error(t, err) {
			fmt.Println("Received message:", received.Msg())
			return
		}

		sentMsg1 := &Ball{1}
		err = sender.Send(sentMsg1)
		assert.NoError(t, err)

		received, ok, err = receiver.Next(newTimmer())
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, sentMsg1, received.Msg())
		assert.NoError(t, received.Ack(true))

		sentMsg2 := &Ball{2}
		err = sender.Send(sentMsg2)
		assert.NoError(t, err)

		received, ok, err = receiver.Next(newTimmer())
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, sentMsg2, received.Msg())
		assert.NoError(t, received.Ack(true))

		err = sender.Close()
		assert.NoError(t, err)

		received, ok, err = receiver.Next(newTimmer())
		assert.NoError(t, err)
		assert.False(t, ok)
	})

}
