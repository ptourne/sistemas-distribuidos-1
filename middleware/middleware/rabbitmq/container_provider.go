package rabbitmq

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/testcontainers/testcontainers-go"
)

type ContainerProvider struct {
	config  Configuration
	lock    sync.Mutex
	counter uint16
}

func NewContainerProvider(config Configuration) *ContainerProvider {
	return &ContainerProvider{config: config, lock: sync.Mutex{}, counter: 0}
}

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
func (cp *ContainerProvider) DeployRabbitmq() (*Container, Configuration, error) {
	cp.lock.Lock()
	counter := cp.counter
	cp.counter++
	cp.lock.Unlock()
	fmt.Println("Starting RabbitMQ container", counter)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	config := cp.config
	config.Port += counter
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

type AsyncDeployRabbitRes struct {
	Container *Container
	Config    Configuration
	Err       error
}

func (p *ContainerProvider) AsyncDeployRabbit() chan AsyncDeployRabbitRes {
	ch := make(chan AsyncDeployRabbitRes)
	go func() {
		container, config, err := p.DeployRabbitmq()
		ch <- AsyncDeployRabbitRes{container, config, err}
	}()
	return ch
}
