package map_reducer

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
	"github.com/stretchr/testify/assert"
)

type num struct {
	val uint64
}

func (b num) Encode() ([]byte, error) {
	return codec.Uint64Encode(b.val)
}

func (b *num) Decode(data []byte) (*num, error) {
	r := bytes.NewReader(data)
	id, err := codec.Uint64Decode(r)
	if err != nil {
		return nil, err
	}
	return &num{val: id}, nil
}

type sum struct {
	val uint64
}

func (s sum) Encode() ([]byte, error) {
	return codec.Uint64Encode(s.val)
}

func (s *sum) Decode(data []byte) (*sum, error) {
	r := bytes.NewReader(data)
	id, err := codec.Uint64Decode(r)
	if err != nil {
		return nil, err
	}
	return &sum{val: id}, nil
}

type i = *num
type a = *sum
type r = *num

type sumMapReducer struct{}

func (s sumMapReducer) Map(in i) []a {
	log.Debugf("Mapping input %d", in.val)
	return []*sum{&sum{val: in.val}}
}

func (s sumMapReducer) Reduce(in []a) a {
	log.Debugf("Reducing %d inputs", len(in))
	var red uint64
	for _, v := range in {
		red += v.val
	}
	log.Debugf("Reduced sum is %d", red)
	return &sum{val: red}
}

func (s sumMapReducer) Output(in a) []r {
	log.Debugf("Outputting reduced sum %d", in.val)
	return []*num{&num{val: in.val}}
}

func newTimer() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 3*time.Second)
}

const RABBITMQ_EXPOSED_PORT_BASE = uint16(4000)

var baseConfig = rabbitmq.NewConfiguration("guest", "guest", "localhost", RABBITMQ_EXPOSED_PORT_BASE)

func TestMapReducer(t *testing.T) {
	provider := rabbitmq.NewContainerProvider(baseConfig)

	test1 := provider.AsyncDeployRabbit()

	test1container := <-test1
	defer test1container.Container.Teardown()

	t.Run("OneReducerOneMsg", func(t *testing.T) {
		init := test1container
		assert.NoError(t, init.Err)

		senderConnector, err := rabbitmq.ConnectorCustom(init.Config)
		assert.NoError(t, err)

		senderMiddleware := rabbitmq.NewMiddleware[i](senderConnector)
		sender, err := senderMiddleware.WriteTo("input", []string{"map_reducer"})
		assert.NoError(t, err)
		cid := "1"

		reducerConnector, err := rabbitmq.ConnectorCustom(init.Config)
		assert.NoError(t, err)
		mapReducer, err := NewMapReducer[i, a, r](
			reducerConnector,
			"map_reducer",
			"input",
			2,
			sumMapReducer{},
			[]string{"output"},
			[]string{},
			"1",
			1,
		)
		assert.NoError(t, err)

		handler := make(chan struct{})
		mapReducerCtx, stopMapReducer := context.WithCancel(context.Background())
		go func() {
			log.Infof("Starting map reducer: %v", mapReducer)
			err = mapReducer.Run(mapReducerCtx)
			assert.NoErrorf(t, err, "error running map reducer: %s", err)
			assert.EqualError(t, mapReducerCtx.Err(), context.Canceled.Error())
			log.Infof("map reducer finished")
			close(handler)
		}()

		receiverConnector, err := rabbitmq.ConnectorCustom(init.Config)
		assert.NoError(t, err)
		receiverMiddleware := rabbitmq.NewMiddleware[r](receiverConnector)
		receiver, err := receiverMiddleware.ConsumeFrom("map_reducer", "receiver", 0, 1)
		assert.NoError(t, err)

		err = sender.Send(&num{val: 1}, cid)
		assert.NoError(t, err)

		ctx, cancel := newTimer()
		_, ok, err := receiver.Next(ctx)
		cancel()
		assert.Error(t, err)
		assert.False(t, ok)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		ctx, cancel = newTimer()
		e, ok, err := receiver.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, middleware.Normal, e.Type())
		assert.Equal(t, uint64(1), e.Msg().val)
		e.Ack(true)

		ctx, cancel = newTimer()
		e, ok, err = receiver.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, middleware.Prune, e.Type())
		e.Ack(true)

		ctx, cancel = newTimer()
		e, ok, err = receiver.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.False(t, ok)
		assert.Equal(t, middleware.EOF, e.Type())
		e.Ack(true)

		time.Sleep(time.Second * 7) // we make sure the reducer doesn't crashes.

		stopMapReducer()
		<-handler
	})
}
