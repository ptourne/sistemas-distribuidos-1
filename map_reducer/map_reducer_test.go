package map_reducer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq/transaction_log"
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

var log *logger.ConsoleLogger = logger.NewConsoleLogger("test", logger.Debug)

func (s sumMapReducer) Map(in i) []a {
	log.Debugf("Mapping input %d", in.val)
	return []*sum{{val: in.val}}
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
	return []*num{{val: in.val}}
}

type sumMapReducer2 struct{}

func (s sumMapReducer2) Map(in i) []a {
	log.Debugf("Mapping input %d", in.val)
	return []*sum{{val: in.val}}
}

func (s sumMapReducer2) Reduce(in []a) a {
	log.Debugf("Reducing %d inputs", len(in))
	var red uint64
	for _, v := range in {
		red += v.val
	}
	log.Debugf("Reduced sum is %d", red)
	return &sum{val: red}
}

func (s sumMapReducer2) Output(in a) []r {
	log.Debugf("Outputting reduced sum %d", in.val)
	return []*num{{val: in.val}, {val: in.val + 1}, {val: in.val + 2}}
}

const RABBITMQ_EXPOSED_PORT_BASE = uint16(4000)

var baseConfig = rabbitmq.NewConfiguration("guest", "guest", "localhost", RABBITMQ_EXPOSED_PORT_BASE)

func setupReducerPipelineRK(t *testing.T,
	init rabbitmq.AsyncDeployRabbitRes,
	shardCount uint,
	shardCountOutput uint,
	mapReduce MapReduce[i, a, r],
	maxLogSize uint64,
) (
	sender middleware.Sender[i],
	receivers []middleware.Receiver[r],
	stopReducers context.CancelFunc,
	reducerHandles []chan struct{},
	err error,
) {
	senderConnector, err := rabbitmq.ConnectorCustom(init.Config)
	assert.NoError(t, err)

	middlewareSenderLogger := logger.NewConsoleLogger("midd_send", logger.Debug)
	senderMiddleware := rabbitmq.NewMiddleware[i](senderConnector, middlewareSenderLogger)
	sender, err = senderMiddleware.WriteTo("input", []string{"map_reducer"}, "1", shardCount)
	assert.NoError(t, err)

	reducerHandles = make([]chan struct{}, shardCount)
	mapReducerCtx, stopMapReducer := context.WithCancel(context.Background())
	for i := 0; i < int(shardCount); i++ {
		dirPath := t.TempDir()
		handler := newMapReducer(mapReducerCtx, t, init, i, mapReduce, shardCount, shardCountOutput, dirPath, maxLogSize)
		reducerHandles[i] = handler
	}

	receiverConnector, err := rabbitmq.ConnectorCustom(init.Config)
	assert.NoError(t, err)
	middlewareReceiverLogger := logger.NewConsoleLogger("midd_rec", logger.Debug)
	receivers = []middleware.Receiver[r]{}
	receiverMiddleware := rabbitmq.NewMiddleware[r](receiverConnector, middlewareReceiverLogger)
	for i := 0; i < int(shardCountOutput); i++ {
		rk := fmt.Sprintf("%d", i)
		receiver, err := receiverMiddleware.ConsumeFrom("map_reducer", "receiver", rk, 1, shardCountOutput)
		assert.NoError(t, err)
		assert.NotNil(t, receiver)
		receivers = append(receivers, receiver)
	}
	return sender, receivers, stopMapReducer, reducerHandles, err
}

func newMapReducer(mapReducerCtx context.Context, t *testing.T, init rabbitmq.AsyncDeployRabbitRes, i int, mapReduce MapReduce[i, a, r], shardCount uint, shardCountOutput uint, dirPath string, maxLogSize uint64) chan struct{} {
	reducerConnector, err := rabbitmq.ConnectorCustom(init.Config)
	assert.NoError(t, err)
	id := fmt.Sprintf("%d", i)
	mapReducer, err := NewMapReducer(
		reducerConnector,
		"map_reducer",
		"input",
		2,
		mapReduce,
		[]string{"receiver"},
		id,
		shardCount,
		shardCountOutput,
		dirPath,
		maxLogSize,
	)
	assert.NoError(t, err)
	assert.NotNil(t, mapReducer)
	assert.NotNil(t, mapReducer.partialReducer.Receiver)

	handler := make(chan struct{})
	go func() {
		log.Infof("Starting map reducer: %v", mapReducer)
		err = mapReducer.Run(mapReducerCtx)
		assert.NoErrorf(t, err, "error running map reducer %d: %s", id, err)
		log.Infof("map reducer finished")
		close(handler)
	}()
	return handler
}

func skipCI(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}
}

const TIME_TO_WAIT_FOR_CRASH = 2 // we make sure the reducer doesn't crashes.

func TestMapReducer(t *testing.T) {
	skipCI(t)
	provider := rabbitmq.NewContainerProvider(baseConfig)

	test0 := provider.AsyncDeployRabbit()
	test1 := provider.AsyncDeployRabbit()
	test2 := provider.AsyncDeployRabbit()
	test3 := provider.AsyncDeployRabbit()
	test4 := provider.AsyncDeployRabbit()
	test5 := provider.AsyncDeployRabbit()
	test6 := provider.AsyncDeployRabbit()
	test7 := provider.AsyncDeployRabbit()
	test8 := provider.AsyncDeployRabbit()
	test9 := provider.AsyncDeployRabbit()

	test0container := <-test0
	defer test0container.Container.Teardown()
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

	t.Run("1Reducer0Msg", func(t *testing.T) {
		init := test0container
		assert.NoError(t, init.Err)

		shardCount := uint(1)
		shardCountOutput := uint(1)
		_, receivers, stopMapReducer, handlers, err := setupReducerPipelineRK(t, init, shardCount, shardCountOutput, sumMapReducer{}, 10000000000000000000)
		assert.NoError(t, err)

		for i := 0; i < int(shardCountOutput); i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			wtf, err := receivers[i].Next(ctx)
			cancel()
			if !assert.Error(t, err) {
				log.Debugf("WTF %+v", wtf)
				log.Debugf("WTF type %+v", wtf.Type())
			}
		}

		time.Sleep(time.Second * TIME_TO_WAIT_FOR_CRASH) // we make sure the reducer doesn't crashes.

		stopMapReducer()
		for _, handler := range handlers {
			<-handler
		}
		log.Infof("All handlers finisheded YESSS")
	})

	t.Run("1Reducer1Cid1Msg", func(t *testing.T) {
		init := test1container
		assert.NoError(t, init.Err)

		cid := uint64(1)
		shardCount := uint(1)
		shardCountOutput := uint(1)
		sender, receivers, stopMapReducer, handlers, err := setupReducerPipelineRK(t, init, shardCount, shardCountOutput, sumMapReducer{}, 10000000000000000000)
		assert.NoError(t, err)

		err = sender.Send(&num{val: 1}, cid, 0)
		assert.NoError(t, err)

		for i := range int(shardCountOutput) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_, err = receivers[i].Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		err = sender.Prune(cid)
		assert.NoError(t, err)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		cant_msg_received := []int{}
		cant_msg_not_received := uint(0)

		for i := range int(shardCountOutput) {
			log.Debugf("Waiting for message")
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
			e, err := receivers[i].Next(ctx)
			assert.NoError(t, err)
			log.Debugf("Received message type %s", e.Type())
			switch e.Type() {
			case middleware.Normal:
				cant_msg_received = append(cant_msg_received, i)
				assert.Equal(t, uint64(1), e.Msg().val)
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
		e.Ack(false)
		cancel()

		for i := 0; i < int(shardCountOutput); i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err := receivers[i].Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		assert.Equal(t, []int{0}, cant_msg_received)
		assert.Equal(t, shardCountOutput-1, cant_msg_not_received)

		time.Sleep(time.Second * TIME_TO_WAIT_FOR_CRASH) // we make sure the reducer doesn't crashes.

		stopMapReducer()
		for _, handler := range handlers {
			<-handler
		}
		log.Infof("All handlers finisheded YESSS")
	})

	t.Run("5Reducer1Cid1Msg1Receiver", func(t *testing.T) {
		init := test2container
		assert.NoError(t, init.Err)
		shardCount := uint(5)
		shardCountOutput := uint(1)

		cid := uint64(1)
		sender, receivers, stopMapReducer, handlers, err := setupReducerPipelineRK(t, init, shardCount, shardCountOutput, sumMapReducer{}, 10000000000000000000)
		assert.NoError(t, err)

		err = sender.Send(&num{val: 1}, cid, 0)
		assert.NoError(t, err)

		for i := 0; i < int(shardCountOutput); i++ {
			log.Debugf("timeout waiting for message in receiver %d", i)
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_, err = receivers[i].Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		err = sender.Prune(cid)
		assert.NoError(t, err)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		cant_msg_received := 0
		cant_msg_not_received := 0

		for i := 0; i < int(shardCountOutput); i++ {
			log.Debugf("Waiting for message")
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
			e, err := receivers[i].Next(ctx)
			assert.NoError(t, err)
			log.Debugf("Received message type %s", e.Type())
			switch e.Type() {
			case middleware.Normal:
				cant_msg_received++
				assert.Equal(t, uint64(1), e.Msg().val)
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
		e.Ack(false)
		cancel()

		for i := 0; i < int(shardCountOutput); i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err = receivers[i].Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		assert.Equal(t, 1, cant_msg_received)
		assert.Equal(t, 0, cant_msg_not_received)

		time.Sleep(time.Second * TIME_TO_WAIT_FOR_CRASH) // we make sure the reducer doesn't crashes.

		stopMapReducer()
		for _, handler := range handlers {
			<-handler
		}
		log.Infof("All handlers finisheded YESSS")
	})

	t.Run("10Reducer1Cid10Msg", func(t *testing.T) {
		init := test3container
		assert.NoError(t, init.Err)
		shardCount := uint(10)
		shardCountOutput := uint(1)

		cid := uint64(1)
		sender, receivers, stopMapReducer, handlers, err := setupReducerPipelineRK(t, init, shardCount, shardCountOutput, sumMapReducer{}, 10000000000000000000)
		assert.NoError(t, err)

		expected := uint64(0)
		for i := range uint64(10) {
			err = sender.Send(&num{val: i}, cid, i)
			assert.NoError(t, err)
			expected += i
		}

		for i := 0; i < int(shardCountOutput); i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_, err = receivers[i].Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		err = sender.Prune(cid)
		assert.NoError(t, err)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		cant_msg_received := 0
		cant_msg_not_received := 0

		for i := 0; i < int(shardCountOutput); i++ {
			log.Debugf("Waiting for message")
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
			e, err := receivers[i].Next(ctx)
			assert.NoError(t, err)
			log.Debugf("Received message type %s", e.Type())
			switch e.Type() {
			case middleware.Normal:
				cant_msg_received++
				assert.Equal(t, expected, e.Msg().val)
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
		e.Ack(false)
		cancel()

		for i := 0; i < int(shardCountOutput); i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err = receivers[i].Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		assert.Equal(t, 1, cant_msg_received)
		assert.Equal(t, 0, cant_msg_not_received)

		time.Sleep(time.Second * TIME_TO_WAIT_FOR_CRASH) // we make sure the reducer doesn't crashes.

		stopMapReducer()
		for _, handler := range handlers {
			<-handler
		}
		log.Infof("All handlers finisheded YESSS")
	})

	t.Run("10Reducer1Cid1000Msg", func(t *testing.T) {
		init := test4container
		assert.NoError(t, init.Err)
		shardCount := uint(10)
		shardCountOutput := uint(1)

		cid := uint64(1)
		sender, receivers, stopMapReducer, handlers, err := setupReducerPipelineRK(t, init, shardCount, shardCountOutput, sumMapReducer{}, 10000000000000000000)
		assert.NoError(t, err)

		expected := uint64(0)
		for i := range uint64(1000) {
			err = sender.Send(&num{val: i}, cid, i)
			assert.NoError(t, err)
			expected += i
		}

		for i := 0; i < int(shardCountOutput); i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_, err = receivers[i].Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		err = sender.Prune(cid)
		assert.NoError(t, err)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		cant_msg_received := 0
		cant_msg_not_received := 0

		for i := 0; i < int(shardCountOutput); i++ {
			log.Debugf("Waiting for message")
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
			e, err := receivers[i].Next(ctx)
			assert.NoError(t, err)
			log.Debugf("Received message type %s", e.Type())
			switch e.Type() {
			case middleware.Normal:
				cant_msg_received++
				assert.Equal(t, expected, e.Msg().val)
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
		e.Ack(false)
		cancel()

		for i := 0; i < int(shardCountOutput); i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err = receivers[i].Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		assert.Equal(t, 1, cant_msg_received)
		assert.Equal(t, 0, cant_msg_not_received)

		time.Sleep(time.Second * TIME_TO_WAIT_FOR_CRASH) // we make sure the reducer doesn't crashes.

		stopMapReducer()
		for _, handler := range handlers {
			<-handler
		}
		log.Infof("All handlers finisheded YESSS")
	})

	t.Run("10Reducer1Cid10Msg2ReceiversMultipleOutputs", func(t *testing.T) {
		init := test5container
		assert.NoError(t, init.Err)
		shardCount := uint(10)
		shardCountOutput := uint(2)

		cid := uint64(1)
		sender, receivers, stopMapReducer, handlers, err := setupReducerPipelineRK(t, init, shardCount, shardCountOutput, sumMapReducer2{}, 10000000000000000000)
		assert.NoError(t, err)

		expected := uint64(0)
		for i := range uint64(10) {
			err = sender.SendRK(&num{val: i}, cid, i, fmt.Sprintf("%d", i%uint64(shardCountOutput)))
			assert.NoError(t, err)
			expected += i
		}

		for i := 0; i < int(shardCountOutput); i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_, err = receivers[i].Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		err = sender.Prune(cid)
		assert.NoError(t, err)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		cant_msg_received := 0
		cant_msg_not_received := 0

		for i := 0; i < int(shardCountOutput); i++ {
			log.Debugf("Waiting for message")
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
			e, err := receivers[i].Next(ctx)
			assert.NoError(t, err)
			log.Debugf("Received message type %s", e.Type())
			switch e.Type() {
			case middleware.Normal:
				cant_msg_received++
				if i == 0 {
					assert.Equal(t, expected, e.Msg().val)
					assert.Equal(t, uint64(0), e.Id())
				} else {
					assert.Equal(t, expected+1, e.Msg().val)
					assert.Equal(t, uint64(1), e.Id())
				}
				e.Ack(true)
				if i == 0 {
					e, err = receivers[i].Next(ctx)
					assert.NoError(t, err)
					assert.Equal(t, middleware.Normal, e.Type())
					assert.Equal(t, expected+2, e.Msg().val)
					assert.Equal(t, uint64(2), e.Id())
					e.Ack(true)
				}

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
		e.Ack(false)
		cancel()

		for i := 0; i < int(shardCountOutput); i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err = receivers[i].Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		assert.Equal(t, 2, cant_msg_received)
		assert.Equal(t, 0, cant_msg_not_received)

		time.Sleep(time.Second * TIME_TO_WAIT_FOR_CRASH) // we make sure the reducer doesn't crashes.

		stopMapReducer()
		for _, handler := range handlers {
			<-handler
		}
		log.Infof("All handlers finisheded YESSS")
	})

	t.Run("10Reducer1Cid1000Msg2Receivers", func(t *testing.T) {
		init := test6container
		assert.NoError(t, init.Err)
		shardCount := uint(10)
		shardCountOutput := uint(2)

		cid := uint64(1)
		sender, receivers, stopMapReducer, handlers, err := setupReducerPipelineRK(t, init, shardCount, shardCountOutput, sumMapReducer{}, 10000000000000000000)
		assert.NoError(t, err)

		expected := uint64(0)
		for i := range uint64(1000) {
			err = sender.Send(&num{val: i}, cid, i)
			assert.NoError(t, err)
			expected += i
		}

		for i := 0; i < int(shardCountOutput); i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_, err = receivers[i].Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		err = sender.Prune(cid)
		assert.NoError(t, err)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		cant_msg_received := 0
		cant_msg_not_received := 0

		for i := 0; i < int(shardCountOutput); i++ {
			log.Debugf("Waiting for message")
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
			e, err := receivers[i].Next(ctx)
			assert.NoError(t, err)
			log.Debugf("Received message type %s", e.Type())
			switch e.Type() {
			case middleware.Normal:
				cant_msg_received++
				assert.Equal(t, expected, e.Msg().val)
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
		e.Ack(false)
		cancel()

		for i := 0; i < int(shardCountOutput); i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err = receivers[i].Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		assert.Equal(t, 1, cant_msg_received)
		assert.Equal(t, 1, cant_msg_not_received)

		time.Sleep(time.Second * TIME_TO_WAIT_FOR_CRASH) // we make sure the reducer doesn't crashes.

		stopMapReducer()
		for _, handler := range handlers {
			<-handler
		}
		log.Infof("All handlers finisheded YESSS")
	})

	t.Run("10Reducer2Cid10000MsgEach", func(t *testing.T) {
		init := test7container
		assert.NoError(t, init.Err)
		const cidCount = uint64(2)
		const countPerCID = 10000

		testBulk(t, cidCount, init, countPerCID)
	})

	// Duplicates

	t.Run("DuplicatedNormalMsg", func(t *testing.T) {
		init := test8container
		assert.NoError(t, init.Err)

		cid := uint64(1)
		shardCount := uint(1)
		shardCountOutput := uint(1)
		sender, receivers, stopMapReducer, handlers, err := setupReducerPipelineRK(t, init, shardCount, shardCountOutput, sumMapReducer{}, 10000000000000000000)
		assert.NoError(t, err)

		err = sender.Send(&num{val: 1}, cid, 0)
		assert.NoError(t, err)
		err = sender.Send(&num{val: 1}, cid, 0)
		assert.NoError(t, err)

		for i := range int(shardCountOutput) {
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			_, err = receivers[i].Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		err = sender.Prune(cid)
		assert.NoError(t, err)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		cant_msg_received := []int{}
		cant_msg_not_received := uint(0)

		for i := range int(shardCountOutput) {
			log.Debugf("Waiting for message")
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
			e, err := receivers[i].Next(ctx)
			assert.NoError(t, err)
			log.Debugf("Received message type %s", e.Type())
			switch e.Type() {
			case middleware.Normal:
				cant_msg_received = append(cant_msg_received, i)
				assert.Equal(t, uint64(1), e.Msg().val)
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
		e.Ack(false)
		cancel()

		for i := 0; i < int(shardCountOutput); i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err := receivers[i].Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		assert.Equal(t, []int{0}, cant_msg_received)
		assert.Equal(t, shardCountOutput-1, cant_msg_not_received)

		time.Sleep(time.Second * TIME_TO_WAIT_FOR_CRASH) // we make sure the reducer doesn't crashes.

		stopMapReducer()
		for _, handler := range handlers {
			<-handler
		}
		log.Infof("All handlers finisheded YESSS")
	})

	// Recovery

	t.Run("RecoveringFromCheckpoint", func(t *testing.T) {
		init := test9container
		assert.NoError(t, init.Err)

		cid := uint64(1)
		shardCount := uint(1)
		shardCountOutput := uint(1)
		const maxLogSize = 2
		senderConnector, err := rabbitmq.ConnectorCustom(init.Config)
		assert.NoError(t, err)

		middlewareSenderLogger := logger.NewConsoleLogger("midd_send", logger.Debug)
		senderMiddleware := rabbitmq.NewMiddleware[i](senderConnector, middlewareSenderLogger)
		sender, err := senderMiddleware.WriteTo("input", []string{"map_reducer"}, "1", shardCount)
		assert.NoError(t, err)

		dirPath := t.TempDir()
		mapReducerCtx, stopMapReducer := context.WithCancel(context.Background())
		reducerHandle := newMapReducer(mapReducerCtx, t, init, 0, sumMapReducer{}, shardCount, shardCountOutput, dirPath, maxLogSize)

		receiverConnector, err := rabbitmq.ConnectorCustom(init.Config)
		assert.NoError(t, err)
		middlewareReceiverLogger := logger.NewConsoleLogger("midd_rec", logger.Debug)
		receiverMiddleware := rabbitmq.NewMiddleware[r](receiverConnector, middlewareReceiverLogger)
		receiver, err := receiverMiddleware.ConsumeFrom("map_reducer", "receiver", "0", 1, 1)
		assert.NoError(t, err)
		assert.NotNil(t, receiver)

		partialReducerPath := path.Join(dirPath, "partial_reducer")

		checkpointDirPath := transaction_log.CheckpointDirectory(partialReducerPath)
		logDirPath := transaction_log.LogDirectory(partialReducerPath)

		checkpointDir, err := os.ReadDir(checkpointDirPath)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(checkpointDir), "Checkpoint directory should be empty before sending messages")
		logDir, err := os.ReadDir(logDirPath)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(logDir), "Log directory should contain one log file")

		msgCounter := uint64(0)
		sendMsg := func() {
			err = sender.Send(&num{val: 1}, cid, msgCounter)
			assert.NoError(t, err)
			msgCounter++
		}
		sendMsg()
		time.Sleep(100 * time.Millisecond)
		checkpointDir, err = os.ReadDir(checkpointDirPath)
		assert.NoError(t, err)
		assert.Equal(t, 0, len(checkpointDir), "Checkpoint directory should still be empty")
		logDir, err = os.ReadDir(logDirPath)
		assert.NoError(t, err)
		assert.Equal(t, 1, len(logDir), "Log directory should contain one log file")
		assertContainsDir(t, logDir, "0")
		sendMsg()
		time.Sleep(100 * time.Millisecond)
		logDir, err = os.ReadDir(logDirPath)
		assert.NoError(t, err)
		if !assert.Equal(t, 2, len(logDir), "Log directory (%s) should contain two log files", logDirPath) {
			log.Errorf("Log directory (%s) contents: %v", logDirPath, logDir)
		}
		assertContainsDir(t, logDir, "0")
		assertContainsDir(t, logDir, "1")
		for _, file := range logDir {
			log.Debugf("Log file: '%s'", file.Name())
		}
		checkpointDir, err = os.ReadDir(checkpointDirPath)
		assert.NoError(t, err)
		if !assert.Equal(t, 1, len(checkpointDir), "Checkpoint directory (%s) should contains one checkpoint file", checkpointDirPath) {
			log.Errorf("Checkpoint directory (%s) contents: %v", checkpointDirPath, checkpointDir)
		}
		assertContainsDir(t, checkpointDir, "0")
		sendMsg()
		sendMsg()
		time.Sleep(100 * time.Millisecond)
		logDir, err = os.ReadDir(logDirPath)
		assert.NoError(t, err)
		if !assert.Equal(t, 2, len(logDir), "Log directory (%s) should still contain two log files", logDirPath) {
			log.Errorf("Log directory (%s) contents: %v", logDirPath, logDir)
		}
		assertNotContainsDir(t, logDir, "0")
		assertContainsDir(t, logDir, "1")
		assertContainsDir(t, logDir, "2")
		checkpointDir, err = os.ReadDir(checkpointDirPath)
		assert.NoError(t, err)
		if !assert.Equal(t, 2, len(checkpointDir), "Checkpoint directory (%s) should contains two checkpoint files", checkpointDirPath) {
			log.Errorf("Checkpoint directory (%s) contents: %v", checkpointDirPath, checkpointDir)
		}
		assertContainsDir(t, checkpointDir, "0")
		assertContainsDir(t, checkpointDir, "1")
		sendMsg()
		sendMsg()
		time.Sleep(100 * time.Millisecond)
		logDir, err = os.ReadDir(logDirPath)
		assert.NoError(t, err)
		if !assert.Equal(t, 2, len(logDir), "Log directory (%s) should still contain two log files", logDirPath) {
			log.Errorf("Log directory (%s) contents: %v", logDirPath, logDir)
		}
		assertNotContainsDir(t, logDir, "0")
		assertNotContainsDir(t, logDir, "1")
		assertContainsDir(t, logDir, "2")
		assertContainsDir(t, logDir, "3")
		checkpointDir, err = os.ReadDir(checkpointDirPath)
		assert.NoError(t, err)
		if !assert.Equal(t, 2, len(checkpointDir), "Checkpoint directory (%s) should contains two checkpoint files", checkpointDirPath) {
			log.Errorf("Checkpoint directory (%s) contents: %v", checkpointDirPath, checkpointDir)
		}
		assertNotContainsDir(t, checkpointDir, "0")
		assertContainsDir(t, checkpointDir, "1")
		assertContainsDir(t, checkpointDir, "2")

		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		_, err = receiver.Next(ctx)
		cancel()
		assert.Error(t, err)

		stopMapReducer()
		<-reducerHandle

		mapReducerCtx, stopMapReducer = context.WithCancel(context.Background())
		reducerHandle = newMapReducer(mapReducerCtx, t, init, 0, sumMapReducer{}, shardCount, shardCountOutput, dirPath, maxLogSize)

		sendMsg()

		err = sender.Prune(cid)
		assert.NoError(t, err)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		log.Debugf("Waiting for message")
		ctx, cancel = context.WithTimeout(context.Background(), 50*time.Second)
		e, err := receiver.Next(ctx)
		assert.NoError(t, err)
		log.Debugf("Received message type %s", e.Type())
		assert.Equal(t, middleware.Normal, e.Type())
		assert.Equal(t, msgCounter, e.Msg().val)
		e.Ack(true)

		e, err = receiver.Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, middleware.Prune, e.Type())
		e.Ack(true)

		cancel()

		ctx, cancel = context.WithTimeout(context.Background(), 50*time.Second)
		e, err = receiver.Next(ctx)
		assert.NoError(t, err)
		assert.Equal(t, middleware.EOF, e.Type())
		e.Ack(false)
		cancel()

		for i := 0; i < int(shardCountOutput); i++ {
			ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			_, err := receiver.Next(ctx)
			cancel()
			assert.Error(t, err)
		}

		time.Sleep(time.Second * TIME_TO_WAIT_FOR_CRASH) // we make sure the reducer doesn't crashes.

		stopMapReducer()
		<-reducerHandle
		log.Infof("All handlers finisheded YESSS")
	})

}

func assertContainsDir(t *testing.T, logDir []os.DirEntry, expected string) {
	for _, file := range logDir {
		if file.Name() == expected {
			return
		}
	}
	assert.Fail(t, fmt.Sprintf("should contain %s, actual contents: %v", expected, logDir))
}

func assertNotContainsDir(t *testing.T, logDir []os.DirEntry, expected string) {
	for _, file := range logDir {
		if file.Name() == expected {
			assert.Fail(t, fmt.Sprintf("should not contain %s, actual contents: %v", expected, logDir))
		}
	}
}

func testBulk(t *testing.T, cidCount uint64, init rabbitmq.AsyncDeployRabbitRes, countPerCID uint64) {
	cids := []uint64{}
	for i := range cidCount {
		cids = append(cids, i)
	}
	shardCount := uint(10)
	shardCountOutput := uint(1)
	sender, receivers, stopMapReducer, handlers, err := setupReducerPipelineRK(t, init, shardCount, shardCountOutput, sumMapReducer{}, 10000000000000000000)
	assert.NoError(t, err)

	expecteds := 0
	for i := 0; i < int(countPerCID); i++ {
		for cid := range cidCount {
			err = sender.Send(&num{val: uint64(i)}, cids[cid], uint64(i))
			assert.NoError(t, err)
			if cid == 0 {
				expecteds += i
			}
		}
	}

	for cid := range cidCount {
		err = sender.SendEOF(cids[cid])
		assert.NoError(t, err)
	}

	steps := map[uint64]map[int]int{} //steps
	// no_lider_msg := map[string]uint{} //siempre tiene que dar 9
	for i := range cidCount {
		steps[cids[i]] = map[int]int{}
		for j := 0; j < int(shardCountOutput); j++ {
			steps[cids[i]][j] = -1
		}
	}

	cant_received_msg := map[uint64]uint{}
	for i := 0; i < int(cidCount); i++ {
		cant_received_msg[cids[i]] = 0
	}
	for range cidCount * 3 {
		for i := 0; i < int(shardCountOutput); i++ {
			log.Debugf("Waiting for message")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			e, err := receivers[i].Next(ctx)
			cancel()
			var timeoutErr *middleware.TimeoutErr
			if errors.As(err, &timeoutErr) {
				continue
			} else if err != nil {
				assert.Fail(t, "should not have error")
				log.Errorf("Error: %s", err)
			}

			assert.NoError(t, err)
			log.Debugf("Received message type %s cid %d", e.Type(), e.Cid())
			step := steps[e.Cid()]

			switch step[i] {
			case -1: //primera vez que recibe de este receiver, puede ser normal o prune
				if e.Type() == middleware.Normal {
					assert.Equal(t, uint64(expecteds), e.Msg().val)
					step[i] = 0
					cant_received_msg[e.Cid()]++
				} else if e.Type() == middleware.Prune {
					assert.Equal(t, middleware.Prune, e.Type())
					step[i] = 1
				} else {
					assert.Fail(t, "should not be here")
				}
				e.Ack(true)
			case 0: //segundo mensaje, tiene que ser prune
				assert.Equal(t, middleware.Prune, e.Type())
				step[i] = 1
				e.Ack(true)

			case 1: //tercero, tiene que ser EOF
				assert.Equal(t, middleware.EOF, e.Type())
				step[i] = 2
				e.Ack(true)
			case 2: //cuarto, no tiene que recibir nada
				assert.Fail(t, "should not be here 2?")
			default:
				assert.Fail(t, "should not be here at all")
			}
		}
	}

	//verifico que todos los cid tengan todos en 2
	for cid := range cidCount {
		for i := 0; i < int(shardCountOutput); i++ {
			if steps[cids[cid]][i] != 2 {
				assert.Fail(t, "not 2")
			}
		}
	}

	//verifico que todos los cid hayan recibido solo un mensaje
	for cid := range cidCount {
		if cant_received_msg[cids[cid]] != 1 {
			assert.Fail(t, "not 1")
		}
	}

	for i := 0; i < int(shardCountOutput); i++ {
		//verifico que no tenga mensajes pendientes
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		_, err := receivers[i].Next(ctx)
		cancel()
		assert.Error(t, err)
	}

	time.Sleep(time.Second * TIME_TO_WAIT_FOR_CRASH) // we make sure the reducer doesn't crashes.

	log.Infof("Stopping map reducers")
	stopMapReducer()

	for k, handler := range handlers {
		log.Infof("Waiting for handler %d", k)
		<-handler
	}
}
