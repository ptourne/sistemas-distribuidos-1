package map_reducer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
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

var log *logger.ConsoleLogger = logger.NewConsoleLogger("test", logger.Debug)

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
	return context.WithTimeout(context.Background(), 10*time.Second)
}

const RABBITMQ_EXPOSED_PORT_BASE = uint16(4000)

var baseConfig = rabbitmq.NewConfiguration("guest", "guest", "localhost", RABBITMQ_EXPOSED_PORT_BASE)

func setupReducerPipeline(t *testing.T, init rabbitmq.AsyncDeployRabbitRes, reducerCount uint) (
	sender middleware.Sender[i],
	receiver middleware.Receiver[r],
	stopReducers context.CancelFunc,
	reducerHandles []chan struct{},
	err error,
) {
	senderConnector, err := rabbitmq.ConnectorCustom(init.Config)
	assert.NoError(t, err)

	middlewareSenderLogger := logger.NewConsoleLogger("midd_send", logger.Debug)
	senderMiddleware := rabbitmq.NewMiddleware[i](senderConnector, middlewareSenderLogger)
	sender, err = senderMiddleware.WriteTo("input", []string{"map_reducer"})
	assert.NoError(t, err)

	reducerHandles = make([]chan struct{}, reducerCount)
	mapReducerCtx, stopMapReducer := context.WithCancel(context.Background())
	for idx := range reducerCount {
		reducerConnector, err := rabbitmq.ConnectorCustom(init.Config)
		assert.NoError(t, err)
		id := idx + 1
		mapReducer, err := NewMapReducer(
			reducerConnector,
			"map_reducer",
			"input",
			2,
			sumMapReducer{},
			[]string{"output"},
			[]string{},
			fmt.Sprintf("%d", id),
			reducerCount,
			1,
		)
		assert.NoError(t, err)

		handler := make(chan struct{})
		go func() {
			log.Infof("Starting map reducer: %v", mapReducer)
			err = mapReducer.Run(mapReducerCtx)
			assert.NoErrorf(t, err, "error running map reducer %d: %s", id, err)
			log.Infof("map reducer finished")
			close(handler)
		}()
		reducerHandles[idx] = handler
	}

	receiverConnector, err := rabbitmq.ConnectorCustom(init.Config)
	assert.NoError(t, err)
	middlewareReceiverLogger := logger.NewConsoleLogger("midd_rec", logger.Debug)
	receiverMiddleware := rabbitmq.NewMiddleware[r](receiverConnector, middlewareReceiverLogger)
	receiver, err = receiverMiddleware.ConsumeFrom("map_reducer", "receiver", 1, 1)
	assert.NoError(t, err)
	return sender, receiver, stopMapReducer, reducerHandles, err
}
func skipCI(t *testing.T) {
	if os.Getenv("CI") != "" {
		t.Skip("Skipping testing in CI environment")
	}
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

	t.Run("1Reducer1Cid1Msg", func(t *testing.T) {
		init := test1container
		assert.NoError(t, init.Err)

		cid := "1"
		sender, receiver, stopMapReducer, handlers, err := setupReducerPipeline(t, init, 1)
		assert.NoError(t, err)

		err = sender.Send(&num{val: 1}, cid)
		assert.NoError(t, err)

		ctx, cancel := newTimer()
		_, err = receiver.Next(ctx)
		cancel()
		assert.Error(t, err)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		ctx, cancel = newTimer()
		e, err := receiver.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, middleware.Normal, e.Type())
		assert.Equal(t, uint64(1), e.Msg().val)
		e.Ack(true)

		ctx, cancel = newTimer()
		e, err = receiver.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, middleware.Prune, e.Type())
		e.Ack(true)

		ctx, cancel = newTimer()
		e, err = receiver.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, middleware.EOF, e.Type())
		e.Ack(true)

		time.Sleep(time.Second * 7) // we make sure the reducer doesn't crashes.

		stopMapReducer()
		for _, handler := range handlers {
			<-handler
		}
	})

	t.Run("1Reducer1Cid10Msg", func(t *testing.T) {
		init := test2container
		assert.NoError(t, init.Err)

		cid := "1"
		sender, receiver, stopMapReducer, handlers, err := setupReducerPipeline(t, init, 1)
		assert.NoError(t, err)

		expected := uint64(0)
		for i := range uint64(10) {
			err = sender.Send(&num{val: i}, cid)
			assert.NoError(t, err)
			expected += i
		}

		ctx, cancel := newTimer()
		_, err = receiver.Next(ctx)
		cancel()
		assert.Error(t, err)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		ctx, cancel = newTimer()
		e, err := receiver.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, middleware.Normal, e.Type())
		assert.Equal(t, expected, e.Msg().val)
		e.Ack(true)

		ctx, cancel = newTimer()
		e, err = receiver.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, middleware.Prune, e.Type())
		e.Ack(true)

		ctx, cancel = newTimer()
		e, err = receiver.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, middleware.EOF, e.Type())
		e.Ack(true)

		time.Sleep(time.Second * 7) // we make sure the reducer doesn't crashes.

		stopMapReducer()
		for _, handler := range handlers {
			<-handler
		}
	})

	t.Run("2Reducer1Cid10Msg", func(t *testing.T) {
		init := test3container
		assert.NoError(t, init.Err)

		cid := "1"
		sender, receiver, stopMapReducer, handlers, err := setupReducerPipeline(t, init, 2)
		assert.NoError(t, err)

		expected := uint64(0)
		for i := range uint64(10) {
			err = sender.Send(&num{val: i}, cid)
			assert.NoError(t, err)
			expected += i
		}
		ref := time.Now()
		emptyDuration := time.Since(ref)
		err = sender.Prune(cid)
		fin := time.Since(ref) - emptyDuration
		assert.NoError(t, err)
		log.Infof("Prune duration: %s", fin)

		ctx, cancel := newTimer()
		_, err = receiver.Next(ctx)
		cancel()
		assert.Error(t, err)

		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		ctx, cancel = newTimer()
		e, err := receiver.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, middleware.Normal, e.Type())
		assert.Equal(t, expected, e.Msg().val)
		e.Ack(true)

		ctx, cancel = newTimer()
		e, err = receiver.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, middleware.Prune, e.Type())
		e.Ack(true)

		ctx, cancel = newTimer()
		e, err = receiver.Next(ctx)
		cancel()
		assert.NoError(t, err)
		assert.Equal(t, middleware.EOF, e.Type())
		e.Ack(true)

		time.Sleep(time.Second * 7) // we make sure the reducer doesn't crashes.

		stopMapReducer()
		for _, handler := range handlers {
			<-handler
		}
	})

	t.Run("2Reducer2Cid10MsgEach", func(t *testing.T) {
		init := test4container
		assert.NoError(t, init.Err)

		cid1 := "1"
		cid2 := "2"
		sender, receiver, stopMapReducer, handlers, err := setupReducerPipeline(t, init, 2)
		assert.NoError(t, err)

		expected1 := uint64(0)
		expected2 := uint64(0)
		counter1 := uint64(0)
		counter2 := uint64(0)
		for {
			if counter1 < 10 {
				err = sender.Send(&num{val: counter1}, cid1)
				assert.NoError(t, err)
				expected1 += counter1
				counter1++
			}
			if counter1 > 5 && counter2 < 10 {
				err = sender.Send(&num{val: counter2}, cid2)
				assert.NoError(t, err)
				expected2 += counter2
				counter2++
			}
			if counter1 == 10 && counter2 == 10 {
				break
			}
		}

		ctx, cancel := newTimer()
		_, err = receiver.Next(ctx)
		cancel()
		assert.Error(t, err)

		err = sender.SendEOF(cid1)
		assert.NoError(t, err)
		err = sender.SendEOF(cid2)
		assert.NoError(t, err)

		steps := map[string]uint{
			cid1: 0,
			cid2: 0,
		}
		for range 6 {
			ctx, cancel = newTimer()
			e, err := receiver.Next(ctx)
			cancel()
			assert.NoError(t, err)
			switch steps[e.Cid()] {
			case 0:
				assert.Equal(t, middleware.Normal, e.Type())
				expected := expected1
				if e.Cid() == cid2 {
					expected = expected2
				}
				assert.Equal(t, expected, e.Msg().val)
			case 1:
				assert.Equal(t, middleware.Prune, e.Type())
			case 2:
				assert.NoError(t, err)
				assert.Equal(t, middleware.EOF, e.Type())
			default:
				t.Fatal("Got three msgs from same Cid")
			}
			e.Ack(true)
			steps[e.Cid()]++
		}

		time.Sleep(time.Second * 7) // we make sure the reducer doesn't crashes.

		stopMapReducer()
		for _, handler := range handlers {
			<-handler
		}
	})

	t.Run("10Reducer1Cid10000MsgEach", func(t *testing.T) {
		init := test5container
		assert.NoError(t, init.Err)

		cid := "1"
		sender, receiver, stopMapReducer, handlers, err := setupReducerPipeline(t, init, 10)
		assert.NoError(t, err)
		expected := uint64(0)
		countPerCID := uint64(10000)
		for range countPerCID {
			err = sender.Send(&num{val: 1}, cid)
			assert.NoError(t, err)
			expected += 1
		}
		err = sender.SendEOF(cid)
		assert.NoError(t, err)

		step := uint(0)
		for range 3 {
			log.Debugf("Waiting for message")
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
			e, err := receiver.Next(ctx)
			cancel()
			assert.NoError(t, err)
			cidIdx, err := strconv.Atoi(e.Cid())
			log.Debugf("Received message from cid %d, type %s", cidIdx, e.Type())
			cidIdx--
			assert.NoError(t, err)
			switch step {
			case 0:
				assert.Equal(t, middleware.Normal, e.Type())
				assert.Equal(t, expected, e.Msg().val)
			case 1:
				assert.Equal(t, middleware.Prune, e.Type())
			case 2:
				assert.Equal(t, middleware.EOF, e.Type())
			default:
				t.Fatal("Got three msgs from same Cid")
			}
			e.Ack(true)
			step++
		}

		time.Sleep(time.Second * 7) // we make sure the reducer doesn't crashes.

		log.Infof("Stopping map reducers")
		stopMapReducer()
		for k, handler := range handlers {
			log.Infof("Waiting for handler %d", k)
			<-handler
		}
	})

	t.Run("10Reducer2Cid10000MsgEach", func(t *testing.T) {
		init := test6container
		assert.NoError(t, init.Err)
		const reducerCount = 10
		const cidCount = uint64(2)
		const countPerCID = 10000

		testBulk(t, cidCount, init, reducerCount, countPerCID)
	})

	t.Run("10Reducer10Cid10000MsgEach", func(t *testing.T) {
		init := test7container
		assert.NoError(t, init.Err)
		const reducerCount = 10
		const cidCount = uint64(10)
		const countPerCID = 10000

		testBulk(t, cidCount, init, reducerCount, countPerCID)
	})
}

func testBulk(t *testing.T, cidCount uint64, init rabbitmq.AsyncDeployRabbitRes, reducerCount uint, countPerCID uint64) {
	cids := []string{}
	for i := range cidCount {
		cids = append(cids, fmt.Sprintf("%d", i+1))
	}
	sender, receiver, stopMapReducer, handlers, err := setupReducerPipeline(t, init, reducerCount)
	assert.NoError(t, err)
	expecteds := make([]uint64, cidCount)
	counters := make([]uint64, cidCount)
	startingPoints := make([]uint64, cidCount)
	for i := range cidCount {
		startingPoints[i] = i * uint64(10)
	}
	i := uint64(0)
	finished := uint64(0)
	for {
		if finished == cidCount {
			break
		}
		for j := range cidCount {
			if i > startingPoints[j] && counters[j] < countPerCID {
				err = sender.Send(&num{val: 1}, cids[j])
				assert.NoError(t, err)
				expecteds[j] += 1
				counters[j]++
			} else if counters[j] == countPerCID {
				err = sender.SendEOF(cids[j])
				assert.NoError(t, err)
				finished++
				counters[j]++
			}
			if counters[j] == countPerCID+1 {
				continue
			}
		}
		i++
	}

	steps := map[string]uint{}
	for i := range cidCount {
		steps[cids[i]] = 0
	}
	for range cidCount * 3 {
		log.Debugf("Waiting for message")
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Second)
		e, err := receiver.Next(ctx)
		cancel()
		assert.NoError(t, err)
		cidIdx, err := strconv.Atoi(e.Cid())
		cidIdx--
		assert.NoError(t, err)
		log.Debugf("Received message from cid %d, type %s", cidIdx, e.Type())
		switch steps[e.Cid()] {
		case 0:
			assert.Equal(t, middleware.Normal, e.Type())
			assert.Equal(t, expecteds[cidIdx], e.Msg().val)
		case 1:
			assert.Equal(t, middleware.Prune, e.Type())
		case 2:
			assert.Equal(t, middleware.EOF, e.Type())
		default:
			t.Fatal("Got three msgs from same Cid")
		}
		e.Ack(true)
		steps[e.Cid()]++
	}

	time.Sleep(time.Second * 7) // we make sure the reducer doesn't crashes.

	log.Infof("Stopping map reducers")
	stopMapReducer()
	for k, handler := range handlers {
		log.Infof("Waiting for handler %d", k)
		<-handler
	}
}
