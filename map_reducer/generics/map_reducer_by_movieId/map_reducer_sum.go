package map_reducer_sentiment

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/map_reducer"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

var WORKER_ID = os.Getenv("WORKER_ID")

var log = logger.NewConsoleLogger(fmt.Sprintf("reduce_by_movieId_%s", WORKER_ID), logger.Debug)

type In = *model.Rating
type Acc struct {
	Sums  map[string]uint64
	Count map[string]uint64
}
type Res = *model.Row

type MapReducerSum = map_reducer.MapReducer[In, *Acc, Res]

func NewMapReducerByMovieId(
	connector *rabbitmq.RabbitMQConnector,
	name string,
	input string,
	routingKey string,
	routingKeys []string,
	batchSize uint,
	subscribers []string,
	id string,
	workerCount uint,
	shardCountOutput uint,
	dirPath string,
	maxLogSize uint64,
) (*MapReducerSum, error) {
	return NewMapReducer[In, *Acc, Res](
		connector,
		name,
		input,
		batchSize,
		&SumMapReduce{},
		subscribers,
		routingKey,
		routingKeys,
		id,
		workerCount,
		shardCountOutput,
		dirPath,
		maxLogSize,
	)
}

type SumMapReduce struct {
}

func (r SumMapReduce) Map(in In) []*Acc {
	rating := uint64(in.Rating)
	movieId := fmt.Sprintf("%d", in.Id)

	return []*Acc{
		{Sums: map[string]uint64{movieId: rating},
			Count: map[string]uint64{movieId: 1}},
	}
}

func (r SumMapReduce) Reduce(acc []*Acc) *Acc {
	newAcc := acc[0]
	for _, acc := range acc[1:] {
		newAcc.Merge(acc)
	}
	return newAcc
}

func (r SumMapReduce) Output(acc *Acc) []Res {
	output := make([]*model.Row, len(acc.Sums))
	i := 0
	for movieId, rating := range acc.Sums {
		output[i] = &model.Row{
			Numerics: map[string]uint64{"count": acc.Count[movieId], "rating": rating},
			Strings:  map[string]string{"movieID": movieId},
			Arrays:   map[string][]string{},
			Floats:   map[string]float64{},
		}
		i++
	}
	return output
}

func (a *Acc) Merge(b *Acc) {
	for movieId, rating := range b.Sums {
		prevVal := a.Sums[movieId]
		a.Sums[movieId] = rating + prevVal
		prevVal2 := a.Count[movieId]
		a.Count[movieId] = b.Count[movieId] + prevVal2
		log.Debugf("Merging accs: %v", a.Count[movieId])
	}
}

func NewMapReducer[I codec.Serializable[I], A codec.Serializable[A], R codec.Serializable[R]](
	connector *rabbitmq.RabbitMQConnector,
	name string,
	input string,
	batchSize uint,
	mapReducer map_reducer.MapReduce[I, A, R],
	subscribers []string,
	routingKey string,
	routingKeys []string,
	id string,
	count uint,
	shardCount uint,
	dirPath string,
	maxLogSize uint64,
) (*map_reducer.MapReducer[I, A, R], error) {
	return map_reducer.NewMapReducer(
		connector,
		name,
		input,
		batchSize,
		mapReducer,
		subscribers,
		id,
		count,
		shardCount,
		dirPath,
		maxLogSize,
	)
	// var t string = "direct"
	// nameId := fmt.Sprintf("reduce_by_movieId_%s", WORKER_ID)
	// // subscribersMap := make(map[string][]string)
	// // for _, subscriber := range subscribers {
	// // 	subscribersMap[subscriber] = []string{""}
	// // }

	// if len(routingKeys) == 0 {
	// 	routingKeys = []string{""}
	// 	t = "fanout"
	// }

	// if batchSize < 2 {
	// 	return nil, fmt.Errorf("batchSize must be at least two")
	// }

	// connector, err := rabbitmq.Connector()
	// if err != nil {
	// 	return nil, err
	// }

	// connIn := rabbitmq.NewMiddleware[I](connector)
	// inputCh, err := connIn.ConsumeFromRK(input, nameId, t, routingKeys[0])
	// if err != nil {
	// 	return nil, err
	// }
	// connOut := rabbitmq.NewMiddleware[R](connector)
	// output, err := connOut.WriteTo(name, subscribers)
	// if err != nil {
	// 	return nil, err
	// }

	// accName := accName(nameId, routingKeys[0])
	// connAcc := rabbitmq.NewMiddleware[A](connector)
	// accIn, err := connAcc.ConsumeFrom(accName, nameId)
	// if err != nil {
	// 	return nil, err
	// }
	// err = accIn.Qos(1, 0)
	// if err != nil {
	// 	return nil, err
	// }
	// accOut, err := connAcc.WriteTo(accName, []string{})
	// if err != nil {
	// 	return nil, err
	// }

	// return &map_reducer.MapReducer[I, A, R]{
	// 	BatchSize:             batchSize,
	// 	Input:                 inputCh,
	// 	PartialResultSender:   accOut,
	// 	PartialResultReceiver: accIn,
	// 	MapReduce:             mapReducer,
	// 	Output:                output,
	// }, nil
}

func accName(name string, routingKey string) string {
	return name + "_acc"
}

func (a Acc) Encode() ([]byte, error) {
	sumsBytes, err := codec.MapEncode(a.Sums, codec.Uint64Encode)
	if err != nil {
		return nil, err
	}

	countBytes, err := codec.MapEncode(a.Count, codec.Uint64Encode)
	if err != nil {
		return nil, err
	}

	encoded := append(sumsBytes, countBytes...)
	return encoded, nil
}

func (a *Acc) Decode(data []byte) (*Acc, error) {
	r := bytes.NewReader(data)
	sums, err := codec.MapDecode(r, codec.Uint64Decode)
	if err != nil {
		return nil, err
	}
	counts, err := codec.MapDecode(r, codec.Uint64Decode)
	if err != nil {
		return nil, err
	}
	return &Acc{Sums: sums, Count: counts}, nil
}

type Rating struct {
	Id     uint32
	Rating uint8
}

func (r *Rating) Encode() []byte {
	buf := make([]byte, 5)
	binary.BigEndian.PutUint32(buf, r.Id)
	buf[4] = r.Rating
	return buf
}

func (r *Rating) Decode(data []byte) {
	if len(data) < 5 {
		return
	}
	r.Id = binary.BigEndian.Uint32(data[:4])
	r.Rating = data[4]
}
