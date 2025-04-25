package map_reducer_sentiment

import (
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/map_reducer"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
)

var WORKER_ID = os.Getenv("WORKER_ID")

var log = logger.NewConsoleLogger(fmt.Sprintf("reduce_by_movieId_%s", WORKER_ID), logger.Info)

type In = common.Row
type Acc struct {
	Sums  map[string]uint `json:"sums" validate:"required"`
	Count map[string]uint    `json:"count" validate:"required"`
}
type Res = common.Row

type MapReducerSum = map_reducer.MapReducer[In, Acc, Res]

func NewMapReducerByMovieId(name string, input string, routingKeys []string, batchSize uint, subscribers []string) (*MapReducerSum, error) {
	return NewMapReducer[In, Acc, Res](name, input, batchSize, &SumMapReduce{}, subscribers, routingKeys)
}

type SumMapReduce struct {
}

func (r SumMapReduce) Map(in In) []Acc {
	rating := in.Numerics["rating"]
	movieId := in.Strings["movieID"]

	return []Acc{
		{Sums: map[string]uint{movieId: rating},
		Count: map[string]uint{movieId: 1}},
	}
}

func (r SumMapReduce) Reduce(acc []Acc) Acc {
	newAcc := acc[0]
	for _, acc := range acc[1:] {
		newAcc.Merge(acc)
	}
	return newAcc
}

func (r SumMapReduce) Output(acc Acc) []Res {
	output := make([]common.Row, len(acc.Sums))
	i := 0
	for movieId, rating := range acc.Sums {
		output[i] = common.Row{
			Numerics: map[string]uint{"count": acc.Count[movieId], "rating": rating},
			Strings:  map[string]string{"movieID": movieId},
			Arrays:   map[string][]string{},
			Floats:   map[string]float64{},
		}
		i++
	}
	return output
}

func (a *Acc) Merge(b Acc) {
	for movieId, rating := range b.Sums {
		prevVal := a.Sums[movieId]
		a.Sums[movieId] = rating + prevVal
		prevVal2 := a.Count[movieId]
		a.Count[movieId] = b.Count[movieId] + prevVal2
		log.Infof("Merging accs: %v", a.Count[movieId])
	}
}

func NewMapReducer[I, A, R any](name string, input string, batchSize uint, mapReducer map_reducer.MapReduce[I, A, R], subscribers []string, routingKeys []string) (*map_reducer.MapReducer[I, A, R], error) {
	var t string = "direct"
	nameId :=fmt.Sprintf("reduce_by_movieId_%s", WORKER_ID)
	// subscribersMap := make(map[string][]string)
	// for _, subscriber := range subscribers {
	// 	subscribersMap[subscriber] = []string{""}
	// }

	if len(routingKeys) == 0 {
		routingKeys = []string{""}
		t = "fanout"
	}

	if batchSize < 2 {
		return nil, fmt.Errorf("batchSize must be at least two")
	}
	connIn, err := middleware.NewRabbitmq[I]()
	if err != nil {
		return nil, err
	}
	inputCh, err := connIn.ConsumeFromRK(input, nameId, t, routingKeys[0])
	if err != nil {
		return nil, err
	}
	connOut, err := middleware.NewRabbitmq[R]()
	if err != nil {
		return nil, err
	}
	output, err := connOut.WriteTo(name, subscribers)
	if err != nil {
		return nil, err
	}

	accName := accName(nameId, routingKeys[0]) 
	connAcc, err := middleware.NewRabbitmq[A]()
	if err != nil {
		return nil, err
	}
	accIn, err := connAcc.ConsumeFrom(accName, nameId)
	if err != nil {
		return nil, err
	}
	err = accIn.Qos(1, 0)
	if err != nil {
		return nil, err
	}
	accOut, err := connAcc.WriteTo(accName, []string{})
	if err != nil {
		return nil, err
	}

	return &map_reducer.MapReducer[I, A, R]{
		BatchSize:             batchSize,
		Input:                 inputCh,
		PartialResultSender:   accOut,
		PartialResultReceiver: accIn,
		MapReduce:             mapReducer,
		Output:                output,
	}, nil
}

func accName(name string, routingKey string) string {
	return name + "_acc"
}