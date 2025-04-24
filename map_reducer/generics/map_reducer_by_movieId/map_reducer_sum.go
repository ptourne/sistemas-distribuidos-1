package map_reducer_sentiment

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/map_reducer"
)

type In = common.Row
type Acc struct {
	Sums  map[string]uint `json:"sums" validate:"required"`
	Count map[string]uint    `json:"count" validate:"required"`
}
type Res = common.Row

type MapReducerSum = map_reducer.MapReducer[In, Acc, Res]

func NewMapReducerByMovieId(name string, input string, batchSize uint, subscribers []string) (*MapReducerSum, error) {
	return map_reducer.NewMapReducer[In, Acc, Res](name, input, batchSize, &SumMapReduce{}, subscribers)
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
	}
}
