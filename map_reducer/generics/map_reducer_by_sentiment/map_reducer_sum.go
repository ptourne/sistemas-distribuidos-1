package map_reducer_sentiment

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/map_reducer"
)

type In = common.Row
type Acc struct {
	Sums  map[string]float64 `json:"sums" validate:"required"`
	Count map[string]uint    `json:"count" validate:"required"`
}
type Res = common.Row

type MapReducerSum = map_reducer.MapReducer[In, Acc, Res]

func NewMapReducerBySentiment(name string, input string, batchSize uint, subscribers []string) (*MapReducerSum, error) {
	return map_reducer.NewMapReducer[In, Acc, Res](name, input, batchSize, &SumMapReduce{}, subscribers)
}

type SumMapReduce struct {
}

func (r SumMapReduce) Map(in In) []Acc {
	rate := in.Floats["rate"]
	sentiment := in.Strings["sentiment"]
	return []Acc{
		{Sums: map[string]float64{sentiment: rate}},
		{Count: map[string]uint{sentiment: 1}},
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
	for sentiment, rate := range acc.Sums {
		output[i] = common.Row{
			Numerics: map[string]uint{"count": acc.Count[sentiment]},
			Strings:  map[string]string{"sentiment": sentiment},
			Arrays:   map[string][]string{},
			Floats:   map[string]float64{"rate": rate},
		}
		i++
	}
	return output
}

func (a *Acc) Merge(b Acc) {
	for sentiment, rate := range b.Sums {
		prevVal := a.Sums[sentiment]
		a.Sums[sentiment] = rate + prevVal
		prevVal2 := a.Count[sentiment]
		a.Count[sentiment] = b.Count[sentiment] + prevVal2
	}
}
