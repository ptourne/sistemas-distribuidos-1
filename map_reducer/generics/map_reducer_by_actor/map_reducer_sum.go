package map_reducer_sentiment

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/map_reducer"
)

type In = common.Row
type Acc struct {
	Count map[string]uint    `json:"count" validate:"required"`
}
type Res = common.Row

type MapReducerSum = map_reducer.MapReducer[In, Acc, Res]

func NewMapReducerByActor(name string, input string, batchSize uint, subscribers []string) (*MapReducerSum, error) {
	return map_reducer.NewMapReducer[In, Acc, Res](name, input, batchSize, &SumMapReduce{}, subscribers)
}

type SumMapReduce struct {
}

func (r SumMapReduce) Map(in In) []Acc {
	actor := in.Strings["actor"]
	return []Acc{
		{Count: map[string]uint{actor: 1}},
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
	output := make([]common.Row, len(acc.Count))
	i := 0
	for actor, count := range acc.Count {
		output[i] = common.Row{
			Numerics: map[string]uint{"count": count},
			Strings:  map[string]string{"actor": actor},
			Arrays:   map[string][]string{},
			Floats:   map[string]float64{},
		}
		i++
	}
	return output
}

func (a *Acc) Merge(b Acc) {
	for actor, count := range b.Count {
		prevVal := a.Count[actor]
		a.Count[actor] = prevVal + count 
	}
}
