package map_reducer_sum

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/map_reducer"
)

type In = common.Row
type Acc struct {
	Sums map[string]uint `json:"sums" validate:"required"`
}
type Res = []common.Row

type MapReducerSum = map_reducer.MapReducer[In, Acc, Res]

func NewMapReducerSum(name string, input string, batchSize uint) (*MapReducerSum, error) {
	return map_reducer.NewMapReducer[In, Acc, Res](name, input, batchSize, &SumMapReduce{})
}

type SumMapReduce struct {
}

func (r SumMapReduce) Map(in In) Acc {
	budget := in.Numerics["budget"]
	country := in.Strings["country"]
	return Acc{
		Sums: map[string]uint{country: budget},
	}
}

func (r SumMapReduce) Reduce(acc []Acc) Acc {
	newAcc := acc[0]
	for _, acc := range acc[1:] {
		newAcc.Merge(acc)
	}
	return newAcc
}

func (r SumMapReduce) Output(acc Acc) Res {
	output := make([]common.Row, len(acc.Sums))
	i := 0
	for country, budgetSum := range acc.Sums {
		output[i] = common.Row{
			Numerics: map[string]uint{"budget_sum": budgetSum},
			Strings:  map[string]string{"country": country},
			Arrays:   map[string][]string{},
			Floats:   map[string]float64{},
		}
		i++
	}
	return output
}

func (a *Acc) Merge(b Acc) {
	for country, budgetSum := range b.Sums {
		a.Sums[country] += budgetSum
	}
}
