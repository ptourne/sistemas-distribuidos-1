package reduce_by_country_sum_budget

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/worker/map_reducer"
)

type In = common.Row
type Acc struct {
	sums map[string]uint
}
type Res = []common.Row

type TopMapReducer = map_reducer.MapReducer[In, Acc, Res]

func NewTopMapReducer(name string, input string, topSize uint, batchSize uint) (*TopMapReducer, error) {
	return map_reducer.NewMapReducer[In, Acc, Res](name, input, batchSize, &SumMapReduce{})
}

type SumMapReduce struct {
}

func (r SumMapReduce) Map(in In) Acc {
	budget := in.Numerics["budget"]
	country := in.Strings["country"]
	return Acc{
		sums: map[string]uint{country: budget},
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
	output := make([]common.Row, len(acc.sums))
	i := 0
	for country, budgetSum := range acc.sums {
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
	for country, budgetSum := range b.sums {
		a.sums[country] += budgetSum
	}
}
