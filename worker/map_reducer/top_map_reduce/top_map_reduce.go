package top_map_reduce

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/worker/map_reducer"
)

type In = common.Row
type Acc struct {
	top       []common.Row
	isGreater func(a, b common.Row) bool
}
type Res = []common.Row

type TopMapReducer = map_reducer.MapReducer[In, Acc, Res]

func NewTopMapReducer(name string, input string, topSize uint, batchSize uint) (*TopMapReducer, error) {
	return map_reducer.NewMapReducer[In, Acc, Res](name, input, batchSize, &TopMapReduce{topSize})
}

type TopMapReduce struct {
	topSize uint
}

func (r TopMapReduce) Map(in In) Acc {
	return Acc{
		top: []common.Row{in},
		isGreater: func(a common.Row, b common.Row) bool {
			return a.Numerics["budget_sum"] > b.Numerics["budget_sum"]
		},
	}
}

func (r TopMapReduce) Reduce(acc []Acc) Acc {
	newTop := acc[0]
	for _, acc := range acc[1:] {
		newTop.MergeSort(acc, r.topSize)
	}
	return newTop
}

func (r TopMapReduce) Output(acc Acc) Res {
	return acc.top
}

func (a *Acc) MergeSort(b Acc, topSize uint) {
	a_idx := 0
	b_idx := 0
	new_idx := 0
	bTop := b.top
	aTop := a.top
	newSize := min(len(aTop)+len(bTop), int(topSize))
	newTop := make([]common.Row, newSize)
	for range topSize {
		if a_idx > len(aTop)-1 {
			newTop[new_idx] = bTop[b_idx]
			new_idx++
			b_idx++
			continue
		}
		if b_idx > len(bTop)-1 {
			newTop[new_idx] = aTop[a_idx]
			new_idx++
			a_idx++
			continue
		}
		if a.isGreater(aTop[a_idx], bTop[b_idx]) {
			newTop[new_idx] = aTop[a_idx]
			new_idx++
			a_idx++
		} else {
			newTop[new_idx] = bTop[b_idx]
			new_idx++
			b_idx++
		}
	}
	a.top = newTop
}
