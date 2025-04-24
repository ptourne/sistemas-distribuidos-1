package top_map_reduce

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/map_reducer"
)

type ActorMovieCount struct {
	Actor      string `json:"name" validate:"required"`
	Count 	   uint   `json:"budget_sum" validate:"required"`
}

type In = common.Row
type Acc struct {
	Top []ActorMovieCount `json:"top" validate:"required"`
}
type Res = common.Row

type TopMapReducer = map_reducer.MapReducer[In, Acc, Res]

func NewTopMapReducerActor(name string, input string, topSize uint, batchSize uint, subscribers []string) (*TopMapReducer, error) {
	return map_reducer.NewMapReducer[In, Acc, Res](name, input, batchSize, &TopMapReduce{topSize}, subscribers)
}

type TopMapReduce struct {
	topSize uint
}

func (r TopMapReduce) Map(in In) []Acc {
	return []Acc{
		{Top: []ActorMovieCount{
			{
				Actor:      in.Strings["actor"],
				Count: 		in.Numerics["count"],
			},
		}},
	}
}

func isGreater(a ActorMovieCount, b ActorMovieCount) bool {
	return a.Count > b.Count
}

func (r TopMapReduce) Reduce(acc []Acc) Acc {
	newTop := acc[0]
	for _, acc := range acc[1:] {
		newTop.MergeSort(acc, r.topSize)
	}
	return newTop
}

func (r TopMapReduce) Output(acc Acc) []Res {
	rows := make([]common.Row, len(acc.Top))
	for i, actor := range acc.Top {
		rows[i] = common.Row{
			Strings:  map[string]string{"actor": actor.Actor},
			Numerics: map[string]uint{"count": actor.Count},
		}
	}
	return rows
}

func (a *Acc) MergeSort(b Acc, topSize uint) {
	bTop := b.Top
	aTop := a.Top
	newTop := mergeSort(aTop, bTop, isGreater, topSize)
	a.Top = newTop
}

func mergeSort[T any](a, b []T, isGreater func(T, T) bool, topSize uint) []T {
	a_idx := 0
	b_idx := 0
	new_idx := 0
	newSize := min(len(a)+len(b), int(topSize))
	newTop := make([]T, newSize)
	for range newSize {
		if a_idx > len(a)-1 && b_idx < len(b) {
			newTop[new_idx] = b[b_idx]
			new_idx++
			b_idx++
			continue
		}
		if b_idx > len(b)-1 && a_idx < len(a) {
			newTop[new_idx] = a[a_idx]
			new_idx++
			a_idx++
			continue
		}
		if isGreater(a[a_idx], b[b_idx]) {
			newTop[new_idx] = a[a_idx]
			new_idx++
			a_idx++
		} else {
			newTop[new_idx] = b[b_idx]
			new_idx++
			b_idx++
		}
	}
	return newTop
}
