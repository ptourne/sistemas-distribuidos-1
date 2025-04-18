package map_reducer

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
)

type In = common.Row
type Acc struct {
	top       []common.Row
	isGreater func(a, b common.Row) bool
}
type Res = []common.Row

type TopMapReducer = MapReducer[In, Acc, Res]

func NewTopMapReducer(name string, input string, topSize uint, batchSize uint) (*TopMapReducer, error) {
	return NewMapReducer[In, Acc, Res](name, input, batchSize, &TopMapReduce{topSize})
}

type TopMapReduce struct {
	topSize uint
}

func (r TopMapReduce) Map(in In) Acc {
	return Acc{
		top: []common.Row{in},
		isGreater: func(a common.Row, b common.Row) bool {
			return a.Numerics["name"] > b.Numerics["name"]
		},
	}
}

func (r TopMapReduce) Reduce(acc []Acc) Acc {
	newTop := acc[0]
	for _, acc := range acc[1:] {
		newTop.SortMerge(acc, r.topSize)
	}
	return newTop
}

func (r TopMapReduce) Output(acc Acc) Res {
	return acc.top
}

func (a *Acc) SortMerge(b Acc, topSize uint) {
	i := 0
	j := 0
	bTop := b.top
	aTop := a.top
	newSize := len(aTop) + len(bTop)
	if newSize > int(topSize) {
		newSize = int(topSize)
	}
	newTop := make([]common.Row, newSize)
	for range topSize {
		if i > len(aTop)-1 {
			newTop[j] = bTop[j]
			j++
			continue
		}
		if j > len(bTop)-1 {
			newTop[j] = aTop[i]
			i++
			continue
		}
		if a.isGreater(aTop[i], bTop[j]) {
			newTop[j] = aTop[i]
			i++
		} else {
			newTop[j] = bTop[j]
			j++
		}
	}
	a.top = newTop
}
