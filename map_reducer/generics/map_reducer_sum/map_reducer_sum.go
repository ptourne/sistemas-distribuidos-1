package map_reducer_sum

import (
	"bytes"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/map_reducer"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type In = *model.Row
type Acc struct {
	Sums map[string]uint64
}
type Res = *model.Row

type MapReducerSum = map_reducer.MapReducer[In, *Acc, Res]

func NewMapReducerSum(name string, input string, batchSize uint, subscribers []string) (*MapReducerSum, error) {
	return map_reducer.NewMapReducer[In, *Acc, Res](name, input, batchSize, &SumMapReduce{}, subscribers, []string{})
}

type SumMapReduce struct {
}

func (r SumMapReduce) Map(in In) []*Acc {
	budget := in.Numerics["budget"]
	country := in.Strings["country"]
	return []*Acc{
		{Sums: map[string]uint64{country: budget}},
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
	output := make([]Res, len(acc.Sums))
	i := 0
	for country, budgetSum := range acc.Sums {
		output[i] = &model.Row{
			Numerics: map[string]uint64{"budget_sum": budgetSum},
			Strings:  map[string]string{"country": country},
			Arrays:   map[string][]string{},
			Floats:   map[string]float64{},
		}
		i++
	}
	return output
}

func (a *Acc) Merge(b *Acc) {
	for country, budgetSum := range b.Sums {
		prevVal := a.Sums[country]
		a.Sums[country] = budgetSum + prevVal
	}
}

func (a Acc) Encode() ([]byte, error) {
	return codec.MapEncode(a.Sums, codec.Uint64Encode)
}

func (a *Acc) Decode(data []byte) (*Acc, error) {
	r := bytes.NewReader(data)
	sums, err := codec.MapDecode(r, codec.Uint64Decode)
	if err != nil {
		return nil, err
	}
	return &Acc{Sums: sums}, nil
}
