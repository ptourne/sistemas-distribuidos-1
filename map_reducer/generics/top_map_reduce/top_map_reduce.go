package top_map_reduce

import (
	"bytes"
	"io"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/map_reducer"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

type CountryBudget struct {
	Name      string
	BudgetSum uint64
}

type In = *model.Row
type Acc struct {
	Top []CountryBudget
}
type Res = *model.Row

type TopMapReducer = map_reducer.MapReducer[In, *Acc, Res]

func NewTopMapReducer(
	connector *rabbitmq.RabbitMQConnector,
	name string,
	input string,
	topSize uint,
	batchSize uint,
	subscribers []string,
	id string,
	count uint,
) (*TopMapReducer, error) {
	return map_reducer.NewMapReducer[In, *Acc, Res](
		connector,
		name,
		input,
		batchSize,
		&TopMapReduce{topSize},
		subscribers,
		[]string{},
		id,
		count,
	)
}

type TopMapReduce struct {
	topSize uint
}

func (r TopMapReduce) Map(in In) []*Acc {
	return []*Acc{
		{Top: []CountryBudget{
			{
				Name:      in.Strings["country"],
				BudgetSum: in.Numerics["budget_sum"],
			},
		}},
	}
}

func isGreater(a CountryBudget, b CountryBudget) bool {
	return a.BudgetSum > b.BudgetSum
}

func (r TopMapReduce) Reduce(acc []*Acc) *Acc {
	newTop := acc[0]
	for _, acc := range acc[1:] {
		newTop.MergeSort(acc, r.topSize)
	}
	return newTop
}

func (r TopMapReduce) Output(acc *Acc) []Res {
	rows := make([]*model.Row, len(acc.Top))
	for i, country := range acc.Top {
		rows[i] = &model.Row{
			Strings:  map[string]string{"country": country.Name},
			Numerics: map[string]uint64{"budget_sum": country.BudgetSum},
		}
	}
	return rows
}

func (a *Acc) MergeSort(b *Acc, topSize uint) {
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

func (c CountryBudget) Encode() ([]byte, error) {
	name, err := codec.StringEncode(c.Name)
	if err != nil {
		return nil, err
	}
	budgetSum, err := codec.Uint64Encode(c.BudgetSum)
	if err != nil {
		return nil, err
	}
	return bytes.Join([][]byte{name, budgetSum}, []byte{}), nil
}

func (c *CountryBudget) Decode(r io.Reader) (*CountryBudget, error) {
	name, err := codec.StringDecode(r)
	if err != nil {
		return nil, err
	}
	budgetSum, err := codec.Uint64Decode(r)
	if err != nil {
		return nil, err
	}
	return &CountryBudget{Name: name, BudgetSum: budgetSum}, nil
}

func (a Acc) Encode() ([]byte, error) {
	return codec.ArrayEncode(a.Top, func(countryBudget CountryBudget) ([]byte, error) {
		return countryBudget.Encode()
	})
}

func (a *Acc) Decode(data []byte) (*Acc, error) {
	r := bytes.NewReader(data)
	topSize, err := codec.Uint64Decode(r)
	if err != nil {
		return nil, err
	}
	top := make([]CountryBudget, topSize)
	for i := range top {
		pointer, err := (&CountryBudget{}).Decode(r)
		if err != nil {
			return nil, err
		}
		top[i] = *pointer
	}
	return &Acc{Top: top}, nil
}
