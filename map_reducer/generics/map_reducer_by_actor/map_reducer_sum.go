package map_reducer_sentiment

import (
	"bytes"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/map_reducer"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

type In = *model.Row
type Acc struct {
	Count map[string]uint64
}
type Res = *model.Row

type MapReducerSum = map_reducer.MapReducer[In, *Acc, Res]

func NewMapReducerByActor(
	connector *rabbitmq.RabbitMQConnector,
	name string,
	input string,
	batchSize uint,
	subscribers []string,
	id string,
	count uint,
) (*MapReducerSum, error) {
	return map_reducer.NewMapReducer[In, *Acc, Res](
		connector,
		name,
		input,
		batchSize,
		&SumMapReduce{},
		subscribers,
		[]string{},
		id,
		count,
		1,
	)
}

type SumMapReduce struct {
}

func (r SumMapReduce) Map(in In) []*Acc {
	actor := in.Strings["actor"]
	return []*Acc{
		{Count: map[string]uint64{actor: 1}},
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
	output := make([]*model.Row, len(acc.Count))
	i := 0
	for actor, count := range acc.Count {
		output[i] = &model.Row{
			Numerics: map[string]uint64{"count": count},
			Strings:  map[string]string{"actor": actor},
			Arrays:   map[string][]string{},
			Floats:   map[string]float64{},
		}
		i++
	}
	return output
}

func (a *Acc) Merge(b *Acc) {
	for actor, count := range b.Count {
		prevVal := a.Count[actor]
		a.Count[actor] = prevVal + count
	}
}

func (a Acc) Encode() ([]byte, error) {
	return codec.MapEncode(a.Count, codec.Uint64Encode)
	//return nil, nil
}

func (a *Acc) Decode(data []byte) (*Acc, error) {
	r := bytes.NewReader(data)
	count, err := codec.MapDecode(r, codec.Uint64Decode)
	if err != nil {
		return nil, err
	}
	return &Acc{Count: count}, nil
}
