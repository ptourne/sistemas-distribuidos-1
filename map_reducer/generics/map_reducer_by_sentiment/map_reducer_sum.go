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
	Sums  map[string]float64
	Count map[string]uint64
}
type Res = *model.Row

type MapReducerSum = map_reducer.MapReducer[In, *Acc, Res]

func NewMapReducerBySentiment(
	connector *rabbitmq.RabbitMQConnector,
	name string,
	input string,
	batchSize uint,
	subscribers []string,
	id string,
	count uint,
	workerCountOutput uint,
	dirPath string,
	maxLogSize uint64,
) (*MapReducerSum, error) {
	return map_reducer.NewMapReducer[In, *Acc, Res](
		connector,
		name,
		input,
		batchSize,
		&SumMapReduce{},
		subscribers,
		id,
		count,
		workerCountOutput,
		dirPath,
		maxLogSize,
	)
}

type SumMapReduce struct {
}

func (r SumMapReduce) Map(in In) []*Acc {
	rate := in.Floats["rate"]
	sentiment := in.Strings["sentiment"]
	return []*Acc{
		{Sums: map[string]float64{sentiment: rate},
			Count: map[string]uint64{sentiment: 1}},
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
	output := make([]*model.Row, len(acc.Sums))
	i := 0
	for sentiment, rate := range acc.Sums {
		output[i] = &model.Row{
			Numerics: map[string]uint64{"count": acc.Count[sentiment]},
			Strings:  map[string]string{"sentiment": sentiment},
			Arrays:   map[string][]string{},
			Floats:   map[string]float64{"rate": rate},
		}
		i++
	}
	return output
}

func (a *Acc) Merge(b *Acc) {
	for sentiment, rate := range b.Sums {
		prevVal := a.Sums[sentiment]
		a.Sums[sentiment] = rate + prevVal
		prevVal2 := a.Count[sentiment]
		a.Count[sentiment] = b.Count[sentiment] + prevVal2
	}
}

func (a Acc) Encode() ([]byte, error) {
	sumsBytes, err := codec.MapEncode(a.Sums, codec.Float64Encode)
	if err != nil {
		return nil, err
	}

	countBytes, err := codec.MapEncode(a.Count, codec.Uint64Encode)
	if err != nil {
		return nil, err
	}

	encoded := append(sumsBytes, countBytes...)
	return encoded, nil
}

func (a *Acc) Decode(data []byte) (*Acc, error) {
	r := bytes.NewReader(data)
	sums, err := codec.MapDecode(r, codec.Float64Decode)
	if err != nil {
		return nil, err
	}
	counts, err := codec.MapDecode(r, codec.Uint64Decode)
	if err != nil {
		return nil, err
	}
	return &Acc{Sums: sums, Count: counts}, nil
}
