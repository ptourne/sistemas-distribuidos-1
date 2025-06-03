package top_map_reduce

import (
	"bytes"
	"io"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/map_reducer"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware/rabbitmq"
)

type ActorMovieCount struct {
	Actor string
	Count uint64
}

type In = *model.Row
type Acc struct {
	Top []ActorMovieCount
}
type Res = *model.Row

type TopMapReducer = map_reducer.MapReducer[In, *Acc, Res]

func NewTopMapReducerActor(
	connector *rabbitmq.RabbitMQConnector,
	name string,
	input string,
	topSize uint,
	batchSize uint,
	subscribers []string,
	id string,
	count uint,
	workerCountOutput uint,
) (*TopMapReducer, error) {
	return map_reducer.NewMapReducer(
		connector,
		name,
		input,
		batchSize,
		&TopMapReduce{topSize},
		subscribers,
		id,
		count,
		workerCountOutput,
	)
}

type TopMapReduce struct {
	topSize uint
}

func (r TopMapReduce) Map(in In) []*Acc {
	return []*Acc{
		{Top: []ActorMovieCount{
			{
				Actor: in.Strings["actor"],
				Count: in.Numerics["count"],
			},
		}},
	}
}

func isGreater(a ActorMovieCount, b ActorMovieCount) bool {
	return a.Count > b.Count
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
	for i, actor := range acc.Top {
		rows[i] = &model.Row{
			Strings:  map[string]string{"actor": actor.Actor},
			Numerics: map[string]uint64{"count": actor.Count},
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

func (a ActorMovieCount) Encode() ([]byte, error) {
	actor, err := codec.StringEncode(a.Actor)
	if err != nil {
		return nil, err
	}
	count, err := codec.Uint64Encode(a.Count)
	if err != nil {
		return nil, err
	}
	return bytes.Join([][]byte{actor, count}, []byte{}), nil
}

func (a *ActorMovieCount) Decode(r io.Reader) (*ActorMovieCount, error) {
	actor, err := codec.StringDecode(r)
	if err != nil {
		return nil, err
	}
	count, err := codec.Uint64Decode(r)
	if err != nil {
		return nil, err
	}
	return &ActorMovieCount{Actor: actor, Count: count}, nil
}

func (a Acc) Encode() ([]byte, error) {
	return codec.ArrayEncode(a.Top, func(actor ActorMovieCount) ([]byte, error) {
		return actor.Encode()
	})
}

func (a *Acc) Decode(data []byte) (*Acc, error) {
	r := bytes.NewReader(data)
	top, err := codec.ArrayDecode(r, func(r io.Reader) (ActorMovieCount, error) {
		actor := &ActorMovieCount{}
		actor, err := actor.Decode(r)
		return *actor, err
	})
	if err != nil {
		return nil, err
	}
	return &Acc{Top: top}, nil
}
