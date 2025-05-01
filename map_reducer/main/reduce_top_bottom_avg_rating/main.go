package main

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	map_reducer "github.com/ptourne/sistemas-distribuidos-1/map_reducer"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("reduce_top_bottom_avg_rating_%s", WORKER_ID), logger.Info)

type Movie struct {
	ID     string
	Title  string
	Rating float64
}

type In = *model.Row
type Acc struct {
	Top    Movie
	Bottom Movie
}

type Res = *model.Row

func main() {
	mapReducer, err := map_reducer.NewMapReducer[In, *Acc, Res]("reduce_top_bottom_avg_rating", "joiner_ratings", 2, &TopBottomReduce{}, []string{"q3"}, []string{})
	if err != nil {
		log.Errorf("error creating maperducer: %s", err)
		return
	}
	err = mapReducer.Run()
	if err != nil {
		log.Errorf("error running map reducer: %s", err)
		return
	}
	log.Infof("map reducer finished")
}

type TopBottomReduce struct{}

func (r TopBottomReduce) Map(in In) []*Acc {
	log.Infof("Map: %v", in)
	acc := []*Acc{{
		Top: Movie{
			ID:     in.Strings["movieID"],
			Title:  in.Strings["title"],
			Rating: in.Floats["avg_rating"],
		},
		Bottom: Movie{
			ID:     in.Strings["movieID"],
			Title:  in.Strings["title"],
			Rating: in.Floats["avg_rating"],
		},
	}}
	log.Infof("Map result: %v", acc)
	return acc
}

func (r TopBottomReduce) Reduce(acc []*Acc) *Acc {
	newAcc := acc[0]
	for _, acc := range acc[1:] {
		newAcc.Merge(acc)
	}
	return newAcc
}

func (r TopBottomReduce) Output(acc *Acc) []Res {
	rows := make([]*model.Row, 2)

	rows[0] = &model.Row{
		Strings: map[string]string{"movieID": acc.Top.ID, "title": acc.Top.Title},
		Floats:  map[string]float64{"avg_rating": acc.Top.Rating},
	}

	rows[1] = &model.Row{
		Strings: map[string]string{"movieID": acc.Bottom.ID, "title": acc.Bottom.Title},
		Floats:  map[string]float64{"avg_rating": acc.Bottom.Rating},
	}

	return rows
}

func (m Movie) Encode() ([]byte, error) {
	id, err := codec.StringEncode(m.ID)
	if err != nil {
		return nil, fmt.Errorf("error encoding movie ID: %w", err)
	}
	titleBytes, err := codec.StringEncode(m.Title)
	if err != nil {
		return nil, fmt.Errorf("error encoding movie title: %w", err)
	}
	rating, err := codec.Float64Encode(m.Rating)
	if err != nil {
		return nil, fmt.Errorf("error encoding movie rating: %w", err)
	}
	return bytes.Join([][]byte{id, titleBytes, rating}, []byte{}), nil
}

func (m Movie) Decode(r io.Reader) error {
	id, err := codec.StringDecode(r)
	if err != nil {
		return fmt.Errorf("error decoding movie ID: %w", err)
	}
	titleBytes, err := codec.StringDecode(r)
	if err != nil {
		return fmt.Errorf("error decoding movie title: %w", err)
	}
	rating, err := codec.Float64Decode(r)
	if err != nil {
		return fmt.Errorf("error decoding movie rating: %w", err)
	}
	m.ID = id
	m.Title = titleBytes
	m.Rating = rating
	return nil
}

func (a Acc) Encode() ([]byte, error) {
	topMovie, err := a.Top.Encode()
	if err != nil {
		return nil, fmt.Errorf("error encoding top movie: %w", err)
	}
	bottomMovie, err := a.Bottom.Encode()
	if err != nil {
		return nil, fmt.Errorf("error encoding bottom movie: %w", err)
	}
	return bytes.Join([][]byte{topMovie, bottomMovie}, []byte{}), nil
}

func (a *Acc) Decode(data []byte) error {
	r := bytes.NewReader(data)
	topMovie := &Movie{}
	err := topMovie.Decode(r)
	if err != nil {
		return fmt.Errorf("error decoding top movie: %w", err)
	}
	bottomMovie := &Movie{}
	err = bottomMovie.Decode(r)
	if err != nil {
		return fmt.Errorf("error decoding bottom movie: %w", err)
	}
	a.Top = *topMovie
	a.Bottom = *bottomMovie
	return nil
}

func (a *Acc) Merge(b *Acc) {
	if b.Top.Rating > a.Top.Rating {
		a.Top = b.Top
	}
	if b.Bottom.Rating < a.Bottom.Rating {
		a.Bottom = b.Bottom
	}
}
