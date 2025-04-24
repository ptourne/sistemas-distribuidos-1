package main

import (
	"fmt"
	"os"

	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/map_reducer"
)

var WORKER_ID = os.Getenv("WORKER_ID")
var log = logger.NewConsoleLogger(fmt.Sprintf("reduce_top_bottom_avg_rating_%s", WORKER_ID), logger.Info)

type Movie struct {
	ID     string  `json:"id" validate:"required"`
	Title  string  `json:"title" validate:"required"`
	Rating float64 `json:"rating" validate:"required"`
}

type In = common.Row
type Acc struct {
	Top    Movie `json:"top" validate:"required"`
	Bottom Movie `json:"bottom" validate:"required"`
}
type Res = common.Row

func main() {
	mapReducer, err := map_reducer.NewMapReducer[In, Acc, Res]("reduce_top_bottom_avg_rating", "joiner_ratings", 2, &TopBottomReduce{}, []string{"q3"},  []string{})
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

// &common.Row{
// 		Strings: map[string]string{
// 			"movieID": movieID,
// 			"title":   title,
// 		},
// 		Floats: map[string]float64{
// 			"avg_rating": avg,
// 		},
// 	}

func (r TopBottomReduce) Map(in In) []Acc {
	return []Acc{{
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
}

func isGreater(a Movie, b Movie) bool {
	return a.Rating > b.Rating
}

func (r TopBottomReduce) Reduce(acc []Acc) Acc {
	newAcc := acc[0]
	for _, acc := range acc[1:] {
		newAcc.Merge(acc)
	}
	return newAcc
}

func (r TopBottomReduce) Output(acc Acc) []Res {
	rows := make([]common.Row, 2)

	rows[0] = common.Row{
		Strings: map[string]string{"movieID": acc.Top.ID, "title": acc.Top.Title},
		Floats:  map[string]float64{"avg_rating": acc.Top.Rating},
	}

	rows[1] = common.Row{
		Strings: map[string]string{"movieID": acc.Bottom.ID, "title": acc.Bottom.Title},
		Floats:  map[string]float64{"avg_rating": acc.Bottom.Rating},
	}

	return rows
}

func (a *Acc) Merge(b Acc) {
	if b.Top.Rating > a.Top.Rating {
		a.Top = a.Top
	}
	if b.Bottom.Rating < a.Bottom.Rating {
		a.Bottom = b.Bottom
	}
}
