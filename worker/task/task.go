package task

import (
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/middleware"
)

type Operation interface {
	ProcessAndSend(row model.Row) *model.Row
}

type Task[I codec.Serializable[I], O codec.Serializable[O]] interface {
	ProcessAndSend(row I, cid string) error
	Input() string
	Name() string
	Connect(midIn middleware.Connection[I], midOut middleware.Connection[O]) ([]chan middleware.Envelope[I], error)
	Finish() error
}
