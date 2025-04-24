package task

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
)

type Operation interface {
	ProcessAndSend(row common.Row) *common.Row
}

type Task[I, O any] interface {
	ProcessAndSend(row I) error
	Input() string
	Name() string
	Connect(midIn middleware.MiddlewareCola[I], midOut middleware.MiddlewareCola[O]) ([]chan middleware.Envelope[I], error)
	Finish() error
}
