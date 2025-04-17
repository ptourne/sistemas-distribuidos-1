package task

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
	"github.com/ptourne/sistemas-distribuidos-1/middleware"
)

type Operation interface {
	ProcessAndSend(row common.Row) *common.Row
}

type Task interface {
	ProcessAndSend(row common.Row) error
	Input() string
	Name() string
	Connect(middlewareConnection middleware.MiddlewareCola[common.Row]) (chan middleware.Envelope[common.Row], error)
	Finish() error
}
