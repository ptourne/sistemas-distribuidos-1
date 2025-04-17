package middleware

import (
	"context"
	"time"

	"github.com/ptourne/sistemas-distribuidos-1/common"
)

//ver de declare, tratr de devovler un struct, ghacer read/write
type MiddlewareCola interface {
	CreateReadQueue(readExchangeName string, readQueueName string) error
	CreateWriteQueue(writeExchangeName string) error
	Read(timeout *time.Timer) (*common.Row, error)
	Write(row *common.Row, ctx context.Context) error
}