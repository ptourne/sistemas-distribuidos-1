package middleware

import (
	"context"

	"github.com/ptourne/sistemas-distribuidos-1/common"
)

//ver de declare, tratr de devovler un struct, ghacer read/write
type MiddlewareCola interface {
	CreateReadWriteQueue(readExchangeName string, readQueueName string, writeExchangeName string) error
	Read(inputExchangeName string, inputQueueName string, outputExchangeName string) (*common.Row, error)
	Write(row *common.Row, ctx context.Context) error
}