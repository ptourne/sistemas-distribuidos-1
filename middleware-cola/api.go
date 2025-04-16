package cola

import (
	"github.com/ptourne/sistemas-distribuidos-1/common"
)

//ver de declare, tratr de devovler un struct, ghacer read/write
type MiddlewareCola interface {
	createQueue(inputExchange string, inputQueue string, outputExchange string) error
	read() (*common.Row, error)
	write(queueName string, row *common.Row) error
}