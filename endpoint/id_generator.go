package main

import (
	"os"
	"path"

	"github.com/ptourne/sistemas-distribuidos-1/endpoint/transaction_log"
)

type IDGenerator interface {
	GenerateID() uint64
	CurrentID() uint64
	Close() error
}

type EndpointIDGenerator struct {
	dirPath        string
	id             uint64
	transactionLog transaction_log.TransactionLogInterface
}

func NewEndpointIDGenerator() IDGenerator {
	dirPath, err := os.Getwd()
	if err != nil {
		log.Errorf("Failed to get current working directory: %v", err)
		panic(err)
	}
	fullPath := path.Join(dirPath, "endpoint-dir-ids")

	tlog, err := transaction_log.RecoverFromLogs(fullPath)

	if err != nil {
		log.Infof("No existing transaction log: %v", err)
		tlog, err = transaction_log.NewTransactionLog(fullPath)
		if err != nil {
			log.Errorf("Failed to create transaction log: %v", err)
			panic(err)
		}
	}

	log.Infof("CID = %d", tlog.Counter())

	return &EndpointIDGenerator{
		dirPath:        fullPath,
		id:             tlog.Counter(),
		transactionLog: tlog,
	}
}

func (e *EndpointIDGenerator) CurrentID() uint64 {
	return e.id
}

func (e *EndpointIDGenerator) GenerateID() uint64 {
	id := e.id
	e.id++

	err := e.transactionLog.Update(
		e.id,
		id,
	)
	if err != nil {
		log.Errorf("Failed to update transaction log with ID %d: %v", id, err)
	}

	return id
}

func (e *EndpointIDGenerator) Close() error {
	if err := e.transactionLog.Close(); err != nil {
		return err
	}
	return nil
}
