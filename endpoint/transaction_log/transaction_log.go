package transaction_log

import (
	"fmt"
	"os"
	"path"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type ReceivedType rune

const (
	ReceivedType_Normal ReceivedType = 'N'
	ReceivedType_EOF    ReceivedType = 'E'
)

// var log = logger.NewConsoleLogger("transaction_logger", logger.Info)

type TransactionLog struct {
	fileName  string
	counter   uint64
	lastIdACK uint64
}

type TransactionLogInterface interface {
	Update(counter uint64, lastIdACK uint64) error
	Close() error
	Counter() uint64
}

func RecoverFromLogs(dirPath string) (TransactionLogInterface, error) {
	logFileName := path.Join(dirPath, logFileName())
	logFile, err := os.Open(logFileName)
	if err != nil {
		return nil, fmt.Errorf("failed to open transaction log: %v", err)
	}
	counter, lastIdACK, errF := ReadLogFile(logFile)
	if err := logFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close transaction log file: %v", err)
	}
	if errF != nil {
		return nil, fmt.Errorf("failed to read transaction log file: %v", errF)
	}

	transactionLog := &TransactionLog{
		fileName:  logFileName,
		counter:   counter,
		lastIdACK: lastIdACK,
	}
	return transactionLog, nil
}

func (t *TransactionLog) Counter() uint64 {
	return max(t.counter, t.lastIdACK)
}

func NewTransactionLog(dirPath string) (TransactionLogInterface, error) {
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		panic(fmt.Errorf("failed to create log cid directory: %v", err))
	}

	logFileNamePath := path.Join(dirPath, logFileName())
	logFile, err := os.Create(logFileNamePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction log file: %v", err)
	}
	if err := logFile.Close(); err != nil {
		return nil, fmt.Errorf("failed to close transaction log file after creation: %v", err)
	}

	return &TransactionLog{
		fileName: logFileNamePath,
		counter:  0,
	}, nil
}

func WriteLogFile(file *os.File, counter uint64, lastIdACK uint64) error {
	encodeCounter, err := codec.Uint64Encode(counter)
	if err != nil {
		return fmt.Errorf("failed to encode counter: %w", err)
	}
	if err := codec.DoWrite(encodeCounter, file); err != nil {
		return fmt.Errorf("failed to write counter to log file: %w", err)
	}
	encodeLastIdACK, err := codec.Uint64Encode(lastIdACK)
	if err != nil {
		return fmt.Errorf("failed to encode last id ack: %w", err)
	}
	if err := codec.DoWrite(encodeLastIdACK, file); err != nil {
		return fmt.Errorf("failed to write last id ack to log file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync log file: %w", err)
	}
	return nil
}

func ReadLogFile(file *os.File) (counter uint64, lastIdACK uint64, err error) {
	counter, err = codec.Uint64Decode(file)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read filename from log file: %w", err)
	}
	lastIdACK, err = codec.Uint64Decode(file)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to read filename from log file: %w", err)
	}
	return counter, lastIdACK, nil
}

func SaveLogSafely(path string, counter uint64, lastIdACK uint64) error {
	tempPath := path + ".tmp"

	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("error creando archivo temporal: %w", err)
	}

	errW := WriteLogFile(file, counter, lastIdACK)
	if err := file.Close(); err != nil {
		return fmt.Errorf("error cerrando archivo: %w", err)
	}
	if errW != nil {
		if rmErr := os.Remove(tempPath); rmErr != nil {
			return fmt.Errorf("error eliminando archivo temporal: %w, original error: %w", rmErr, errW)
		}
		return fmt.Errorf("error escribiendo en archivo temporal: %w", errW)
	}

	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("error renombrando archivo: %w", err)
	}

	return nil
}

func (t *TransactionLog) Update(counter uint64, lastIdACK uint64) error {
	t.counter = counter
	t.lastIdACK = lastIdACK

	err := SaveLogSafely(t.fileName, t.counter, t.lastIdACK)
	if err != nil {
		return fmt.Errorf("failed to save transaction log safely: %w", err)
	}
	return nil
}

func (t *TransactionLog) Close() error {
	if err := os.Remove(t.fileName); err != nil {
		return fmt.Errorf("failed to remove transaction log file: %w", err)
	}

	return nil
}

func logFileName() string {
	return "log"
}
