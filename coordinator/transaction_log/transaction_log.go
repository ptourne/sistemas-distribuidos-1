package transaction_log

import (
	"fmt"
	"os"
	"path"
	"strconv"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type ReceivedType rune

const (
	ReceivedType_Normal ReceivedType = 'N'
	ReceivedType_EOF    ReceivedType = 'E'
)

// var log = logger.NewConsoleLogger("coordinator_logger", logger.Info)

type transactionLog struct {
	logFileName          string // path to the log file
	cid                  uint64
	fileName             string
	counter              uint64
	read                 []string
	lastReadNotIncluided []byte
	lastIdACK            uint64
}

type TransactionLog interface {
	Update(fileName string, counter uint64, read []string, lastReadNotIncluided []byte, lastIdACK uint64) error
	Recover() (cid uint64, fileName string, counter uint64, read []string, lastReadNotIncluided []byte, lastIdACK uint64)
	Close() error
}

func RecoverFromLogs(dirPath string) ([]TransactionLog, error) {
	cidsLogs, err := logFiles(logDirectory(dirPath))
	if err != nil {
		return nil, fmt.Errorf("failed to read transaction logs files: %v", err)
	}
	if len(cidsLogs) == 0 {
		return []TransactionLog{}, nil
	}

	transactionLogs := make([]TransactionLog, 0, len(cidsLogs))

	for _, dirCid := range cidsLogs {
		if !dirCid.IsDir() {
			panic(fmt.Errorf("expected directory for cid %s, but got file", dirCid.Name()))
		}
		cid := dirCid.Name()
		logFilePath, err := logFiles(path.Join(logDirectory(dirPath), cid))
		if err != nil {
			return nil, fmt.Errorf("failed to read transaction log files for cid %s: %v", cid, err)
		}
		if len(logFilePath) == 0 {
			continue
		}
		logFileName := path.Join(logDirectory(dirPath), cid, logFileName())
		logFile, err := os.Open(logFileName)
		if err != nil {
			return nil, fmt.Errorf("failed to open transaction log file for cid %s: %v", cid, err)
		}
		fileName, counter, read, lastReadNotIncluided, lastIdACK, errF := ReadLogFile(logFile)
		if err := logFile.Close(); err != nil {
			return nil, fmt.Errorf("failed to close transaction log file for cid %s: %v", cid, err)
		}
		if errF != nil {
			return nil, fmt.Errorf("failed to read transaction log file for cid %s: %v", cid, errF)
		}

		cidU, err := strconv.ParseUint(cid, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cid not uint64")
		}

		transactionLog := &transactionLog{
			logFileName:          logFileName,
			cid:                  cidU,
			fileName:             fileName,
			counter:              counter,
			lastReadNotIncluided: lastReadNotIncluided,
			read:                 read,
			lastIdACK:            lastIdACK,
		}
		transactionLogs = append(transactionLogs, transactionLog)
	}
	return transactionLogs, nil
}

func NewTransactionLogForCid(dirPath string, cid uint64) (TransactionLog, error) {
	logCidDirectory := path.Join(logDirectory(dirPath), fmt.Sprintf("%d", cid))
	if err := os.MkdirAll(logCidDirectory, 0755); err != nil {
		panic(fmt.Errorf("failed to create log cid directory: %v", err))
	}

	logFileNamePath := path.Join(logCidDirectory, logFileName())

	return &transactionLog{
		cid:         cid,
		logFileName: logFileNamePath,
	}, nil
}

func WriteLogFile(file *os.File, filename string, counter uint64, read []string, lastReadNotIncluided []byte, lastIdACK uint64) error {
	encodeString, err := codec.StringEncode(filename)
	if err != nil {
		return fmt.Errorf("failed to encode filename: %w", err)
	}
	if err := codec.DoWrite(encodeString, file); err != nil {
		return fmt.Errorf("failed to write filename to log file: %w", err)
	}
	encodeCounter, err := codec.Uint64Encode(counter)
	if err != nil {
		return fmt.Errorf("failed to encode counter: %w", err)
	}
	if err := codec.DoWrite(encodeCounter, file); err != nil {
		return fmt.Errorf("failed to write counter to log file: %w", err)
	}
	encodeCsv, err := codec.CsvRecordEncode(read)
	if err != nil {
		return fmt.Errorf("failed to encode read: %w", err)
	}

	if err := codec.DoWrite(encodeCsv, file); err != nil {
		return fmt.Errorf("failed to write read to log file: %w", err)
	}
	encodeLastReadNotIncluided, err := codec.BytesEncode(lastReadNotIncluided)
	if err != nil {
		return fmt.Errorf("failed to encode last read not included: %w", err)
	}
	if err := codec.DoWrite(encodeLastReadNotIncluided, file); err != nil {
		return fmt.Errorf("failed to write last read not included to log file: %w", err)
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

func ReadLogFile(file *os.File) (filename string, counter uint64, read []string, lastReadNotIncluided []byte, lastIdACK uint64, err error) {
	filename, err = codec.StringDecode(file)
	if err != nil {
		return "", 0, nil, nil, 0, fmt.Errorf("failed to read filename from log file: %w", err)
	}
	counter, err = codec.Uint64Decode(file)
	if err != nil {
		return "", 0, nil, nil, 0, fmt.Errorf("failed to read filename from log file: %w", err)
	}
	read, err = codec.CsvRecordDecode(file)
	if err != nil {
		return "", 0, nil, nil, 0, fmt.Errorf("failed to read filename from log file: %w", err)
	}
	lastReadNotIncluided, err = codec.BytesDecode(file)
	if err != nil {
		return "", 0, nil, nil, 0, fmt.Errorf("failed to read filename from log file: %w", err)
	}
	lastIdACK, err = codec.Uint64Decode(file)
	if err != nil {
		return "", 0, nil, nil, 0, fmt.Errorf("failed to read filename from log file: %w", err)
	}
	return filename, counter, read, lastReadNotIncluided, lastIdACK, nil
}

func SaveLogSafely(path string, fileName string, counter uint64, read []string, lastReadNotIncluided []byte, lastIdACK uint64) error {
	tempPath := path + ".tmp"

	// creo archivo temporal
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("error creando archivo temporal: %w", err)
	}

	errW := WriteLogFile(file, fileName, counter, read, lastReadNotIncluided, lastIdACK)
	if err := file.Close(); err != nil {
		return fmt.Errorf("error cerrando archivo: %w", err)
	}
	if errW != nil {
		if rmErr := os.Remove(tempPath); rmErr != nil {
			return fmt.Errorf("error eliminando archivo temporal: %w, original error: %w", rmErr, errW)
		}
		return fmt.Errorf("error escribiendo en archivo temporal: %w", errW)
	}

	// if fileName == "" {
	// 	return fmt.Errorf("filename cannot be empty")
	// } //testing

	// Renombrar de forma atómica
	if err := os.Rename(tempPath, path); err != nil {
		return fmt.Errorf("error renombrando archivo: %w", err)
	}

	return nil
}

func (t *transactionLog) Update(fileName string, counter uint64, read []string, lastReadNotIncluided []byte, lastIdACK uint64) error {
	t.fileName = fileName
	t.counter = counter
	t.read = read
	t.lastReadNotIncluided = lastReadNotIncluided
	t.lastIdACK = lastIdACK

	err := SaveLogSafely(t.logFileName, t.fileName, t.counter, t.read, t.lastReadNotIncluided, t.lastIdACK)
	if err != nil {
		return fmt.Errorf("failed to save transaction log safely: %w", err)
	}
	return nil
}

func (t *transactionLog) Recover() (cid uint64, fileName string, counter uint64, read []string, lastReadNotIncluided []byte, lastIdACK uint64) {
	return t.cid, t.fileName, t.counter, t.read, t.lastReadNotIncluided, t.lastIdACK
}

func (t *transactionLog) Close() error {
	return nil //TODO
}

func logDirectory(dirPath string) string {
	return path.Join(dirPath, "logs")
}

func logFileName() string {
	return "log"
}

func logFiles(path string) ([]os.DirEntry, error) {
	files, err := os.ReadDir(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []os.DirEntry{}, nil
		}
		return nil, fmt.Errorf("failed to read transaction log directory: %v", err)
	}
	return files, nil
}
