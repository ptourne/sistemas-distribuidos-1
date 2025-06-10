package transaction_log

import (
	"fmt"
	"io"
	"os"
	"path"
	"strconv"

	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type Transaction struct {
	Cid  uint64
	Id   uint64
	Data []byte
}

type TransactionLog interface {
	Received(cid, id uint64, data []byte) error
	ReceivedEOF(cid, id uint64) error
	Acknowledged(cid, id uint64) error
	OpenedTransaction() *Transaction
	IsDuplicate(cid, id uint64) bool
}

type A interface {
	Received(cid, id uint64, data []byte) error
	ReceivedEOF(cid, id uint64) error
	Acknowledged(cid, id uint64) error
	FromCheckpoint(data []byte) error
	Dump() []byte
}

func NewTransactionLogFromDir(dirPath string, parent A) (TransactionLog, error) {
	logs, err := logFiles(dirPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read transaction log directory: %v", err)
	}

	var lastLogFileN int = -1
	for _, log_file := range logs {
		if log_file.IsDir() {
			continue
		}
		logName := log_file.Name()
		logN, err := strconv.Atoi(logName)
		if err != nil {
			return nil, fmt.Errorf("failed to parse transaction log file name %s: %v", logName, err)
		}
		if logN > lastLogFileN {
			lastLogFileN = logN
		}
	}
	switch lastLogFileN {
	case -1:
		return newTransactionLogFromScratch(dirPath)
	case 0:
		return newTransactionLogFromFirstLog(dirPath, parent)
	default:
		return newTransactionLogFromLastLogAndCheckpoint(dirPath, lastLogFileN, parent)
	}
}

func newTransactionLogFromScratch(dirPath string) (TransactionLog, error) {
	tl, err := newTransactionLog(dirPath, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction log from scratch: %v", err)
	}

	logFilePath := path.Join(tl.logDirectory, fmt.Sprintf("%d", tl.idx))
	writer, err := os.Create(logFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction log file: %v", err)
	}
	tl.logWriter = writer
	return tl, nil
}

func newTransactionLogFromFirstLog(dirPath string, parent A) (TransactionLog, error) {
	tlog, err := newTransactionLog(dirPath, 1)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction log from first log: %v", err)
	}
	reader, err := os.Open(path.Join(logDirectory(dirPath), "0"))
	if err != nil {
		return nil, fmt.Errorf("failed to open transaction log file: %v", err)
	}

	err = tlog.CatchUpWithLog(reader, parent)
	if errc := reader.Close(); errc != nil {
		return nil, fmt.Errorf("failed to close previous log file: %w", errc)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to catch up with transaction log 0: %v", err)
	}
	data := parent.Dump()
	tlog.Dump(data)

	return tlog, nil
}

func newTransactionLogFromLastLogAndCheckpoint(dirPath string, lastLogFileN int, parent A) (TransactionLog, error) {
	tlog, err := newTransactionLogFromCheckpoint(dirPath, lastLogFileN, parent)
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction log from first log: %v", err)
	}
	reader, err := os.Open(path.Join(logDirectory(dirPath), "0"))
	if err != nil {
		return nil, fmt.Errorf("failed to open transaction log file: %v", err)
	}

	err = tlog.CatchUpWithLog(reader, parent)
	if errc := reader.Close(); errc != nil {
		return nil, fmt.Errorf("failed to close previous log file: %w", errc)
	}
	if err != nil {
		return nil, fmt.Errorf("failed to catch up with transaction log 0: %v", err)
	}

	data := parent.Dump()
	tlog.Dump(data)

	return tlog, nil
}

func newTransactionLogFromCheckpoint(dirPath string, lastLogFileN int, parent A) (*transactionLog, error) {
	checkpointDirectory := checkpointDirectory(dirPath)
	checkpointFilePath := path.Join(checkpointDirectory, fmt.Sprintf("%d", lastLogFileN-1))
	file, err := os.Open(checkpointFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open checkpoint file: %v", err)
	}

	lastClosedTransactionsLen, err := codec.Uint64Decode(file)
	if err != nil {
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("failed to close previous checkpoint file: %w", err)
		}
		return nil, fmt.Errorf("failed to read last closed transaction from checkpoint file: %v", err)
	}
	lastClosedTransactions := make(map[uint64]uint64, lastClosedTransactionsLen)
	for range lastClosedTransactionsLen {
		cid, err := codec.Uint64Decode(file)
		if err != nil {
			if err := file.Close(); err != nil {
				return nil, fmt.Errorf("failed to close previous checkpoint file: %w", err)
			}
			return nil, fmt.Errorf("failed to read cid last closed transaction from checkpoint file: %v", err)
		}
		lastTransaction, err := codec.Uint64Decode(file)
		if err != nil {
			if err := file.Close(); err != nil {
				return nil, fmt.Errorf("failed to close previous checkpoint file: %w", err)
			}
			return nil, fmt.Errorf("failed to read last closed transaction from checkpoint file: %v and cid: %v", err, cid)
		}
		lastClosedTransactions[cid] = lastTransaction
	}

	data, err := io.ReadAll(file)
	if err != nil {
		if err := file.Close(); err != nil {
			return nil, fmt.Errorf("failed to close previous checkpoint file: %w", err)
		}
		return nil, fmt.Errorf("failed to read data from checkpoint file: %v", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("failed to close previous checkpoint file: %w", err)
	}

	err = parent.FromCheckpoint(data)
	if err != nil {
		return nil, fmt.Errorf("failed to load checkpoint data into parent: %v", err)
	}

	logDirectory := logDirectory(dirPath)
	logFile, err := os.Create(path.Join(logDirectory, fmt.Sprintf("%d", lastLogFileN+1)))
	if err != nil {
		return nil, fmt.Errorf("failed to create transaction log file: %v", err)
	}
	return &transactionLog{
		checkpointDirectory:    checkpointDirectory,
		logDirectory:           logDirectory,
		idx:                    uint64(lastLogFileN),
		logWriter:              logFile,
		openedTransaction:      nil,
		lastClosedTransactions: lastClosedTransactions,
	}, nil
}

func (l transactionLog) CatchUpWithLog(reader io.Reader, parent A) error {
	for {
		logType, err := codec.DoRead(1, reader)
		if err != nil {
			break
		}
		switch LogType(logType[0]) {
		case LogType_Received:
			var log received
			err := log.Decode(reader)
			if err != nil {
				break
			}
			l.received(log)
			switch log.t {
			case ReceivedType_Normal:
				parent.Received(log.cid, log.id, log.data)
			case ReceivedType_EOF:
				parent.ReceivedEOF(log.cid, log.id)
			}
		case LogType_Acknowledged:
			var log acknowledged
			err := log.Decode(reader)
			if err != nil {
				break
			}
			l.closeTransaction(log.cid, log.id)
			parent.Acknowledged(log.cid, log.id)
		default:
			return nil
		}
	}
	return nil
}

func logFiles(dirPath string) ([]os.DirEntry, error) {
	path := logDirectory(dirPath)
	files, err := os.ReadDir(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []os.DirEntry{}, nil
		}
		return nil, fmt.Errorf("failed to read transaction log directory: %v", err)
	}
	return files, nil
}

type transactionLog struct {
	checkpointDirectory    string
	logDirectory           string
	idx                    uint64
	logWriter              *os.File
	openedTransaction      *Transaction
	lastClosedTransactions map[uint64]uint64
}

func (l *transactionLog) Dump(data []byte) error {
	filePath := path.Join(l.checkpointDirectory, fmt.Sprintf("%d", l.idx))
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create log file: %w", err)
	}
	defer file.Close()

	lastClosedTransactionLen := len(l.lastClosedTransactions)
	buf, err := codec.Uint64Encode(uint64(lastClosedTransactionLen))
	if err != nil {
		return fmt.Errorf("failed to encode last closed transaction length: %w", err)
	}
	err = codec.DoWrite(buf, file)
	if err != nil {
		return fmt.Errorf("failed to write last closed transaction length to checkpoint file: %w", err)
	}

	for cid, lastTransaction := range l.lastClosedTransactions {
		buf, err := codec.Uint64Encode(cid)
		if err != nil {
			return fmt.Errorf("failed to encode last closed transaction: %w", err)
		}
		err = codec.DoWrite(buf, file)
		if err != nil {
			return fmt.Errorf("failed to write last closed transaction to checkpoint file: %w", err)
		}
		buf, err = codec.Uint64Encode(lastTransaction)
		if err != nil {
			return fmt.Errorf("failed to encode last closed transaction: %w", err)
		}
		err = codec.DoWrite(buf, file)
		if err != nil {
			return fmt.Errorf("failed to write last closed transaction to checkpoint file: %w", err)
		}

	}
	if len(data) > 0 {
		err = codec.DoWrite(data, file)
		if err != nil {
			return fmt.Errorf("failed to write data to checkpoint file: %w", err)
		}
	}

	if err = file.Sync(); err != nil {
		return fmt.Errorf("failed to sync checkpoint file: %w", err)
	}

	l.idx++
	newLogFilePath := path.Join(l.logDirectory, fmt.Sprintf("%d", l.idx))
	newLogFile, err := os.Create(newLogFilePath)
	if err != nil {
		return fmt.Errorf("failed to create new log file: %w", err)
	}
	if err := l.logWriter.Close(); err != nil {
		return fmt.Errorf("failed to close previous log file: %w", err)
	}
	l.logWriter = newLogFile

	return nil
}

func checkpointDirectory(dirPath string) string {
	return path.Join(dirPath, "checkpoints")
}

func logDirectory(dirPath string) string {
	return path.Join(dirPath, "logs")
}

func newTransactionLog(dirPath string, idx uint64) (*transactionLog, error) {
	checkpointDirectory := checkpointDirectory(dirPath)
	if err := os.MkdirAll(checkpointDirectory, 0755); err != nil {
		panic(fmt.Errorf("failed to create transaction log directory: %v", err))
	}
	logDirectory := logDirectory(dirPath)
	if err := os.MkdirAll(logDirectory, 0755); err != nil {
		panic(fmt.Errorf("failed to create transaction log directory: %v", err))
	}

	return &transactionLog{
		checkpointDirectory:    checkpointDirectory,
		logDirectory:           logDirectory,
		idx:                    idx,
		logWriter:              nil,
		openedTransaction:      nil,
		lastClosedTransactions: make(map[uint64]uint64),
	}, nil
}

func (t *transactionLog) Received(cid, id uint64, data []byte) error {
	received := received{cid, id, ReceivedType_Normal, data}
	t.received(received)
	buf := received.Encode()
	if err := codec.DoWrite(buf, t.logWriter); err != nil {
		return fmt.Errorf("failed to write received log: %w", err)
	}
	if err := t.logWriter.Sync(); err != nil {
		return fmt.Errorf("failed to sync log file: %w", err)
	}
	return nil
}

func (t *transactionLog) ReceivedEOF(cid, id uint64) error {
	received := received{cid, id, ReceivedType_EOF, nil}
	t.received(received)
	buf := received.Encode()
	if err := codec.DoWrite(buf, t.logWriter); err != nil {
		return fmt.Errorf("failed to write received log: %w", err)
	}
	if err := t.logWriter.Sync(); err != nil {
		return fmt.Errorf("failed to sync log file: %w", err)
	}
	return nil
}

func (t *transactionLog) Acknowledged(cid, id uint64) error {
	t.closeTransaction(cid, id)
	buf := acknowledged{cid, id}.Encode()
	if err := codec.DoWrite(buf, t.logWriter); err != nil {
		return fmt.Errorf("failed to write acknowledged log: %w", err)
	}
	if err := t.logWriter.Sync(); err != nil {
		return fmt.Errorf("failed to sync log file: %w", err)
	}
	return nil
}

func (t *transactionLog) OpenedTransaction() *Transaction {
	return t.openedTransaction
}

func (t *transactionLog) IsDuplicate(cid, id uint64) bool {
	lastTransaction, ok := t.lastClosedTransactions[cid]
	return ok && lastTransaction >= id
}

func (t *transactionLog) received(log received) {
	t.openedTransaction = &Transaction{
		Cid:  log.cid,
		Id:   log.id,
		Data: log.data,
	}
}

func (t *transactionLog) closeTransaction(cid uint64, id uint64) {
	if t.openedTransaction != nil &&
		t.openedTransaction.Cid == cid &&
		t.openedTransaction.Id == id {
		t.lastClosedTransactions[cid] = id
		t.openedTransaction = nil
	} else {
		panic(fmt.Sprintf("transaction %d not found", id))
	}
}
