package transaction_log

import (
	"bufio"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

type QueryType uint8

const (
	QUERY_BEGIN QueryType = iota
	QUERY_ROW
	QUERY_END
)

var log = logger.NewConsoleLogger("coordinator_logger", logger.Info)

type transactionLog struct {
	logFileName          string // path to the log file
	cid                  uint64
	fileName             string
	counter              uint64
	read                 []string
	lastReadNotIncluided []byte
	lastIdACK            uint64
	queryFileName        string
	isQueryPhase         bool
	lastQueryNumber      uint8
	lastQueryRows        []*model.Row
	lastQueryEnded       bool
}

type TransactionLog interface {
	Update(fileName string, counter uint64, read []string, lastReadNotIncluided []byte, lastIdACK uint64) error
	Recover() (cid uint64, fileName string, counter uint64, read []string, lastReadNotIncluided []byte, lastIdACK uint64, err error)
	Cid() uint64
	Print() string
	CloseAll() error
	CloseLog() error
	ReadLastQueryRows() (uint8, []*model.Row, bool, error)
	WriteBeginQuery(numberQuery uint8) error
	WriteRowQuery(row *model.Row) error
	WriteEndQuery() error
	RecoverQueryPhase() (uint8, []*model.Row, bool, error)
	IsQueryPhase() bool
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
		verifyQueryPhase, err := verifyQueryPhase(logFilePath)
		if err != nil {
			return nil, fmt.Errorf("failed to verify query phase for cid %s: %v", cid, err)
		}
		var transactionLog = &transactionLog{}
		transactionLog.isQueryPhase = verifyQueryPhase
		transactionLog.queryFileName = path.Join(logDirectory(dirPath), cid, queryFileName())
		transactionLog.logFileName = path.Join(logDirectory(dirPath), cid, logFileName())
		cidU, err := strconv.ParseUint(cid, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cid not uint64")
		}
		transactionLog.cid = cidU

		if verifyQueryPhase {
			lastQueryNumber, lastQueryRows, lastQueryEnded, err := transactionLog.ReadLastQueryRows()
			if err != nil {
				return nil, fmt.Errorf("failed to read last query rows for cid %s: %v", cid, err)
			}
			transactionLog.lastQueryNumber = lastQueryNumber
			transactionLog.lastQueryRows = lastQueryRows
			transactionLog.lastQueryEnded = lastQueryEnded
		} else {
			logFile, err := os.Open(transactionLog.logFileName)
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
			transactionLog.fileName = fileName
			transactionLog.counter = counter
			transactionLog.read = read
			transactionLog.lastReadNotIncluided = lastReadNotIncluided
			transactionLog.lastIdACK = lastIdACK
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
	queryFileNamePath := path.Join(logCidDirectory, "querys")

	return &transactionLog{
		cid:           cid,
		logFileName:   logFileNamePath,
		queryFileName: queryFileNamePath,
	}, nil
}

func verifyQueryPhase(logFilePath []os.DirEntry) (bool, error) {
	//verifico si hay un archivo con el nombre "querys"
	for _, file := range logFilePath {
		if file.Name() == "querys" {
			return true, nil
		}
	}
	return false, nil
}

func (t *transactionLog) IsQueryPhase() bool {
	return t.isQueryPhase
}

func (t *transactionLog) RecoverQueryPhase() (uint8, []*model.Row, bool, error) {
	if !t.isQueryPhase {
		return 0, nil, false, fmt.Errorf("not in query phase")
	}
	return t.lastQueryNumber, t.lastQueryRows, t.lastQueryEnded, nil
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
	if t.isQueryPhase {
		return fmt.Errorf("in query phase")
	}
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

func (t *transactionLog) Recover() (cid uint64, fileName string, counter uint64, read []string, lastReadNotIncluided []byte, lastIdACK uint64, err error) {
	if t.isQueryPhase {
		return 0, "", 0, nil, nil, 0, fmt.Errorf("in query phase")
	}
	return t.cid, t.fileName, t.counter, t.read, t.lastReadNotIncluided, t.lastIdACK, nil
}
func (t *transactionLog) Cid() uint64 {
	return t.cid
}

func (t *transactionLog) CloseAll() error {
	//elimino el archivo de log del cid
	if err := os.Remove(t.logFileName); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove transaction log file: %w", err)
		}
		// Si el archivo no existe, continuamos para intentar borrar el directorio
	}

	if err := os.Remove(t.queryFileName); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove query file: %w", err)
		}
	}

	//elimino el directorio del cid (y todo su contenido)
	logDir := path.Dir(t.logFileName)
	if err := os.RemoveAll(logDir); err != nil {
		if !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove transaction log directory: %w", err)
		}
		// Si el directorio no existe, no es un error
	}
	log.Infof("Transaction log closed and removed for cid: %d", t.cid)

	return nil
}

func (t *transactionLog) CloseLog() error {
	//elimibo el archivo de log del cid
	if err := os.Remove(t.logFileName); err != nil {
		return fmt.Errorf("failed to remove transaction log file: %w", err)
	}

	log.Infof("Transaction log closed for cid: %d", t.cid)

	return nil
}

func logDirectory(dirPath string) string {
	return path.Join(dirPath, "logs")
}

func logFileName() string {
	return "log"
}

func queryFileName() string {
	return "querys"
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

func (t *transactionLog) Print() string {
	return fmt.Sprintf("TransactionLog with cid: %d\nfileName: %s\ncounter: %d\nread: %v\nlastReadNotIncluided: %v\nlastIdACK: %d", t.cid, t.fileName, t.counter, t.read, string(t.lastReadNotIncluided), t.lastIdACK)
}

func (t *transactionLog) WriteBeginQuery(numberQuery uint8) error {
	f, err := os.OpenFile(t.queryFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open query file: %w", err)
	}
	defer f.Close()
	return t.writeBeginQuery(f, numberQuery)
}

func (t *transactionLog) WriteRowQuery(row *model.Row) error {
	f, err := os.OpenFile(t.queryFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open query file: %w", err)
	}
	defer f.Close()
	return t.writeRowQuery(f, row)
}

func (t *transactionLog) WriteEndQuery() error {
	f, err := os.OpenFile(t.queryFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open query file: %w", err)
	}
	defer f.Close()
	return t.writeEndQuery(f)
}

func (t *transactionLog) writeBeginQuery(file *os.File, numberQuery uint8) error {
	queryTypeEncode, err := codec.Uint8Encode(uint8(QUERY_BEGIN))
	if err != nil {
		return fmt.Errorf("failed to encode string: %w", err)
	}
	numberQueryEncode, err := codec.Uint8Encode(numberQuery)
	if err != nil {
		return fmt.Errorf("failed to encode number query: %w", err)
	}
	buffer := make([]byte, len(queryTypeEncode)+len(numberQueryEncode)+1)
	copy(buffer, queryTypeEncode)
	copy(buffer[len(queryTypeEncode):], numberQueryEncode)
	buffer[len(queryTypeEncode)+len(numberQueryEncode)] = '\n'
	return codec.DoWrite(buffer, file)
}

func (t *transactionLog) writeRowQuery(file *os.File, row *model.Row) error {
	queryTypeEncode, err := codec.Uint8Encode(uint8(QUERY_ROW))
	if err != nil {
		return fmt.Errorf("failed to encode string: %w", err)
	}
	encodeRow, err := row.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode row: %w", err)
	}
	buffer := make([]byte, len(queryTypeEncode)+len(encodeRow)+1)
	copy(buffer, queryTypeEncode)
	copy(buffer[len(queryTypeEncode):], encodeRow)
	buffer[len(queryTypeEncode)+len(encodeRow)] = '\n'
	return codec.DoWrite(buffer, file)
}

func (t *transactionLog) writeEndQuery(file *os.File) error {
	buffer := make([]byte, 1+1)
	queryTypeEncode, err := codec.Uint8Encode(uint8(QUERY_END))
	if err != nil {
		return fmt.Errorf("failed to encode string: %w", err)
	}
	copy(buffer, queryTypeEncode)
	buffer[len(queryTypeEncode)] = '\n'
	return codec.DoWrite(buffer, file)
}

func (t *transactionLog) ReadLastQueryRows() (uint8, []*model.Row, bool, error) {
	f, err := os.Open(t.queryFileName)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil, false, fmt.Errorf("failed to open query file: %w", err)
		}
		return 0, nil, false, fmt.Errorf("failed to open query file: %w", err)
	}
	defer f.Close()

	// Leer todas las líneas del archivo
	var lines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if len(lines) == 0 {
		return 0, nil, false, fmt.Errorf("no lines found in query file")
	}

	// Leer desde la última línea hacia la primera
	lastQueryNumber := uint8(0)
	lastRows := []*model.Row{}
	foundBegin := false
	queryEnded := false

	for i := len(lines) - 1; i >= 0; i-- {
		line := lines[i]
		reader := strings.NewReader(line)
		//decodifico primer byte
		queryType, err := codec.Uint8Decode(reader)
		if err != nil {
			log.Errorf("failed to decode query type: %v", err)
			continue
		}

		if queryType == uint8(QUERY_BEGIN) {
			log.Infof("found begin: %s", line)
			// Encontramos el BEGIN, extraer el número y terminar
			n, err := codec.Uint8Decode(reader)
			if err != nil {
				log.Errorf("failed to parse BEGIN line: %v", err)
				continue
			}
			lastQueryNumber = n
			foundBegin = true
			break
		} else if queryType == uint8(QUERY_ROW) {
			log.Infof("found row: %s", line)
			row, err := model.RowDecode(reader)
			if err != nil {
				log.Errorf("failed to decode row: %v", err)
				continue
			}
			// Insertar al inicio para mantener el orden original
			lastRows = append([]*model.Row{row}, lastRows...)
		} else if queryType == uint8(QUERY_END) {
			queryEnded = true
			continue
		} else {
			// Línea corrupta, ignorar
			continue
		}
	}

	// Si no encontramos BEGIN, no hay bloque válido
	if !foundBegin {
		return 0, nil, false, fmt.Errorf("no found begin")
	}

	// Escribir solo el último bloque BEGIN/ROW/END (o BEGIN/ROW si no hay END) en el archivo temporal
	err = updateQueriesLog(t, lastQueryNumber, lastRows, queryEnded)
	if err != nil {
		return lastQueryNumber, lastRows, queryEnded, fmt.Errorf("failed to update queries log: %w", err)
	}

	return lastQueryNumber, lastRows, queryEnded, nil
}

func updateQueriesLog(t *transactionLog, lastQueryNumber uint8, lastRows []*model.Row, queryEnded bool) error {
	tempPath := t.queryFileName + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp query file: %w", err)
	}
	defer tempFile.Close()

	if lastQueryNumber > 0 {
		log.Infof("writing begin query: %d", lastQueryNumber)
		if err := t.writeBeginQuery(tempFile, lastQueryNumber); err != nil {
			return fmt.Errorf("failed to write BEGIN to temp query file: %w", err)
		}
	}
	if len(lastRows) > 0 {
		log.Infof("writing rows: %d", len(lastRows))
		for _, row := range lastRows {
			// Escribir QUERY_ROW
			if err := t.writeRowQuery(tempFile, row); err != nil {
				return fmt.Errorf("failed to write row to temp query file: %w", err)
			}
		}
	}

	if queryEnded {
		log.Infof("writing end query")
		if err := t.writeEndQuery(tempFile); err != nil {
			return fmt.Errorf("failed to write END to temp query file: %w", err)
		}
	}

	if err := tempFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp query file: %w", err)
	}
	if err := os.Rename(tempPath, t.queryFileName); err != nil {
		return fmt.Errorf("failed to rename temp query file: %w", err)
	}

	return nil
}

func ReadQueriesRows(file *os.File) (currentQueryNumber uint8, queryRowsMap map[uint8][]*model.Row, queryEnded bool, err error) {
	// Leer todas las líneas del archivo
	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if len(lines) == 0 {
		return 0, nil, false, fmt.Errorf("no lines found in query file")
	}

	// Leer desde la primera línea hacia la última
	queryRowsMap = make(map[uint8][]*model.Row)
	currentQueryNumber = uint8(0)
	queryEnded = false

	for i := 0; i < len(lines); i++ {
		line := lines[i]
		reader := strings.NewReader(line)

		// Decodificar primer byte
		queryType, err := codec.Uint8Decode(reader)
		if err != nil {
			log.Errorf("failed to decode query type: %v", err)
			continue
		}

		if queryType == uint8(QUERY_BEGIN) {
			log.Infof("found begin: %s", line)
			// Encontramos el BEGIN, extraer el número
			n, err := codec.Uint8Decode(reader)
			if err != nil {
				log.Errorf("failed to parse BEGIN line: %v", err)
				continue
			}
			currentQueryNumber = n
			queryEnded = false
			// Inicializar el slice para esta query si no existe
			if _, exists := queryRowsMap[currentQueryNumber]; !exists {
				queryRowsMap[currentQueryNumber] = []*model.Row{}
			}
		} else if queryType == uint8(QUERY_ROW) {
			log.Infof("found row: %s", line)
			row, err := model.RowDecode(reader)
			if err != nil {
				log.Errorf("failed to decode row: %v", err)
				continue
			}
			// Agregar la row a la query actual
			if currentQueryNumber > 0 {
				queryRowsMap[currentQueryNumber] = append(queryRowsMap[currentQueryNumber], row)
			}
		} else if queryType == uint8(QUERY_END) {
			queryEnded = true
		} else {
			// Línea corrupta, ignorar
			continue
		}
	}

	// Si no encontramos ninguna query válida
	if len(queryRowsMap) == 0 {
		return 0, nil, false, fmt.Errorf("no valid queries found")
	}

	return currentQueryNumber, queryRowsMap, queryEnded, nil
}
