package transaction_log

import (
	"fmt"
	"os"
	"path"
	"strconv"

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
	logFileName            string // path to the log file
	cid                    uint64
	fileName               string
	counter                uint64
	read                   []string
	lastReadNotIncluided   []byte
	lastIdACK              uint64
	queryFileName          string
	phase                  HandleClientPhase
	lastQueryNumber        uint8
	lastQueryRows          []RowWithID
	lastQueryEnded         bool
	checkpointInterval     uint64   // cada cuántas escrituras hacer checkpoint
	writeCount             uint64   // contador de escrituras desde el último checkpoint
	logFile                *os.File // archivo de log mantenido abierto
	phaseFileName          string   // path to the phase file, used to indicate in which phase is the transaction log
	skipReceivingExactFile bool
}

type TransactionLog interface {
	Update(fileName string, counter uint64, read []string, lastReadNotIncluided []byte, lastIdACK uint64) error
	Recover() (cid uint64, fileName string, counter uint64, read []string, lastReadNotIncluided []byte, lastIdACK uint64, skipReceivingExactFile bool, err error)
	Cid() uint64
	Print() string
	RemoveAll() error
	RemoveLog() error
	CloseLog() error
	CleanLog() error
	ReadLastQueryRows() (uint8, []RowWithID, bool, error)
	WriteBeginQuery(numberQuery uint8) error
	WriteRowQuery(row *model.Row, idRow uint64, senderId uint64) error
	WriteEndQuery() error
	RecoverQueryPhase() (uint8, []RowWithID, bool, error)
	UpdatePhase(phase HandleClientPhase) error
	Phase() HandleClientPhase
}

// Define a struct to hold both id and row
// lastRows will now be a slice of this struct

type RowWithID struct {
	ID       uint64
	Row      *model.Row
	SenderID uint64
}

func RecoverFromLogs(dirPath string, checkpointInterval uint64) ([]TransactionLog, error) {
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
		cidFilesInsideDir, err := logFiles(path.Join(logDirectory(dirPath), cid))
		if err != nil {
			return nil, fmt.Errorf("failed to read transaction log files for cid %s: %v", cid, err)
		}
		if len(cidFilesInsideDir) == 0 {
			continue
		}
		var transactionLog = &transactionLog{
			queryFileName: path.Join(logDirectory(dirPath), cid, queryFileName()),
			logFileName:   path.Join(logDirectory(dirPath), cid, logFileName()),
			phaseFileName: path.Join(logDirectory(dirPath), cid, phaseFileName()),
		}
		err = transactionLog.RecoverPhase()
		if err != nil {
			return nil, fmt.Errorf("failed to recover phase for cid %s: %v", cid, err)
		}
		transactionLog.checkpointInterval = checkpointInterval // valor por defecto para logs recuperados
		transactionLog.writeCount = 0
		cidU, err := strconv.ParseUint(cid, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("cid not uint64")
		}
		transactionLog.cid = cidU

		switch transactionLog.phase {
		case HandleClientPhase_RecInput:
			logFile, err := os.Open(transactionLog.logFileName)
			if err != nil {
				log.Errorf("failed to open transaction log file for cid %s: %v", cid, err)
			}
			fileName, counter, read, lastReadNotIncluided, lastIdACK, skipReceivingExactFile := ReadLastLogEntry(logFile)
			if err := logFile.Close(); err != nil {
				log.Errorf("failed to close transaction log file for cid %s: %v", cid, err)
			}
			transactionLog.skipReceivingExactFile = skipReceivingExactFile
			transactionLog.fileName = fileName
			transactionLog.counter = counter
			transactionLog.read = read
			transactionLog.lastReadNotIncluided = lastReadNotIncluided
			transactionLog.lastIdACK = lastIdACK

			// log.Infof("RECOVER: \n fileName: %s \n counter: %d \n read: %v \n lastReadNotIncluided: %v \n lastIdACK: %d", fileName, counter, read, lastReadNotIncluided, lastIdACK)

			// //actualizo el archivo de log
			err = SaveLogSafely(transactionLog.logFileName, fileName, counter, read, lastReadNotIncluided, lastIdACK)
			if err != nil {
				return nil, fmt.Errorf("failed to save log file: %w", err)
			}
			transactionLog.writeCount = 1
			logFile, err = os.OpenFile(transactionLog.logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				return nil, fmt.Errorf("failed to open log file for writing for cid %s: %v", cid, err)
			}
			transactionLog.logFile = logFile
		case HandleClientPhase_RecQuery:
			lastQueryNumber, lastQueryRows, lastQueryEnded, err := transactionLog.ReadLastQueryRows()
			if err != nil {
				log.Errorf("failed to read last query rows for cid %s: %v", cid, err)
			}
			transactionLog.lastQueryNumber = lastQueryNumber
			transactionLog.lastQueryRows = lastQueryRows
			transactionLog.lastQueryEnded = lastQueryEnded
		}

		transactionLogs = append(transactionLogs, transactionLog)
	}
	return transactionLogs, nil
}

func NewTransactionLogForCid(dirPath string, cid uint64, checkpointInterval uint64) (TransactionLog, error) {
	logCidDirectory := path.Join(logDirectory(dirPath), fmt.Sprintf("%d", cid))
	if err := os.MkdirAll(logCidDirectory, 0755); err != nil {
		panic(fmt.Errorf("failed to create log cid directory: %v", err))
	}

	logFileNamePath := path.Join(logCidDirectory, logFileName())
	queryFileNamePath := path.Join(logCidDirectory, queryFileName())
	phaseFileNamePath := path.Join(logCidDirectory, phaseFileName())

	// Abrir el archivo de log en modo append
	logFile, err := os.OpenFile(logFileNamePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}

	tlog := &transactionLog{
		cid:                cid,
		logFileName:        logFileNamePath,
		queryFileName:      queryFileNamePath,
		phaseFileName:      phaseFileNamePath,
		checkpointInterval: checkpointInterval,
		writeCount:         0,
		logFile:            logFile,
	}

	err = tlog.UpdatePhase(HandleClientPhase_RecInput)
	if err != nil {
		if closeErr := tlog.CloseLog(); closeErr != nil {
			return nil, fmt.Errorf("failed to close log file after phase update error: %w", closeErr)
		}
		return nil, fmt.Errorf("failed to update phase: %w", err)
	}
	return tlog, nil
}

func verifyQueryPhase(logFilePath []os.DirEntry) (bool, error) {
	//verifico si hay un archivo con el nombre "querys"
	for _, file := range logFilePath {
		if file.Name() == queryFileName() {
			return true, nil
		}
	}
	return false, nil
}

func (t *transactionLog) Phase() HandleClientPhase {
	//verifico si hay un archivo con el nombre "querys
	return t.phase
}

func (t *transactionLog) RecoverQueryPhase() (uint8, []RowWithID, bool, error) {
	if t.phase != HandleClientPhase_RecQuery {
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

func readLogFile(file *os.File) (filename string, counter uint64, read []string, lastReadNotIncluided []byte, lastIdACK uint64, err error) {
	filename, err = codec.StringDecode(file)
	if err != nil {
		return "", 0, nil, nil, 0, fmt.Errorf("failed to read filename from log file: %w", err)
	}
	counter, err = codec.Uint64Decode(file)
	if err != nil {
		return "", 0, nil, nil, 0, fmt.Errorf("failed to read counter from log file: %w", err)
	}
	read, err = codec.CsvRecordDecode(file)
	if err != nil {
		return "", 0, nil, nil, 0, fmt.Errorf("failed to read read from log file: %w", err)
	}
	lastReadNotIncluided, err = codec.BytesDecode(file)
	if err != nil {
		return "", 0, nil, nil, 0, fmt.Errorf("failed to read last read not included from log file: %w", err)
	}
	lastIdACK, err = codec.Uint64Decode(file)
	if err != nil {
		return "", 0, nil, nil, 0, fmt.Errorf("failed to read last id ack from log file: %w", err)
	}
	return filename, counter, read, lastReadNotIncluided, lastIdACK, nil
}

func ReadLastLogEntry(file *os.File) (filename string, counter uint64, read []string, lastReadNotIncluided []byte, lastIdACK uint64, skipReceivingExactFile bool) {
	skipReceivingExactFile = true
	// Leer todas las entradas del archivo hasta encontrar un error de parseo
	for {
		currentFilename, currentCounter, currentRead, currentLastReadNotIncluided, currentLastIdACK, err := readLogFile(file)
		if err != nil {
			// Si hay error de parseo, devolver los últimos valores válidos
			return filename, counter, read, lastReadNotIncluided, lastIdACK, skipReceivingExactFile
		}

		// Guardar los valores actuales como los últimos válidos
		filename = currentFilename
		counter = currentCounter
		read = currentRead
		lastReadNotIncluided = currentLastReadNotIncluided
		lastIdACK = currentLastIdACK
		skipReceivingExactFile = false
	}
}

func SaveLogSafely(path string, fileName string, counter uint64, read []string, lastReadNotIncluided []byte, lastIdACK uint64) error {
	tempPath := path + ".tmp"

	// creo archivo temporal
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("error creando archivo temporal: %w", err)
	}

	defer file.Close()

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

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
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
	if t.phase != HandleClientPhase_RecInput {
		return fmt.Errorf("in query phase")
	}
	t.fileName = fileName
	t.counter = counter
	t.read = read
	t.lastReadNotIncluided = lastReadNotIncluided
	t.lastIdACK = lastIdACK

	// Incrementar contador de escrituras
	t.writeCount++
	// Verificar si es momento de hacer checkpoint
	if t.writeCount >= t.checkpointInterval {
		// Cerrar el archivo actual antes de hacer checkpoint
		if t.logFile != nil {
			if err := t.logFile.Close(); err != nil {
				return fmt.Errorf("failed to close log file before checkpoint: %w", err)
			}
		}

		// Hacer checkpoint usando SaveLogSafely (reemplaza todo el archivo)
		err := SaveLogSafely(t.logFileName, t.fileName, t.counter, t.read, t.lastReadNotIncluided, t.lastIdACK)
		if err != nil {
			return fmt.Errorf("failed to save checkpoint: %w", err)
		}

		// Reabrir el archivo después del checkpoint
		logFile, err := os.OpenFile(t.logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to reopen log file after checkpoint: %w", err)
		}
		t.logFile = logFile
		t.writeCount = 0
		log.Infof("Checkpoint created for cid %d after %d writes", t.cid, t.checkpointInterval)
	} else {
		err := WriteLogFile(t.logFile, t.fileName, t.counter, t.read, t.lastReadNotIncluided, t.lastIdACK)
		if err != nil {
			return fmt.Errorf("failed to write to log file: %w", err)
		}
	}
	return nil
}

func (t *transactionLog) Recover() (cid uint64, fileName string, counter uint64, read []string, lastReadNotIncluided []byte, lastIdACK uint64, skipReceivingExactFile bool, err error) {
	if t.phase != HandleClientPhase_RecInput {
		return 0, "", 0, nil, nil, 0, false, fmt.Errorf("not in receiving phase")
	}

	return t.cid, t.fileName, t.counter, t.read, t.lastReadNotIncluided, t.lastIdACK, t.skipReceivingExactFile, nil
}
func (t *transactionLog) Cid() uint64 {
	return t.cid
}

func (t *transactionLog) RemoveAll() error {
	// Cerrar el archivo de log si está abierto
	if t.logFile != nil {
		if err := t.logFile.Close(); err != nil {
			return fmt.Errorf("failed to close log file: %w", err)
		}
		t.logFile = nil
	}

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

func (t *transactionLog) RemoveLog() error {
	// Cerrar el archivo de log si está abierto
	if t.logFile != nil {
		if err := t.logFile.Close(); err != nil {
			return fmt.Errorf("failed to close log file: %w", err)
		}
		t.logFile = nil
	}

	//elimibo el archivo de log del cid
	if err := os.Remove(t.logFileName); err != nil {
		return fmt.Errorf("failed to remove transaction log file: %w", err)
	}

	log.Infof("Transaction log closed for cid: %d", t.cid)

	return nil
}

func (t *transactionLog) CloseLog() error {
	if t.logFile != nil {
		if err := t.logFile.Close(); err != nil {
			return fmt.Errorf("failed to close log file: %w", err)
		}
		t.logFile = nil
	}
	return nil
}

func (t *transactionLog) CleanLog() error {
	//dejo el archivo limpio (sin ningun byte)
	if t.logFile != nil {
		// Truncate the file to size 0 to empty it
		if err := t.logFile.Truncate(0); err != nil {
			return fmt.Errorf("failed to truncate log file: %w", err)
		}
		// Seek to beginning of file
		if _, err := t.logFile.Seek(0, 0); err != nil {
			return fmt.Errorf("failed to seek to beginning of log file: %w", err)
		}
		// Sync to ensure changes are written to disk
		if err := t.logFile.Sync(); err != nil {
			return fmt.Errorf("failed to sync log file: %w", err)
		}
	}
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

func phaseFileName() string {
	return "phase"
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

func (t *transactionLog) WriteRowQuery(row *model.Row, idRow uint64, senderId uint64) error {
	f, err := os.OpenFile(t.queryFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open query file: %w", err)
	}
	defer f.Close()
	return t.writeRowQuery(f, row, idRow, senderId)
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
	buffer := make([]byte, len(queryTypeEncode)+len(numberQueryEncode))
	copy(buffer, queryTypeEncode)
	copy(buffer[len(queryTypeEncode):], numberQueryEncode)
	if err := codec.DoWrite(buffer, file); err != nil {
		return fmt.Errorf("failed to write buffer to file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}
	return nil
}

func (t *transactionLog) writeRowQuery(file *os.File, row *model.Row, idRow uint64, senderId uint64) error {
	queryTypeEncode, err := codec.Uint8Encode(uint8(QUERY_ROW))
	if err != nil {
		return fmt.Errorf("failed to encode query type: %w", err)
	}
	encodeRow, err := row.Encode()
	if err != nil {
		return fmt.Errorf("failed to encode row: %w", err)
	}
	idRowEncode, err := codec.Uint64Encode(idRow)
	if err != nil {
		return fmt.Errorf("failed to encode id row: %w", err)
	}
	idSenderEncode, err := codec.Uint64Encode(senderId)
	if err != nil {
		return fmt.Errorf("failed to encode sender id: %w", err)
	}
	buffer := make([]byte, len(queryTypeEncode)+len(encodeRow)+len(idRowEncode)+len(idSenderEncode))
	copy(buffer, queryTypeEncode)
	copy(buffer[len(queryTypeEncode):], encodeRow)
	copy(buffer[len(queryTypeEncode)+len(encodeRow):], idRowEncode)
	copy(buffer[len(queryTypeEncode)+len(encodeRow)+len(idRowEncode):], idSenderEncode)
	if err := codec.DoWrite(buffer, file); err != nil {
		return fmt.Errorf("failed to write buffer to file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}
	return nil
}

func (t *transactionLog) writeEndQuery(file *os.File) error {
	queryTypeEncode, err := codec.Uint8Encode(uint8(QUERY_END))
	if err != nil {
		return fmt.Errorf("failed to encode string: %w", err)
	}
	buffer := make([]byte, len(queryTypeEncode))
	copy(buffer, queryTypeEncode)
	if err := codec.DoWrite(buffer, file); err != nil {
		return fmt.Errorf("failed to write buffer to file: %w", err)
	}
	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}
	return nil
}

func (t *transactionLog) ReadLastQueryRows() (uint8, []RowWithID, bool, error) {
	f, err := os.Open(t.queryFileName)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil, false, fmt.Errorf("failed to open query file: %w", err)
		}
		return 0, nil, false, fmt.Errorf("failed to open query file: %w", err)
	}
	defer f.Close()

	// Leer datos binarios directamente de arriba a abajo
	lastQueryNumber := uint8(0)
	lastRows := []RowWithID{}
	foundBegin := false
	queryEnded := false

	for {
		// Leer tipo de query
		queryType, err := codec.Uint8Decode(f)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Errorf("failed to decode query type2: %v", err)
			break
		}

		if queryType == uint8(QUERY_BEGIN) {
			// Encontramos el BEGIN, extraer el número
			n, err := codec.Uint8Decode(f)
			if err != nil {
				log.Errorf("failed to parse BEGIN: %v", err)
				break
			}
			lastQueryNumber = n
			lastRows = []RowWithID{} // Reset rows for new query
			foundBegin = true
			queryEnded = false
		} else if queryType == uint8(QUERY_ROW) {
			row, err := model.RowDecode(f)
			if err != nil {
				log.Errorf("failed to decode row: %v", err)
				break
			}
			idRow, err := codec.Uint64Decode(f)
			if err != nil {
				log.Errorf("failed to decode id row: %v", err)
				break
			}
			senderId, err := codec.Uint64Decode(f)
			if err != nil {
				log.Errorf("failed to decode sender id: %v", err)
				break
			}
			lastRows = append(lastRows, RowWithID{ID: idRow, Row: row, SenderID: senderId})
		} else if queryType == uint8(QUERY_END) {
			queryEnded = true
		} else {
			// Tipo de query desconocido, ignorar
			log.Errorf("unknown query type: %d", queryType)
			break
		}
	}

	// Si no encontramos BEGIN, no hay bloque válido
	if !foundBegin {
		return 0, nil, false, nil
	}

	// Escribir solo el último bloque BEGIN/ROW/END (o BEGIN/ROW si no hay END) en el archivo temporal
	err = updateQueriesLog(t, lastQueryNumber, lastRows, queryEnded)
	if err != nil {
		return lastQueryNumber, lastRows, queryEnded, fmt.Errorf("failed to update queries log: %w", err)
	}

	return lastQueryNumber, lastRows, queryEnded, nil
}

type HandleClientPhase uint8

const (
	HandleClientPhase_RecInput HandleClientPhase = iota
	HandleClientPhase_RecQuery
	HandleClientPhase_Finished
)

func (p HandleClientPhase) Encode() []byte {
	return []byte{byte(p)}
}

func (p *HandleClientPhase) Decode(data []byte) error {
	if len(data) != 1 {
		return fmt.Errorf("invalid data length for HandleClientPhase: %d", len(data))
	}
	*p = HandleClientPhase(data[0])
	return nil
}

func (t *transactionLog) UpdatePhase(phase HandleClientPhase) error {
	err := savePhaseSafely(t.phaseFileName, phase)
	if err != nil {
		return fmt.Errorf("failed to save phase: %w", err)
	}
	t.phase = phase
	if phase == HandleClientPhase_RecQuery {
		//creo el archivo de querys vacio
		tempPath := t.queryFileName + ".tmp"
		tempFile, err := os.Create(tempPath)
		if err != nil {
			return fmt.Errorf("failed to create temp query file: %w", err)
		}
		defer tempFile.Close()
		tempFile.Close()
		//renombro el archivo de querys
		if err := os.Rename(tempPath, t.queryFileName); err != nil {
			return fmt.Errorf("failed to rename temp query file: %w", err)
		}
	}
	return nil
}

func (t *transactionLog) RecoverPhase() error {
	file, err := os.Open(t.phaseFileName)
	if err != nil {
		if os.IsNotExist(err) {
			t.phase = HandleClientPhase_RecInput
			return nil // Si no existe el archivo, asumimos que estamos en la fase inicial
		}
		return fmt.Errorf("failed to open phase file: %w", err)
	}
	defer file.Close()
	data, err := codec.DoRead(1, file)
	if err != nil {
		return fmt.Errorf("failed to read phase file: %w", err)
	}
	if err := t.phase.Decode(data); err != nil {
		return fmt.Errorf("failed to decode phase: %w", err)
	}
	return nil

}

func savePhaseSafely(path string, phase HandleClientPhase) error {
	tempPath := path + ".tmp"

	// creo archivo temporal
	file, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("error creando archivo temporal: %w", err)
	}
	defer file.Close()

	errW := codec.DoWrite(phase.Encode(), file)
	if errW != nil {
		if rmErr := os.Remove(tempPath); rmErr != nil {
			return fmt.Errorf("error eliminando archivo temporal: %w, original error: %w", rmErr, errW)
		}
		return fmt.Errorf("error escribiendo en archivo temporal: %w", errW)
	}

	if err := file.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
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

func updateQueriesLog(t *transactionLog, lastQueryNumber uint8, lastRows []RowWithID, queryEnded bool) error {
	tempPath := t.queryFileName + ".tmp"
	tempFile, err := os.Create(tempPath)
	if err != nil {
		return fmt.Errorf("failed to create temp query file: %w", err)
	}
	defer tempFile.Close()

	if lastQueryNumber > 0 {
		if err := t.writeBeginQuery(tempFile, lastQueryNumber); err != nil {
			return fmt.Errorf("failed to write BEGIN to temp query file: %w", err)
		}
	}
	if len(lastRows) > 0 {
		for _, rowWithID := range lastRows {
			// Escribir QUERY_ROW
			if err := t.writeRowQuery(tempFile, rowWithID.Row, rowWithID.ID, rowWithID.SenderID); err != nil {
				return fmt.Errorf("failed to write row to temp query file: %w", err)
			}
		}
	}

	if queryEnded {
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

func ReadQueriesRows(file *os.File) (currentQueryNumber uint8, queryRowsMap map[uint8][]RowWithID, queryEnded bool, err error) {
	// Leer datos binarios directamente de arriba a abajo
	queryRowsMap = make(map[uint8][]RowWithID)
	currentQueryNumber = uint8(0)
	queryEnded = false

	for {
		// Leer tipo de query
		queryType, err := codec.Uint8Decode(file)
		if err != nil {
			if err.Error() == "EOF" {
				break
			}
			log.Errorf("failed to decode query type: %v", err)
			break
		}

		if queryType == uint8(QUERY_BEGIN) {
			// Encontramos el BEGIN, extraer el número
			n, err := codec.Uint8Decode(file)
			if err != nil {
				log.Errorf("failed to parse BEGIN: %v", err)
				break
			}
			currentQueryNumber = n
			queryEnded = false
			// Inicializar el slice para esta query si no existe
			if _, exists := queryRowsMap[currentQueryNumber]; !exists {
				queryRowsMap[currentQueryNumber] = []RowWithID{}
			}
		} else if queryType == uint8(QUERY_ROW) {
			row, err := model.RowDecode(file)
			if err != nil {
				log.Errorf("failed to decode row: %v", err)
				break
			}
			idRow, err := codec.Uint64Decode(file)
			if err != nil {
				log.Errorf("failed to decode id row: %v", err)
				break
			}
			senderId, err := codec.Uint64Decode(file)
			if err != nil {
				log.Errorf("failed to decode sender id: %v", err)
				break
			}
			// Agregar la row con id a la query actual
			if currentQueryNumber > 0 {
				queryRowsMap[currentQueryNumber] = append(queryRowsMap[currentQueryNumber], RowWithID{ID: idRow, Row: row, SenderID: senderId})
			}
		} else if queryType == uint8(QUERY_END) {
			queryEnded = true
		} else {
			// Tipo de query desconocido, ignorar
			log.Errorf("unknown query type: %d", queryType)
			break
		}
	}

	// Si no encontramos ninguna query válida
	if len(queryRowsMap) == 0 {
		return 0, nil, false, fmt.Errorf("no valid queries found")
	}

	return currentQueryNumber, queryRowsMap, queryEnded, nil
}
