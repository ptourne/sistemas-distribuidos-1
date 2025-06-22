package transaction_log

import (
	"os"
	"path"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTransactionLog(t *testing.T) {
	t.Run("TestSaveLogSafely", func(t *testing.T) {
		tmpDir := t.TempDir()
		logPath := filepath.Join(tmpDir, "logfile")
		filename := "safeFile"
		counter := uint64(10)
		read := []string{"x", "y"}
		lastReadNotIncluided := []byte{9, 8, 7}
		lastIdACK := uint64(1)

		err := SaveLogSafely(logPath, filename, counter, read, lastReadNotIncluided, lastIdACK)
		assert.NoError(t, err)

		file, err := os.Open(logPath)
		assert.NoError(t, err)
		defer file.Close()

		gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK, err := ReadLogFile(file)
		assert.NoError(t, err)
		assert.Equal(t, filename, gotFilename)
		assert.Equal(t, counter, gotCounter)
		assert.Equal(t, read, gotRead)
		assert.Equal(t, lastReadNotIncluided, gotLastReadNotIncluided)
		assert.Equal(t, lastIdACK, gotLastIdACK)
	})
	t.Run("TestNewTransactionLogForCidAndUpdate", func(t *testing.T) {
		tmpDir := t.TempDir()
		cid := uint64(55)
		tlog, err := NewTransactionLogForCid(tmpDir, cid)
		assert.NoError(t, err)
		assert.NotNil(t, tlog)

		// Test Update
		fileName := "file"
		counter := uint64(1)
		read := []string{"a"}
		lastReadNotIncluided := []byte{1}
		lastIdACK := uint64(2)

		err = tlog.Update(fileName, counter, read, lastReadNotIncluided, lastIdACK)
		assert.NoError(t, err)

		// Check if the log file was created
		logFilePath := path.Join(logDirectory(tmpDir), strconv.FormatUint(cid, 10), logFileName())
		_, err = os.Stat(logFilePath)
		assert.NoError(t, err)
	})
	t.Run("TestRecoverFromLogs", func(t *testing.T) {
		tmpDir := t.TempDir()
		cid := uint64(77)
		tlog, err := NewTransactionLogForCid(tmpDir, cid)
		assert.NoError(t, err)

		// Guardar log
		fileName := "recoverfile"
		counter := uint64(5)
		read := []string{"r1", "r2"}
		lastReadNotIncluided := []byte{5, 6}
		lastIdACK := uint64(7)

		err = tlog.Update(fileName, counter, read, lastReadNotIncluided, lastIdACK)
		assert.NoError(t, err)

		// Test RecoverFromLogs
		logs, err := RecoverFromLogs(tmpDir)
		assert.NoError(t, err)
		assert.NotEmpty(t, logs)
		assert.Len(t, logs, 1)

		recoveredLog := logs[0]
		cidR, filenameR, counterR, readR, lastReadNotIncluidedR, lastIdACKR, err := recoveredLog.Recover()
		assert.NoError(t, err)
		assert.Equal(t, cid, cidR)
		assert.Equal(t, fileName, filenameR)
		assert.Equal(t, counter, counterR)
		assert.Equal(t, read, readR)
		assert.Equal(t, lastReadNotIncluided, lastReadNotIncluidedR)
		assert.Equal(t, lastIdACK, lastIdACKR)
	})
}

// func TestReadLogFile_Corrupt(t *testing.T) {
// 	tmpDir := t.TempDir()
// 	cid := uint64(77)
// 	tlog, err := NewTransactionLogForCid(tmpDir, cid)
// 	assert.NoError(t, err)

// 	// Guardar log
// 	fileName := "recoverfile"
// 	counter := uint64(5)
// 	read := []string{"r1", "r2"}
// 	lastReadNotIncluided := []byte{5, 6}
// 	lastIdACK := uint64(7)

// 	err = tlog.Update(fileName, counter, read, lastReadNotIncluided, lastIdACK)
// 	assert.NoError(t, err)

// 	// Guardar log
// 	fileName2 := ""
// 	counter2 := uint64(5)
// 	read2 := []string{"r1", "r2"}
// 	lastReadNotIncluided2 := []byte{5, 6}
// 	lastIdACK2 := uint64(7)

// 	err = tlog.Update(fileName2, counter2, read2, lastReadNotIncluided2, lastIdACK2)
// 	assert.Error(t, err)

// 	// Test RecoverFromLogs
// 	logs, err := RecoverFromLogs(tmpDir)
// 	assert.NoError(t, err)
// 	assert.NotEmpty(t, logs)
// 	assert.Len(t, logs, 1)

// 	recoveredLog := logs[0]
// 	cidR, filenameR, counterR, readR, lastReadNotIncluidedR, lastIdACKR := recoveredLog.Recover()
// 	assert.Equal(t, cid, cidR)
// 	assert.Equal(t, fileName, filenameR)
// 	assert.Equal(t, counter, counterR)
// 	assert.Equal(t, read, readR)
// 	assert.Equal(t, lastReadNotIncluided, lastReadNotIncluidedR)
// 	assert.Equal(t, lastIdACK, lastIdACKR)
// }

func TestTransactionLogQueries(t *testing.T) {
	t.Run("Test writeRowQuery", func(t *testing.T) {
		tempDir := t.TempDir()
		cid := uint64(1)

		// Crear transaction log
		tl, err := NewTransactionLogForCid(tempDir, cid)
		require.NoError(t, err)
		require.NotNil(t, tl)

		log.Infof("WriteBeginQuery")
		err = tl.WriteBeginQuery(1)
		assert.NoError(t, err)
		queryNumber, rows, queryEnded, err := tl.ReadLastQueryRows()
		assert.NoError(t, err)
		assert.Equal(t, uint8(1), queryNumber)
		assert.Equal(t, []*model.Row{}, rows)
		assert.False(t, queryEnded)

		log.Infof("WriteRowQuery")
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"a": 1}})
		assert.NoError(t, err)
		queryNumber, rows, queryEnded, err = tl.ReadLastQueryRows()
		assert.NoError(t, err)
		assert.Equal(t, uint8(1), queryNumber)
		assert.Len(t, rows, 1)
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"a": 1}}, rows[0]))
		assert.False(t, queryEnded)

		log.Infof("WriteEndQuery")
		err = tl.WriteEndQuery()
		assert.NoError(t, err)
		queryNumber, rows, queryEnded, err = tl.ReadLastQueryRows()
		assert.NoError(t, err)
		assert.Equal(t, uint8(1), queryNumber)
		assert.Len(t, rows, 1)
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"a": 1}}, rows[0]))
		assert.True(t, queryEnded)
	})

	t.Run("Test complete block with END", func(t *testing.T) {
		tempDir := t.TempDir()
		cid := uint64(1)

		tl, err := NewTransactionLogForCid(tempDir, cid)
		require.NoError(t, err)

		// Escribir un bloque completo
		err = tl.WriteBeginQuery(1)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"a": 1}})
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"b": 2}})
		assert.NoError(t, err)
		err = tl.WriteEndQuery()
		assert.NoError(t, err)

		// Leer el último bloque
		queryNumber, rows, queryEnded, err := tl.ReadLastQueryRows()
		assert.NoError(t, err)
		assert.Equal(t, uint8(1), queryNumber)
		assert.Len(t, rows, 2)
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"a": 1}}, rows[0]))
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"b": 2}}, rows[1]))
		assert.True(t, queryEnded)
	})

	t.Run("Test incomplete block without END", func(t *testing.T) {
		tempDir := t.TempDir()
		cid := uint64(1)

		tl, err := NewTransactionLogForCid(tempDir, cid)
		require.NoError(t, err)

		// Escribir un bloque incompleto
		err = tl.WriteBeginQuery(2)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"c": 3}})
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"d": 4}})
		assert.NoError(t, err)
		// Sin WriteEndQuery

		// Leer el último bloque
		queryNumber, rows, queryEnded, err := tl.ReadLastQueryRows()
		assert.NoError(t, err)
		assert.Equal(t, uint8(2), queryNumber)
		assert.Len(t, rows, 2)
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"c": 3}}, rows[0]))
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"d": 4}}, rows[1]))
		assert.False(t, queryEnded)
	})

	t.Run("Test multiple blocks - keep last one", func(t *testing.T) {
		tempDir := t.TempDir()
		cid := uint64(1)

		tl, err := NewTransactionLogForCid(tempDir, cid)
		require.NoError(t, err)

		// Primer bloque
		err = tl.WriteBeginQuery(1)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"a": 1}})
		assert.NoError(t, err)
		err = tl.WriteEndQuery()
		assert.NoError(t, err)

		// Segundo bloque
		err = tl.WriteBeginQuery(2)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"b": 2}})
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"c": 3}})
		assert.NoError(t, err)
		err = tl.WriteEndQuery()
		assert.NoError(t, err)

		// Tercer bloque (incompleto)
		err = tl.WriteBeginQuery(3)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"d": 4}})
		assert.NoError(t, err)
		// Sin END

		// Leer el último bloque
		queryNumber, rows, queryEnded, err := tl.ReadLastQueryRows()
		assert.NoError(t, err)
		assert.Equal(t, uint8(3), queryNumber)
		assert.Len(t, rows, 1)
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"d": 4}}, rows[0]))
		assert.False(t, queryEnded)
	})

	t.Run("Test complex row with all fields", func(t *testing.T) {
		tempDir := t.TempDir()
		cid := uint64(1)

		tl, err := NewTransactionLogForCid(tempDir, cid)
		require.NoError(t, err)

		complexRow := &model.Row{
			Type:     model.QueryRow,
			Numerics: map[string]uint64{"id": 123, "count": 456},
			Strings:  map[string]string{"name": "test", "type": "movie"},
			Arrays:   map[string][]string{"genres": {"action", "drama"}},
			Floats:   map[string]float64{"rating": 4.5, "score": 8.7},
		}

		err = tl.WriteBeginQuery(1)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(complexRow)
		assert.NoError(t, err)
		err = tl.WriteEndQuery()
		assert.NoError(t, err)

		queryNumber, rows, queryEnded, err := tl.ReadLastQueryRows()
		assert.NoError(t, err)
		assert.Equal(t, uint8(1), queryNumber)
		assert.Len(t, rows, 1)
		assert.True(t, model.EqualsRows(complexRow, rows[0]))
		assert.True(t, queryEnded)
	})

	t.Run("Test empty file", func(t *testing.T) {
		tempDir := t.TempDir()
		cid := uint64(1)

		tl, err := NewTransactionLogForCid(tempDir, cid)
		require.NoError(t, err)

		_, _, _, err = tl.ReadLastQueryRows()
		assert.Error(t, err)
	})

	t.Run("Test BEGIN without rows", func(t *testing.T) {
		tempDir := t.TempDir()
		cid := uint64(1)

		tl, err := NewTransactionLogForCid(tempDir, cid)
		require.NoError(t, err)

		err = tl.WriteBeginQuery(1)
		assert.NoError(t, err)
		err = tl.WriteEndQuery()
		assert.NoError(t, err)

		queryNumber, rows, queryEnded, err := tl.ReadLastQueryRows()
		assert.NoError(t, err)
		assert.Equal(t, uint8(1), queryNumber)
		assert.Empty(t, rows)
		assert.True(t, queryEnded)
	})

	t.Run("Test file cleanup after ReadLastQueryRows", func(t *testing.T) {
		tempDir := t.TempDir()
		cid := uint64(1)

		tl, err := NewTransactionLogForCid(tempDir, cid)
		require.NoError(t, err)

		// Escribir múltiples bloques
		err = tl.WriteBeginQuery(1)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"a": 1}})
		assert.NoError(t, err)
		err = tl.WriteEndQuery()
		assert.NoError(t, err)

		err = tl.WriteBeginQuery(2)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"b": 2}})
		assert.NoError(t, err)
		// Sin END

		// Leer el último bloque
		queryNumber, rows, queryEnded, err := tl.ReadLastQueryRows()
		assert.NoError(t, err)
		assert.Equal(t, uint8(2), queryNumber)
		assert.Len(t, rows, 1)
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"b": 2}}, rows[0]))
		assert.False(t, queryEnded)
	})

}

func TestReadQueriesRows(t *testing.T) {
	t.Run("Test single complete query", func(t *testing.T) {
		tempDir := t.TempDir()
		cid := uint64(1)

		tl, err := NewTransactionLogForCid(tempDir, cid)
		require.NoError(t, err)

		// Escribir una query completa
		err = tl.WriteBeginQuery(1)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"a": 1}})
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"b": 2}})
		assert.NoError(t, err)
		err = tl.WriteEndQuery()
		assert.NoError(t, err)

		// Abrir el archivo y leer todas las queries
		file, err := os.Open(tl.(*transactionLog).queryFileName)
		assert.NoError(t, err)
		defer file.Close()

		currentQueryNumber, queryRowsMap, queryEnded, err := ReadQueriesRows(file)
		assert.NoError(t, err)
		assert.Equal(t, uint8(1), currentQueryNumber)
		assert.Len(t, queryRowsMap, 1)
		assert.True(t, queryEnded)

		// Verificar que la query 1 tiene las rows correctas
		rows, exists := queryRowsMap[1]
		assert.True(t, exists)
		assert.Len(t, rows, 2)
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"a": 1}}, rows[0]))
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"b": 2}}, rows[1]))
	})

	t.Run("Test multiple complete queries", func(t *testing.T) {
		tempDir := t.TempDir()
		cid := uint64(1)

		tl, err := NewTransactionLogForCid(tempDir, cid)
		require.NoError(t, err)

		// Primera query
		err = tl.WriteBeginQuery(1)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"a": 1}})
		assert.NoError(t, err)
		err = tl.WriteEndQuery()
		assert.NoError(t, err)

		// Segunda query
		err = tl.WriteBeginQuery(2)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"b": 2}})
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"c": 3}})
		assert.NoError(t, err)
		err = tl.WriteEndQuery()
		assert.NoError(t, err)

		// Tercera query
		err = tl.WriteBeginQuery(3)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"d": 4}})
		assert.NoError(t, err)
		err = tl.WriteEndQuery()
		assert.NoError(t, err)

		// Abrir el archivo y leer todas las queries
		file, err := os.Open(tl.(*transactionLog).queryFileName)
		assert.NoError(t, err)
		defer file.Close()

		currentQueryNumber, queryRowsMap, queryEnded, err := ReadQueriesRows(file)
		assert.NoError(t, err)
		assert.Equal(t, uint8(3), currentQueryNumber) // Última query procesada
		assert.Len(t, queryRowsMap, 3)
		assert.True(t, queryEnded)

		// Verificar query 1
		rows1, exists := queryRowsMap[1]
		assert.True(t, exists)
		assert.Len(t, rows1, 1)
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"a": 1}}, rows1[0]))

		// Verificar query 2
		rows2, exists := queryRowsMap[2]
		assert.True(t, exists)
		assert.Len(t, rows2, 2)
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"b": 2}}, rows2[0]))
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"c": 3}}, rows2[1]))

		// Verificar query 3
		rows3, exists := queryRowsMap[3]
		assert.True(t, exists)
		assert.Len(t, rows3, 1)
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"d": 4}}, rows3[0]))
	})

	t.Run("Test incomplete query at end", func(t *testing.T) {
		tempDir := t.TempDir()
		cid := uint64(1)

		tl, err := NewTransactionLogForCid(tempDir, cid)
		require.NoError(t, err)

		// Query completa
		err = tl.WriteBeginQuery(1)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"a": 1}})
		assert.NoError(t, err)
		err = tl.WriteEndQuery()
		assert.NoError(t, err)

		// Query incompleta
		err = tl.WriteBeginQuery(2)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"b": 2}})
		assert.NoError(t, err)
		// Sin END

		// Abrir el archivo y leer todas las queries
		file, err := os.Open(tl.(*transactionLog).queryFileName)
		assert.NoError(t, err)
		defer file.Close()

		currentQueryNumber, queryRowsMap, queryEnded, err := ReadQueriesRows(file)
		assert.NoError(t, err)
		assert.Equal(t, uint8(2), currentQueryNumber)
		assert.Len(t, queryRowsMap, 2)
		assert.False(t, queryEnded) // La última query no terminó

		// Verificar query 1
		rows1, exists := queryRowsMap[1]
		assert.True(t, exists)
		assert.Len(t, rows1, 1)
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"a": 1}}, rows1[0]))

		// Verificar query 2
		rows2, exists := queryRowsMap[2]
		assert.True(t, exists)
		assert.Len(t, rows2, 1)
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"b": 2}}, rows2[0]))
	})

	t.Run("Test empty file", func(t *testing.T) {
		tempDir := t.TempDir()
		cid := uint64(1)

		tl, err := NewTransactionLogForCid(tempDir, cid)
		require.NoError(t, err)

		// Crear archivo vacío
		file, err := os.Create(tl.(*transactionLog).queryFileName)
		assert.NoError(t, err)
		file.Close()

		// Intentar leer archivo vacío
		file, err = os.Open(tl.(*transactionLog).queryFileName)
		assert.NoError(t, err)
		defer file.Close()

		_, _, _, err = ReadQueriesRows(file)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "no valid queries found")
	})

	t.Run("Test query without rows", func(t *testing.T) {
		tempDir := t.TempDir()
		cid := uint64(1)

		tl, err := NewTransactionLogForCid(tempDir, cid)
		require.NoError(t, err)

		// Query sin rows
		err = tl.WriteBeginQuery(1)
		assert.NoError(t, err)
		err = tl.WriteEndQuery()
		assert.NoError(t, err)

		// Abrir el archivo y leer
		file, err := os.Open(tl.(*transactionLog).queryFileName)
		assert.NoError(t, err)
		defer file.Close()

		currentQueryNumber, queryRowsMap, queryEnded, err := ReadQueriesRows(file)
		assert.NoError(t, err)
		assert.Equal(t, uint8(1), currentQueryNumber)
		assert.Len(t, queryRowsMap, 1)
		assert.True(t, queryEnded)

		// Verificar que la query 1 existe pero sin rows
		rows, exists := queryRowsMap[1]
		assert.True(t, exists)
		assert.Empty(t, rows)
	})

	t.Run("Test complex rows with all fields", func(t *testing.T) {
		tempDir := t.TempDir()
		cid := uint64(1)

		tl, err := NewTransactionLogForCid(tempDir, cid)
		require.NoError(t, err)

		complexRow1 := &model.Row{
			Type:     model.QueryRow,
			Numerics: map[string]uint64{"id": 123, "count": 456},
			Strings:  map[string]string{"name": "test1", "type": "movie"},
			Arrays:   map[string][]string{"genres": {"action", "drama"}},
			Floats:   map[string]float64{"rating": 4.5, "score": 8.7},
		}

		complexRow2 := &model.Row{
			Type:     model.QueryRow,
			Numerics: map[string]uint64{"id": 789, "count": 101},
			Strings:  map[string]string{"name": "test2", "type": "series"},
			Arrays:   map[string][]string{"genres": {"comedy", "romance"}},
			Floats:   map[string]float64{"rating": 3.8, "score": 7.2},
		}

		complexRow3 := &model.Row{
			Strings:  map[string]string{"country": "US"},
			Numerics: map[string]uint64{"budget_sum": 120153886644},
		}

		err = tl.WriteBeginQuery(1)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(complexRow1)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(complexRow2)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(complexRow3)
		assert.NoError(t, err)
		err = tl.WriteEndQuery()
		assert.NoError(t, err)

		// Abrir el archivo y leer
		file, err := os.Open(tl.(*transactionLog).queryFileName)
		assert.NoError(t, err)
		defer file.Close()

		currentQueryNumber, queryRowsMap, queryEnded, err := ReadQueriesRows(file)
		assert.NoError(t, err)
		assert.Equal(t, uint8(1), currentQueryNumber)
		assert.Len(t, queryRowsMap, 1)
		assert.True(t, queryEnded)

		// Verificar que las rows complejas se leyeron correctamente
		rows, exists := queryRowsMap[1]
		assert.True(t, exists)
		assert.Len(t, rows, 3)
		assert.True(t, model.EqualsRows(complexRow1, rows[0]))
		assert.True(t, model.EqualsRows(complexRow2, rows[1]))
		assert.True(t, model.EqualsRows(complexRow3, rows[2]))
	})

	t.Run("Test corrupted lines are ignored", func(t *testing.T) {
		tempDir := t.TempDir()
		cid := uint64(1)

		tl, err := NewTransactionLogForCid(tempDir, cid)
		require.NoError(t, err)

		// Escribir query válida
		err = tl.WriteBeginQuery(1)
		assert.NoError(t, err)
		err = tl.WriteRowQuery(&model.Row{Numerics: map[string]uint64{"a": 1}})
		assert.NoError(t, err)
		err = tl.WriteEndQuery()
		assert.NoError(t, err)

		// Agregar líneas corruptas manualmente
		file, err := os.OpenFile(tl.(*transactionLog).queryFileName, os.O_APPEND|os.O_WRONLY, 0644)
		assert.NoError(t, err)
		file.WriteString("corrupted line 1\n")
		file.WriteString("another corrupted line\n")
		file.Close()

		// Leer el archivo
		file, err = os.Open(tl.(*transactionLog).queryFileName)
		assert.NoError(t, err)
		defer file.Close()

		currentQueryNumber, queryRowsMap, queryEnded, err := ReadQueriesRows(file)
		assert.NoError(t, err)
		assert.Equal(t, uint8(1), currentQueryNumber)
		assert.Len(t, queryRowsMap, 1)
		assert.True(t, queryEnded)

		// Verificar que la query válida se leyó correctamente
		rows, exists := queryRowsMap[1]
		assert.True(t, exists)
		assert.Len(t, rows, 1)
		assert.True(t, model.EqualsRows(&model.Row{Numerics: map[string]uint64{"a": 1}}, rows[0]))
	})
}
