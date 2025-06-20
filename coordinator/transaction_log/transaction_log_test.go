package transaction_log

import (
	"os"
	"path"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTransactionLog(t *testing.T) {
	t.Run("TestSaveLogSafely", func(t *testing.T) {
		tmpDir := t.TempDir()
		logPath := filepath.Join(tmpDir, "logfile")
		filename := "safeFile"
		counter := uint64(10)
		read := []string{"x", "y"}
		lastReadNotIncluided := []byte{9, 8, 7}
		lastIdACK := uint64(123)

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
		cidR, filenameR, counterR, readR, lastReadNotIncluidedR, lastIdACKR := recoveredLog.Recover()
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
