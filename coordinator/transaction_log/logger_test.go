package transaction_log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestWriteAndReadLogFile_Basic(t *testing.T) {
	t.Run("Logfile", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "logfile")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		filename := "testfile"
		counter := uint64(42)
		read := []string{"a", "b", "c"}
		lastReadNotIncluided := []byte{1, 2, 3, 4}
		lastIdACK := uint64(99)

		err = WriteLogFile(tmpFile, filename, counter, read, lastReadNotIncluided, lastIdACK)
		assert.NoError(t, err)

		// Reabrir para lectura desde el principio
		tmpFile.Seek(0, 0)
		gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK, err := ReadLogFile(tmpFile)
		assert.NoError(t, err)
		assert.Equal(t, filename, gotFilename)
		assert.Equal(t, counter, gotCounter)
		assert.Equal(t, read, gotRead)
		assert.Equal(t, lastReadNotIncluided, gotLastReadNotIncluided)
		assert.Equal(t, lastIdACK, gotLastIdACK)
	})

	t.Run("LogfileEmpty", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "logfile_empty")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		filename := ""
		counter := uint64(0)
		read := []string{}
		lastReadNotIncluided := []byte{}
		lastIdACK := uint64(0)

		err = WriteLogFile(tmpFile, filename, counter, read, lastReadNotIncluided, lastIdACK)
		assert.NoError(t, err)

		tmpFile.Seek(0, 0)
		gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK, err := ReadLogFile(tmpFile)
		assert.NoError(t, err)
		assert.Equal(t, filename, gotFilename)
		assert.Equal(t, counter, gotCounter)
		assert.Equal(t, read, gotRead)
		assert.Equal(t, lastReadNotIncluided, gotLastReadNotIncluided)
		assert.Equal(t, lastIdACK, gotLastIdACK)
	})
	t.Run("LogfileCorrupt", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "logfile_corrupt")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		// Escribir datos inválidos
		tmpFile.Write([]byte{0x01, 0x02, 0x03})
		tmpFile.Seek(0, 0)

		_, _, _, _, _, err = ReadLogFile(tmpFile)
		assert.Error(t, err)
	})

	t.Run("LogfileLarge", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "logfile_large")
		assert.NoError(t, err)
		defer os.Remove(tmpFile.Name())
		defer tmpFile.Close()

		filename := "largefile"
		counter := uint64(123456789)
		read := make([]string, 1000)
		for i := range read {
			read[i] = "line"
		}
		lastReadNotIncluided := make([]byte, 4096)
		for i := range lastReadNotIncluided {
			lastReadNotIncluided[i] = byte(i % 256)
		}
		lastIdACK := uint64(987654321)

		err = WriteLogFile(tmpFile, filename, counter, read, lastReadNotIncluided, lastIdACK)
		assert.NoError(t, err)

		tmpFile.Seek(0, 0)
		gotFilename, gotCounter, gotRead, gotLastReadNotIncluided, gotLastIdACK, err := ReadLogFile(tmpFile)
		assert.NoError(t, err)
		assert.Equal(t, filename, gotFilename)
		assert.Equal(t, counter, gotCounter)
		assert.Equal(t, read, gotRead)
		assert.Equal(t, lastReadNotIncluided, gotLastReadNotIncluided)
		assert.Equal(t, lastIdACK, gotLastIdACK)
	})
}
