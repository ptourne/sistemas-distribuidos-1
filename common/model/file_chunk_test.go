package model

import (
	"testing"
	"time"
)

func TestFileChunkSpeed(t *testing.T) {
	const count = 100000

	newFunction(t, count, 8)
	newFunction(t, count, 1024)
	newFunction(t, count, 1024*10)
	// newFunction(t, count, 1024*1024)

	t.Fail()

}

func newFunction(t *testing.T, count uint, size uint) {
	bigChunk := make([]byte, size) // 10 MB
	fileChunkFast := FileChunk{Bytes: bigChunk}
	startFast := time.Now()
	for range count {
		data, _ := fileChunkFast.Encode()
		var nul FileChunk
		_, _ = nul.Decode(data)
	}
	elapsedFast := time.Since(startFast)
	t.Logf("FileChunk Encode/Decode took %dms", elapsedFast.Microseconds())

	startSlow := time.Now()
	for range count {
		data, _ := fileChunkFast.Encode()
		var nul SlowFileChunk
		_, _ = nul.Decode(data)
	}
	elapsedSlow := time.Since(startSlow)
	t.Logf("FileChunk Encode/Decode took %dms", elapsedSlow.Microseconds())
	ratio := elapsedSlow.Microseconds() / elapsedFast.Microseconds()
	t.Logf("FileChunk is %d times slower than FileChunk", ratio)
}
