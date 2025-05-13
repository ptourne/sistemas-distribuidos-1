package model

import (
	"fmt"
	"testing"
	"time"
)

type Measurement struct {
	Bytes uint
	Fast  int64
	Slow  int64
}

func TestFileChunkSpeed(t *testing.T) {
	const count = 100000

	measurements := make([]Measurement, 3)
	measurements[0] = testFileChunkCodec(t, count, 8)
	measurements[1] = testFileChunkCodec(t, count, 1024)
	measurements[2] = testFileChunkCodec(t, count, 1024*10)
	output := `
| Bytes | Fast (μs) | Slow (μs) | Ratio  |
|-------|-----------|-----------|--------|`
	for _, m := range measurements {
		ratio := float64(m.Slow) / float64(m.Fast)
		output += fmt.Sprintf("\n| %5d | %9d | %9d | %5.2f |", m.Bytes, m.Fast, m.Slow, ratio)
	}
	t.Log(output)
	t.Fail()
}

func testFileChunkCodec(t *testing.T, count uint, size uint) Measurement {
	bigChunk := make([]byte, size) // 10 MB
	fileChunkFast := FileChunk{Bytes: bigChunk}
	startFast := time.Now()
	for range count {
		data, _ := fileChunkFast.Encode()
		var nul FileChunk
		_, _ = nul.Decode(data)
	}
	elapsedFast := time.Since(startFast)

	startSlow := time.Now()
	for range count {
		data, _ := fileChunkFast.Encode()
		var nul SlowFileChunk
		_, _ = nul.Decode(data)
	}
	elapsedSlow := time.Since(startSlow)
	return Measurement{size, elapsedFast.Microseconds(), elapsedSlow.Microseconds()}
}
