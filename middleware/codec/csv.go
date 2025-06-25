package codec

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
)

func CsvRecordEncode(read []string) ([]byte, error) {
	var csvBuf bytes.Buffer
	csvWriter := csv.NewWriter(&csvBuf)
	if err := csvWriter.Write(read); err != nil {
		return nil, fmt.Errorf("error escribiendo csv: %w", err)
	}
	csvWriter.Flush()
	if err := csvWriter.Error(); err != nil {
		return nil, fmt.Errorf("error finalizando csv: %w", err)
	}
	data := csvBuf.Bytes()
	dataLen := len(data)
	lenBytes, err := Uint64Encode(uint64(dataLen))
	if err != nil {
		return nil, fmt.Errorf("failed to encode string length: %w", err)
	}
	buf := make([]byte, len(lenBytes)+dataLen)
	copy(buf, lenBytes)
	copy(buf[len(lenBytes):], data)
	return buf, nil
}

func CsvRecordDecode(r io.Reader) ([]string, error) {
	dataLen, err := Uint64Decode(r)
	if err != nil {
		return nil, fmt.Errorf("failed to decode string length:3 %w", err)
	}

	data, err := DoRead(dataLen, r)
	if err != nil {
		return nil, fmt.Errorf("failed to read string data: %w", err)
	}

	if dataLen == 1 {
		return []string{}, nil
	}

	// Parsear como línea CSV
	csvReader := csv.NewReader(bytes.NewReader(data))
	record, err := csvReader.Read()
	if err != nil {
		return nil, fmt.Errorf("error parseando csv: %w", err)
	}

	return record, nil
}
