package common

import (
	"bytes"

	// "github.com/ptourne/sistemas-distribuidos-1/common/logger"
	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/ptourne/sistemas-distribuidos-1/middleware/codec"
)

// var log = logger.NewConsoleLogger("PackageFile", logger.Debug)

type PackageFile struct {
	PackageType TypePackage
	Buf         model.FileChunk
}

func (p PackageFile) Encode() ([]byte, error) {
	t, err := codec.Uint8Encode(uint8(p.PackageType))
	if err != nil {
		return nil, err
	}
	buf, err := p.Buf.Encode()
	if err != nil {
		return nil, err
	}
	data := make([]byte, 0)
	data = append(data, t...)
	data = append(data, buf...)
	return data, nil
}

func (p *PackageFile) Decode(data []byte) (*PackageFile, error) {
	r := bytes.NewReader(data)
	val, err := codec.Uint8Decode(r)
	if err != nil {
		return nil, err
	}
	// log.Infof("PackageFile Decode: %d", val)
	// log.Infof("PackageFile Decode TYPE: %d", TypePackage(val))

	packageFile := &PackageFile{}
	packageFile.PackageType = TypePackage(val)
	var bufNul model.FileChunk
	buf, err := bufNul.DecodeReader(r)
	if err != nil {
		return nil, err
	}
	packageFile.Buf = *buf
	return packageFile, nil
}
