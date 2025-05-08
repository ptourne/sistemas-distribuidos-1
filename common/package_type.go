package common

import (

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
	val := uint8(data[0])
	packageFile := &PackageFile{}
	packageFile.PackageType = TypePackage(val)
	var bufNul model.FileChunk
	buf, err := bufNul.Decode(data[1:])
	if err != nil {
		return nil, err
	}
	packageFile.Buf = *buf
	return packageFile, nil
}
