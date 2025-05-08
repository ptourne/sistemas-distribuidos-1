package model

import (
	"encoding/binary"
	"fmt"
)

type Rating struct {
	Id     uint32
	Rating uint8
}

func (r *Rating) Encode() ([]byte, error) {
	buf := make([]byte, 5)
	binary.BigEndian.PutUint32(buf, r.Id)
	buf[4] = r.Rating
	return buf, nil
}

func (r *Rating) Decode(data []byte) (*Rating, error) {
	var rating Rating
	if len(data) < 5 {
		return nil, fmt.Errorf("data length is less than 5")
	}
	rating.Id = binary.BigEndian.Uint32(data[:4])
	rating.Rating = data[4]
	return &rating, nil
}
