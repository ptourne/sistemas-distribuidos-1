package common

import (
	"encoding/binary"
	"io"
)

type Film struct {
	Title     string
	Countries []string // 2-letter country codes
}

func DoRead(
	l uint64,
	r io.Reader,
) ([]byte, error) {
	p := make([]byte, l)
	read := 0
	for read < int(l) {
		n, err := r.Read(p[read:])
		if err != nil {
			return nil, err
		}
		read += n
	}
	return p, nil
}

func DoWrite(
	b []byte,
	w io.Writer,
) error {
	l := len(b)
	written := 0
	for written < l {
		n, err := w.Write(b[written:])
		if err != nil {
			return err
		}
		written += n
	}
	return nil
}

func EncodeUint64(w io.Writer, u uint64) error {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, u)
	return DoWrite(
		b,
		w,
	)
}

func DecodeUint64(r io.Reader) (uint64, error) {
	res, err := DoRead(8, r)
	if err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(res), err
}

func EncodeString(w io.Writer, s string) error {
	err := EncodeUint64(w, uint64(len(s)))
	if err != nil {
		return err
	}
	return DoWrite([]byte(s), w)
}

func DecodeString(r io.Reader) (string, error) {
	len, err := DecodeUint64(r)
	if err != nil {
		return "", err
	}
	strB, err := DoRead(len, r)
	return string(strB), err
}

func (f *Film) Encode(w io.Writer) error {
	err := EncodeString(w, f.Title)
	if err != nil {
		return err
	}
	err = EncodeUint64(w, uint64(len(f.Countries)))
	if err != nil {
		return err
	}
	for _, country := range f.Countries {
		err = EncodeString(w, country)
		if err != nil {
			return err
		}
	}
	return nil
}

func (f *Film) Decode(r io.Reader) error {
	title, err := DecodeString(r)
	if err != nil {
		return err
	}
	f.Title = title

	len, err := DecodeUint64(r)
	if err != nil {
		return err
	}
	f.Countries = make([]string, len)
	for i := range f.Countries {
		f.Countries[i], err = DecodeString(r)
		if err != nil {
			return err
		}
	}
	return nil
}
