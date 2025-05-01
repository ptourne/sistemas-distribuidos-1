package codec

import "io"

// DoRead reads the value of the given length to the reader using the given decoder function.
// It prevents short reads by reading in a loop until the full length is filled.
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
