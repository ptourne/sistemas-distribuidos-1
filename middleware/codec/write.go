package codec

import "io"

// DoWrite writes the value of the given length to the writer using the given encoder function.
// It prevents short writes by writing the value in a loop until the full length is written.
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
