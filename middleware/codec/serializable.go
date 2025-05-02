package codec

type Serializable[T any] interface {
	// Encode encodes the object into a byte slice.
	Encode() ([]byte, error)
	// Decode decodes the byte slice into the object.
	Decode(data []byte) (T, error)
}
