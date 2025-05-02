package codec

type Serializable interface {
	// Encode encodes the object into a byte slice.
	Encode() ([]byte, error)
	// Decode decodes the byte slice into the object.
	Decode(data []byte) error
}
