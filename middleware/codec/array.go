package codec

func ArrayEncode[T any](value []T, encoder func(T) ([]byte, error)) ([]byte, error) {
	lenBytes, err := Uint64Encode(uint64(len(value)))
	if err != nil {
		return nil, err
	}

	dataBytes := make([]byte, 0)
	for _, v := range value {
		data, err := encoder(v)
		if err != nil {
			return nil, err
		}
		dataBytes = append(dataBytes, data...)
	}
	return append(lenBytes, dataBytes...), nil
}
