package main

// const idLength = 32

var i uint64 = 1

func GenerateRandomID() uint64 {
	// numBytes := (idLength * 3) / 4

	// randomBytes := make([]byte, numBytes)
	// rand.Read(randomBytes)

	// hexEncoded := hex.EncodeToString(randomBytes)

	// return hexEncoded[:idLength]
	id := i
	i++
	return id
}
