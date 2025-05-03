package main

import "fmt"

// const idLength = 32

var i = 1

func GenerateRandomID() string {
	// numBytes := (idLength * 3) / 4

	// randomBytes := make([]byte, numBytes)
	// rand.Read(randomBytes)

	// hexEncoded := hex.EncodeToString(randomBytes)

	// return hexEncoded[:idLength]
	id := i
	i++
	return fmt.Sprintf("%d", id)
}
