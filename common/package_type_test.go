package common

import (
	"testing"

	"github.com/ptourne/sistemas-distribuidos-1/common/model"
	"github.com/stretchr/testify/assert"
)

func TestPackageFileCodec(t *testing.T) {
	// Test encoding and decoding of PackageFile
	// Create a sample PackageFile
	packageFile := PackageFile{
		PackageType: FileName,
		Buf: model.FileChunk{
			Bytes: []byte("movies_metadata.csv"),
		},
	}
	// Encode the PackageFile
	encoded, err := packageFile.Encode()
	assert.NoError(t, err, "PackageFile encode failed: %v", err)

	// Decode the PackageFile
	var nul *PackageFile
	decodedPackageFile, err := nul.Decode(encoded)
	assert.NoError(t, err, "PackageFile decode failed: %v", err)

	// Check if the decoded PackageFile matches the original
	assert.Equal(t, packageFile, *decodedPackageFile, "Decoded PackageFile does not match original")
}
