// This file houses utility functions with unit tests in mind. That is, shared code to assist setting up /
// tearing down test cases related to tinydb.
package tinydb

import (
	"encoding/base64"
	"fmt"
	"math/rand"
)

// GenerateFilename is used by tests to generate random filenames, to avoid collisions with previous test runs
// For example, if we reuse the same file over and over, our tests may be blind to cases where file creation is required
func GenerateFilename(baseName string) (string, error) {
	randBytes := make([]byte, 6) // 6 bytes will give us 8 characters in base64
	_, err := rand.Read(randBytes)
	if err != nil {
		return "", err
	}
	randString := base64.RawURLEncoding.EncodeToString(randBytes)

	newFileName := fmt.Sprintf("%s-%s.data", baseName, randString)
	return newFileName, nil
}
