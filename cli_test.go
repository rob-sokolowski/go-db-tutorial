package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
)

// generateFilename is used by tests to generate random filenames, to avoid collisions with previous test runs
// For example, if we reuse the same file over and over, our tests may be blind to cases where file creation is required
func generateFilename(baseName string) (string, error) {
	randBytes := make([]byte, 6) // 6 bytes will give us 8 characters in base64
	_, err := rand.Read(randBytes)
	if err != nil {
		return "", err
	}
	randString := base64.RawURLEncoding.EncodeToString(randBytes)

	newFileName := fmt.Sprintf("%s-%s.data", baseName, randString)
	return newFileName, nil
}

// TestCli tests high-level functionality of the CLI, in multiple steps:
// 1) Create fresh database using a randomly-generated filename, select, verify there's no data
// 2) Insert two rows, select again, .exit, verify output
// 3) Restart the CLI with the same file, the two rows should still be present
func TestCli(t *testing.T) {
	// test setup:
	filename, err := generateFilename("test-db")
	if err != nil {
		t.Fatalf("could not create test filename %s", err)
	}
	testdbPath := fmt.Sprintf("./test-data/%s", filename)

	// step 1
	reader := strings.NewReader("insert 1 rob rob@example.com\nselect\n.exit\n")
	out := bytes.Buffer{}

	Cli(reader, &out, testdbPath, "NaiveTable")

	want := "db > statement executed.\ndb > &{1 rob rob@example.com}\nstatement executed.\ndb > adios!\n"
	if out.String() != want {
		t.Errorf("unexpected output")
	}
}
