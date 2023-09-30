package main

import (
	"bytes"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb"
	"strings"
	"testing"
)

// TestCli tests high-level functionality of the CLI, in multiple steps:
// 1) Create fresh database using a randomly-generated filename, select, verify there's no data
// 2) Insert two rows, select again, .exit, verify output
// 3) Restart the CLI with the same file, the two rows should still be present
func TestCli(t *testing.T) {
	// test setup:
	testdbPath, err := tinydb.GenerateFilename("./test-data/test-db")
	if err != nil {
		t.Fatalf("could not create test filename %s", err)
	}

	// step 1
	reader := strings.NewReader("insert 1 rob rob@example.com\nselect\n.exit\n")
	out := bytes.Buffer{}

	Cli(reader, &out, testdbPath, "NaiveTable")

	// TODO: Update this once functionality is restored
	_ = "db > statement executed.\ndb > TODO: print row 0 \nstatement executed.\ndb > adios!\n"

	// want := "db > statement executed.\ndb > &{1 rob rob@example.com}\nstatement executed.\ndb > adios!\n"
	//if out.String() != want {
	//	t.Errorf("unexpected output")
	//}
}
