package naivetable

import (
	"bytes"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb"

	"math/rand"
	"testing"
)

func generateRandomString() string {
	lengths := []int{6, 8, 8, 8, 10, 10, 10, 12, 12, 14, 18, 24, 30} // good enough ^.^
	length := lengths[rand.Intn(len(lengths))]

	// Generate a random string, consisting of ascii lowercase chars, of the determined length
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		panic(err)
	}
	for i := range bytes {
		bytes[i] = 'a' + (bytes[i] % 26)
	}
	return string(bytes)
}

// spawnRows generates a slice of rows of the specified amount, count.
// The string fields vary in length, according to their specified distribution
func spawnRows(count int) []tinydb.Row {
	rows := make([]tinydb.Row, count, count)

	for i, _ := range rows {
		rows[i].Id = i
		rows[i].Username = generateRandomString()
		rows[i].Email = generateRandomString()
	}

	return rows
}

// TestSpawnRows checks that the row-spawning process is behaving as expected, so it can be used
// in other tests
func TestSpawnRows(t *testing.T) {
	rows := spawnRows(10_000)

	for i, r := range rows {
		if i != r.Id {
			t.Error("unique, non-zero row.Ids expected")
			t.FailNow()
		}
		if r.Username == "" {
			t.Error("username is blank")
			t.FailNow()
		}
		if r.Email == "" {
			t.Error("email is blank")
			t.FailNow()
		}
	}
}

// TestNewNaiveTable tests the initialization of a NaiveTable resulting from calling NewNaiveTable.
// Two cases: 1) the file backing the table does not exist and must be created. 2) the file backing the table does
// exist, and simply needs to be opened.
func TestNewNaiveTable(t *testing.T) {
	// First, test initialization requiring file creation
	tablename := "testtable"
	filepath, err := tinydb.GenerateFilename("./test-data/naive-table")
	if err != nil {
		t.Errorf("could not create test filepath %s", err)
		t.FailNow()
	}

	table, err := NewNaiveTable(filepath, tablename)
	if table == nil {
		t.Error("expected non-nil table")
		t.FailNow()
	}

	// Next, test initialization reusing the same file, opening instead of creating it.
	table2, err := NewNaiveTable(filepath, tablename)
	if table2 == nil {
		t.Error("expected non-nil table")
		t.FailNow()
	}

	if table.file.Name() != table2.file.Name() {
		t.Error("expected two test tables to be backed by same file")
	}
}

func TestExecuteInsert(t *testing.T) {
	rows := spawnRows(1)
	tablename := "testtable"
	filepath, err := tinydb.GenerateFilename("./test-data/naive-table")
	if err != nil {
		t.Errorf("could not create test filepath %s", err)
		t.FailNow()
	}

	table, err := NewNaiveTable(filepath, tablename)
	statement := tinydb.Statement{
		Stmnt:       "insert",
		RowToInsert: &rows[0],
	}
	wout := &bytes.Buffer{}

	_ = table.ExecuteInsert(statement, wout)
}
