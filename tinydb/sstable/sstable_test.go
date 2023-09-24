package sstable

import (
	"bytes"
	"fmt"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb"
	"testing"
)

// spawnRows generates a slice of rows of the specified amount, count.
// The string fields vary in length, according to their specified distribution
func spawnRows(count int) []tinydb.Row {
	rows := make([]tinydb.Row, count, count)

	for i, _ := range rows {
		id := i * 2
		rows[i].Id = id // don't always have id = row number
		rows[i].Username = fmt.Sprintf("%d-aaaaaaaaaaa", id)
		rows[i].Email = fmt.Sprintf("%d-bbbbbbbbbbbbbb", id)
	}

	return rows
}

// TestSpawnRows checks that the row-spawning process is behaving as expected, so it can be used
// in other tests
func TestSpawnRows(t *testing.T) {
	rows := spawnRows(100)

	for _, r := range rows {
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

func TestSstable(t *testing.T) {
	rows := spawnRows(101)

	table, _ := NewSSTable()
	w := bytes.Buffer{}

	for _, row := range rows {
		stmnt := tinydb.Statement{
			Stmnt:       "insert",
			RowToInsert: &row,
		}

		_ = table.ExecuteInsert(stmnt, &w)
	}

	stmnt := tinydb.Statement{
		Stmnt: "select",
	}
	_ = table.ExecuteSelect(stmnt, &w)
}
