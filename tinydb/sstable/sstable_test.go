package sstable

import (
	// "assert"
	"bytes"
	"fmt"
	"encoding/gob"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb"
	"os"
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


func TestCanOpenFileAndReadFirstKVPair(t *testing.T){
	f, _ := os.Open("test-data/sstable-i0hBPp2Z.data")
	var kv KeyVal
	decoder := gob.NewDecoder(f)

	f.Seek(0,0)
	err := decoder.Decode(&kv)
	if err != nil {
		t.Fatalf("%s", err)
	}

	fmt.Println("KV FROM TEST ", kv)

} 

func TestCanOpenFileAndReadSecondKVPair(t *testing.T){
	f, _ := os.Open("test-data/sstable-i0hBPp2Z.data")
	var kv KeyVal
	decoder := gob.NewDecoder(f)

	f.Seek(125,0)
	err := decoder.Decode(&kv)
	if err != nil {
		t.Fatalf("%s", err) // `gob: unknown type id or corrupted data`
	}

	fmt.Println("KV FROM TEST ", kv)

} 


func TestSstableWritesSparseIxAndCanSeek(t *testing.T) {
	rows := spawnRows(11)
	filename, _ := tinydb.GenerateFilename("./test-data/sstable")
	w := bytes.Buffer{}

	table, _ := NewSSTable(filename)

	for _, row := range rows {
		stmnt := tinydb.Statement{
			Stmnt:       "insert",
			RowToInsert: &row,
		}

		_ = table.ExecuteInsert(stmnt, &w)
	}

	// force persist
	table.Persist(&w)

	// this works because the byte offset is still 0
	_ = table.seek(1)
}

func TestSstable(t *testing.T) {
	rows := spawnRows(101)
	filename, _ := tinydb.GenerateFilename("./test-data/sstable")

	table, _ := NewSSTable(filename)
	w := bytes.Buffer{}

	for _, row := range rows {
		// fmt.Println(row) // check test is doing something
		stmnt := tinydb.Statement{
			Stmnt:       "insert",
			RowToInsert: &row,
		}

		_ = table.ExecuteInsert(stmnt, &w)
	}

	// note, persist is implicitly called since we appended 101 rows, we now try to seek from that file
	_ = table.seek(100)
}
