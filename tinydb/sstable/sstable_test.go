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
		rows[i].Id = i * 2
		rows[i].Username = fmt.Sprintf("%d-aaaaaaaaaaa", i)
		rows[i].Email = fmt.Sprintf("%d-bbbbbbbbbbbbbb", i)
	}

	return rows
}

// func shuffle(data []int) []int {
// 	r := rand.New(rand.NewSource(time.Now().UnixNano()))

// 	for n := len(data); n > 0; n-- {
// 		randIndex := r.Intn(n)
// 		data[n-1], data[randIndex] = data[randIndex], data[n-1]
// 	}

// 	return data
// }

// TestSpawnRows checks that the row-spawning process is behaving as expected, so it can be used
// in other tests
func TestSpawnRows(t *testing.T) {
	rows := spawnRows(100)

	for _, r := range rows {

		// if i != r.Id {
		// 	t.Error("unique, non-zero row.Ids expected")
		// 	t.FailNow()
		// }
		if r.Username == "" {
			t.Error("username is blank")
			t.FailNow()
		}
		if r.Email == "" {
			t.Error("email is blank")
			t.FailNow()
		}
	}

	// fmt.Println(rows)
}

func TestSstable(t *testing.T) {
	rows := spawnRows(100)

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
