package sstable

import (
	"fmt"
	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb"
	"io"
)

type SSTable struct {
	tree    *redblacktree.Tree
	numRows int
}

func NewSSTable() (*SSTable, error) {
	t := &SSTable{
		tree:    redblacktree.NewWithIntComparator(), // Q: other options?
		numRows: 0,
	}

	return t, nil
}

func (t SSTable) ExecuteInsert(statement tinydb.Statement, w io.Writer) error {
	maxRows := 1000 // arbitrarily assigning for now

	if t.numRows == maxRows {
		// throw an error for now
		return fmt.Errorf("max table row count of %d exceeded", maxRows)
	}

	row := *statement.RowToInsert

	// update row value and numRows
	_, exists := t.tree.Get(row.Id)
	if !exists {
		t.numRows++
	}

	t.tree.Put(row.Id, row)

	return nil
}

func (t SSTable) ExecuteSelect(statement tinydb.Statement, w io.Writer) error {
	if t.numRows == 0 {
		fmt.Println("No rows in this table")
	}

	rows := t.tree.Values()
	for _, row := range rows {
		fmt.Println(row)
	}

	return nil
}
