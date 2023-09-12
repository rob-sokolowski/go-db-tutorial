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

type Segment struct {
	byteArray []byte
	blockIdx *redblacktree.Tree
	filename string
}

type Block struct {
	data []keyVal
}

type keyVal struct {
	key int 
	val tinydb.Row 
}


func NewSSTable() (*SSTable, error) {
	t := &SSTable{
		tree:    redblacktree.NewWithIntComparator(), // Q: other options?
		numRows: 0,
	}

	return t, nil
}

func (t *SSTable) ExecuteInsert(statement tinydb.Statement, w io.Writer) error {
	maxRows := 100 // arbitrarily assigning for now

	if t.numRows == maxRows {
		// throw an error for now
		return fmt.Errorf("max table row count of %d exceeded", maxRows)
	}

	row := *statement.RowToInsert

	// if key is new, increment numRows
	_, exists := t.tree.Get(row.Id)
	if !exists {
		t.numRows = t.numRows + 1
	}

	t.tree.Put(row.Id, row)

	return nil
}

func (t *SSTable) ExecuteSelect(statement tinydb.Statement, w io.Writer) error {
	if t.numRows == 0 {
		fmt.Println("No rows in this table")
		return nil
	}

	iterator := t.tree.Iterator()
	for iterator.Next(){
		// k := iterator.Key()
		v := iterator.Value()
		fmt.Printf("%i, %v", k, v)
	}

	return nil
}

func (t *SSTable) Persist(w io.Writer) error {

	type Segment struct {
	byteArray []byte
	blockIdx *redblacktree.Tree
	filename string
}

	iterator := t.tree.Iterator()
	i := 0 
	for iterator.Next(){
		k := iterator.Key()
		v := iterator.Value()
		fmt.Printf("Write out to file %i, %v", k, v)
		i++
	}
return nil
}