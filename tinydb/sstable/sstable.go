package sstable

import (
	"bytes"
	"fmt"
	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb"
	"io"
	// "encoding/gob"
)

type SSTable struct {
	tree        *redblacktree.Tree
	numRows     int
	memtableMax int
}

type Segment struct {
	byteArray []byte
	blockIdx  *redblacktree.Tree
	filename  string
}

type Block struct {
	data []keyVal
}

func NewSSTable() (*SSTable, error) {
	t := &SSTable{
		tree:        redblacktree.NewWithIntComparator(), // Q: other options?
		numRows:     0,
		memtableMax: 100,
	}

	return t, nil
}

func (t *SSTable) ExecuteInsert(statement tinydb.Statement, w io.Writer) error { // arbitrarily assigning for now
	if t.tree.Size() == t.memtableMax {
		t.Persist(w)
		// TODO: Clear tree? return error for now
		return fmt.Errorf("max table row count of %d exceeded", t.memtableMax)
	}

	row := *statement.RowToInsert

	// if key is new, increment numRows
	_, exists := t.tree.Get(row.Id)
	if !exists {
		t.numRows++
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
	for iterator.Next() {
		v := iterator.Value()
		fmt.Printf("%v\n", v)
	}

	return nil
}

type keyVal struct {
	key int
	val tinydb.Row
}

func (t *SSTable) Persist(w io.Writer) error {
	var b bytes.Buffer
	//encoder := gob.NewEncoder(&b)
	//err := encoder.Encode()

	iterator := t.tree.Iterator()
	i := 0
	for iterator.Next() {
		k, v := iterator.Key(), iterator.Value()

		//val := tinydb.Row(v)
		//
		//fmt.Fprintf(&b, v.(tinydb.Row))
		fmt.Println(k, v)
		i++
	}

	fmt.Printf("our bytes: %s\n", b.String())
	return nil
}
