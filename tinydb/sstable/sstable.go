package sstable

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb"
	"io"
	"os"
	"reflect"
)

type SSTable struct {
	tree        *redblacktree.Tree
	numRows     int
	memtableMax int
	ixSparsity  int
	filename    string
}

type keyVal struct {
	key int
	val tinydb.Row
}

// type SsTable_ struct {
// 	Rows []tinydb.Row
// }

type SparseIxEntry struct {
	Key        int
	ByteOffset int
}

// type SSTable struct {
// 	byteArray []byte
// 	blockIdx  []SparseIxEntry
// 	filename  string
// }

func NewSSTable(filename string) (*SSTable, error) {
	t := &SSTable{
		tree:        redblacktree.NewWithIntComparator(), 
		numRows:     0,
		memtableMax: 100,
		ixSparsity:  10,
		filename:    filename,
	}

	return t, nil
}

func (t *SSTable) ExecuteInsert(statement tinydb.Statement, w io.Writer) error { 
	if t.tree.Size() == t.memtableMax {
		t.Persist(w)
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


func (t *SSTable) Persist(w io.Writer) error {
	// write memtable rows to disk:
	//    create file if not exists
	//    append to file if it does
	//
	// clear memtable
	var b bytes.Buffer
	encoder := gob.NewEncoder(&b)

	iterator := t.tree.Iterator()
	i := 0
	sparseIxes := make([]SparseIxEntry, 0) // Q: Do we want to hard-code (len(SparseIxes) == memtableMax / ixSparsity - 1) ?
	for iterator.Next() {
		k, v := iterator.Key(), iterator.Value()
		if i % t.ixSparsity == 0 && i != 0 {
			// check that we are grabbing every ith key
			// fmt.Printf("Hello, %d, %d\n", i, k) 
			ix := SparseIxEntry{
				Key:        k.(int),
				ByteOffset: b.Len(),
			}
			sparseIxes = append(sparseIxes, ix)
		}

		val := v.(tinydb.Row) // cast val as Row

		// TODO: Errors???
		_ = encoder.Encode(k)
		_ = encoder.Encode(val)
		i++
	}

	// fmt.Println(b)
	fmt.Println(sparseIxes)

	// TODO: Encode&append sparseIxes, byteOffsetSparseIxes
	// len1 is already offset for sparseIxes - why not just save that address?
	sparseIxesOffset := b.Len()
	_ = encoder.Encode(sparseIxes)
	_ = encoder.Encode(sparseIxesOffset)
	
	// fmt.Println(b)
	// len2 := b.Len()
	// sparseIxesLen := (int32)(len2 - sparseIxesOffset)
	
	// _ = encoder.Encode(sparseIxesLen) 

	err := os.WriteFile(t.filename, b.Bytes(), 0666)
	if err != nil {
		return err
	}

	return nil
}

func (t *SSTable) seek() error {
	// open file
	f, _ := os.Open(t.filename)
	fmt.Println("PING1", f)
	// if err != nil {
	// 	return err
	// }
	// not reaching here
	fmt.Println("PING2", f)
	defer f.Close()

	b := new(bytes.Buffer) // a pointer to a buffer to hold sparseIx // data for seek to scan over
	// decoder := gob.NewDecoder(f)
	fmt.Println("PING3", b)
	// gets SparseIxOffset

	// TODO: there is no file, so there is no fileInfo 
	fInfo, _ := f.Stat()
	// if err != nil {
	// 	return err
	// }
	fmt.Println("PING4", fInfo)
	// jump to end of file, the last 4 bytes will tell us where to jump to next
	if fInfo.Size() < 4 {
		return fmt.Errorf("file too small")
	}
	byteOffsetForSpaseIx, _ := f.Seek(-5, io.SeekEnd)
	fmt.Println(byteOffsetForSpaseIx)
	fmt.Println(reflect.TypeOf(*b))
	// f.ReadAt(*b, byteOffsetForSpaseIx)

	//print buffer
	// fmt.Println(b)
	


	// _, err = f.Seek(-5, io.SeekEnd) // re-seek
	// // var val int32
	// // do you mean 
	// val, err := f.Seek(-5, io.SeekEnd) // re-seek
	// err = decoder.Decode(&val) // val doesn't have a val tho?
	// if err != nil {
	// 	return err
	// }

	// fmt.Println(val)

	return nil
}
