package sstable

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/emirpasic/gods/trees/redblacktree"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb"
	"io"
	"os"
	// "reflect"
)

type SSTable struct {
	tree        *redblacktree.Tree
	numRows     int
	memtableMax int
	ixSparsity  int
	filename    string
}

type KeyVal struct {
	Key int
	Val tinydb.Row
}

type SparseIxEntry struct {
	Key        int
	ByteOffset int
}

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

// TODO: io.Writer is not getting used - can we remove?
func (t *SSTable) Persist(w io.Writer) error {
	// write memtable rows to disk:
	//    create file if not exists
	//    append to file if it does

	// TODO: clear memtable

	var b bytes.Buffer

	encoder := gob.NewEncoder(&b)

	iterator := t.tree.Iterator()
	i := 0
	sparseIxes := make([]SparseIxEntry, 0) 
	for iterator.Next() {
		k, v := iterator.Key(), iterator.Value()

		// construct sparseIx
		if i%t.ixSparsity == 0 && i != 0 {
			ix := SparseIxEntry{
				Key:        k.(int),
				ByteOffset: b.Len(),
			}
			sparseIxes = append(sparseIxes, ix)
		}
		// fmt.Printf("at index %d offset is %v \n", i, b.Len())

		kv := KeyVal{Key:k.(int), Val: v.(tinydb.Row) ,}
		err := encoder.Encode(kv)
		if err != nil {
			fmt.Println(err)
			return err
		}
		i++
	}

	// prints out indexes
	//fmt.Println("SPARSE INDXS FROM WRITE: ", sparseIxes)

	sparseIxesOffset := b.Len()

	_ = encoder.Encode(sparseIxes)
	_ = encoder.Encode(uint16(sparseIxesOffset))


	err := os.WriteFile(t.filename, b.Bytes(), 0666)
	if err != nil {
		return err
	}

	return nil
}


func (t *SSTable) seek(targetKey int) error {
	// open file
	f, err := os.Open(t.filename)
	if err != nil {
		return err
	}

	defer f.Close()

	fInfo, err := f.Stat()
	if err != nil {
		return err
	}
	if fInfo.Size() < 2 {
		return fmt.Errorf("file too small")
	}

	// write last two bytes of file to buffer
	buffer := make([]byte, 2)
	offset := fInfo.Size() - 2
	_, err = f.ReadAt(buffer, offset)	
	if err != nil {
		fmt.Println(err)
		return err
	}

	//decode sparseIxOffset from buffer
	var sparseIxOffset uint16
	err = binary.Read(bytes.NewReader(buffer), binary.BigEndian, &sparseIxOffset)
	if err != nil {
		fmt.Println(err)
		return err
	}
	fmt.Println("sparseIxOffset", sparseIxOffset)


	// seek sparseIdx start 
	_, err = f.Seek(int64(sparseIxOffset), 0)
	if err != nil {
		fmt.Print("Could not seek idx")
		return err
	}

	// decode sparseIdx from file
	var sparseIxes []SparseIxEntry
	sparseIxDecoder := gob.NewDecoder(f)
	err = sparseIxDecoder.Decode(&sparseIxes)
	if err != nil {
		fmt.Println(err)
		return err
	}
	
	fmt.Println("SPARSE INDXS FROM SEEK: ", sparseIxes)

	// START SEEK LOGIC

	// TODO: The gob decoder needs to read the type definition for KeyVal 
	// stored at the head of the file. If we `seek` to a byte offset without 
	// first reading the header, it will throw `gob: unknown type id or corrupted data`  
	// OPTIONS:
	// 1. Encode a header at the front of each 10-entry block
	// 2. Switch to a different encoder, like protobuffs
	// 3. (most hacky) Always decode the first kv pair; _then_ call f.Seek()  


	// get keyVal ByteOffset
	var targetKVOffset int64

	for _, e := range sparseIxes {
		if e.Key == targetKey {
			targetKVOffset = int64(e.ByteOffset)
			fmt.Println("TARGET OFFSET: ", targetKVOffset)
		}
	}

	// seek to start of KV pair
	_, err = f.Seek(targetKVOffset, 0)
	if err != nil {
		fmt.Print("Could not seek target")
		return err
	}

	kvDecoder := gob.NewDecoder(f)

	for i := 0; i < t.ixSparsity; i++ {

		var kv KeyVal

		if err := kvDecoder.Decode(&kv); err != nil {
			fmt.Println("Error decoding KeyVal:", err)
			return err
		}
		// fmt.Println("Key Val", kv)

		if kv.Key == targetKey {
			fmt.Println("FOUND key: ", kv.Key) 
			return nil
		} 
	}

	return nil
}
