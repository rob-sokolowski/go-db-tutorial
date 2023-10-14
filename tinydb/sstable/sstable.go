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

// io.Writer is not getting used
func (t *SSTable) Persist(wsd io.Writer) error {
	// write memtable rows to disk:
	//    create file if not exists
	//    append to file if it does

	// clear memtable

	var b bytes.Buffer

	encoder := gob.NewEncoder(&b)

	iterator := t.tree.Iterator()
	i := 0
	sparseIxes := make([]SparseIxEntry, 0) // Q: Do we want to hard-code (len(SparseIxes) == memtableMax / ixSparsity - 1) ?
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

		kv := KeyVal{Key:k.(int), Val: v.(tinydb.Row) ,}
		err := encoder.Encode(kv)
		if err != nil {
			fmt.Println(err)
			return err
		}

		i++
	}


	// prints out indexes
	fmt.Println("SPARSE INDXS FROM WRITE: ", sparseIxes)

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

	// decode sparseIxOffset from buffer
	var sparseIxOffset uint16
	err = binary.Read(bytes.NewReader(buffer), binary.BigEndian, &sparseIxOffset)
	if err != nil {
		fmt.Println(err)
		return err
	}

	// write binary for sparseIdx to buffer 
	_, err = f.Seek(int64(sparseIxOffset), 0)
	if err != nil {
		fmt.Print("Could not seek idx")
		return err
	}

	i := offset - int64(sparseIxOffset)
	buffer2 := make([]byte, i)
	_, err = f.Read(buffer2)
	if err != nil {
		fmt.Println("failed to read sparseIdx")
		return err
	}

	// decode sparseIdx from buffer
	var sparseIxes []SparseIxEntry
	sparseIxDecoder := gob.NewDecoder(bytes.NewReader(buffer2))
	err = sparseIxDecoder.Decode(&sparseIxes)
	if err != nil {
		fmt.Println(err)
		return err
	}

	fmt.Println("SPARSE INDXS FROM SEEK: ", sparseIxes)

	// START SEEK LOGIC
	// get keyVal ByteOffset
	var targetOffset int64

	for _, e := range sparseIxes {
		if e.Key == targetKey {
			targetOffset = int64(e.ByteOffset)
			// fmt.Println("TARGET OFFSET: ", targetOffset)
		}
	}

	// read keyVal into byte array and decode
	_, err = f.Seek(targetOffset, 0)
	if err != nil {
		fmt.Print("Could not seek target")
		return err
	}

	size := 522 // TODO: set size to nextByteOffset - prevByteOffset
	kvbuffer := make([]byte, size) 
	_, err = f.Read(kvbuffer)
	if err != nil {
		fmt.Println("failed to read kvpairs")
		return err
	}

	kvDecoder := gob.NewDecoder(bytes.NewReader(kvbuffer))

	for i := 0; i < t.ixSparsity; i++ {

		var kv KeyVal

		// TODO: fix error `gob: unknown type id or corrupted data`
		if err := kvDecoder.Decode(&kv); err != nil {
			fmt.Println("Error decoding KeyVal:", err)
			break
		}
		fmt.Println("PING")

		if kv.Key == targetKey {
			fmt.Println("FOUND key: ", kv.Key) 
		} 


	}


	return nil
}
