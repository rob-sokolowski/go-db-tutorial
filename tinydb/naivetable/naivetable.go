package naivetable

import (
	"encoding/gob"
	"fmt"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb"
	"io"
	"log"
	"math"
	"os"
)

const ROWS_PER_PAGE = 3
const TABLE_PAGE_CAP = 10

type Page = [ROWS_PER_PAGE]*tinydb.Row

type Pager struct {
	filePointer *os.File
	pages       [TABLE_PAGE_CAP]Page
	numRows     int
}

type NaiveTable struct {
	numRows *int
	pager   *Pager
}

func NewNaiveTable(filename string) (*NaiveTable, error) {
	p, err := pagerOpen(filename)
	t := &NaiveTable{
		numRows: &p.numRows,
		pager:   p,
	}

	if err != nil {
		fmt.Println(err)
		return t, err
	}

	return t, nil
}

func pagerOpen(filename string) (*Pager, error) {
	// open file
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	var pages [TABLE_PAGE_CAP]Page

	p := &Pager{
		filePointer: file,
		pages:       pages,
		numRows:     0,
	}

	// TMP: write all data to page cache
	var page Page
	var decodedRows []*tinydb.Row

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&decodedRows)

	if err != nil {
		fmt.Println("Decoding Error:", err)
		return p, nil
	}
	for i := 0; i < len(decodedRows); i++ {
		page[i] = decodedRows[i]
	}
	p.numRows = len(decodedRows)

	// we need the data to be pages
	p.pages[0] = page

	fmt.Println("NaiveTable Loaded.")

	return p, nil
}

func (t NaiveTable) ExecuteSelect(statement tinydb.Statement, w io.Writer) error {
	if *t.numRows == 0 {
		fmt.Fprintln(w, "No rows in this table")
	}
	for i := 0; i < *t.numRows; i++ {
		fmt.Fprintf(w, "TODO: print row %d \n", i)
		// _ := int(math.Floor(float64(i) / float64(ROWS_PER_PAGE)))
		// _ := i % ROWS_PER_PAGE

		// fmt.Fprintln(w, t.Pages[targetPage][pageIx])
	}

	return nil
}

func (t NaiveTable) appendRow(row *tinydb.Row) error {
	targetPage := int(math.Floor(float64(*t.numRows) / float64(ROWS_PER_PAGE)))
	pageIx := *t.numRows % ROWS_PER_PAGE

	// TODO: nil pointer exception is being thrown here, debug!
	t.pager.pages[targetPage][pageIx] = row

	*t.numRows += 1
	return nil
}

func (t NaiveTable) ExecuteInsert(statement tinydb.Statement, w io.Writer) error {
	maxRows := TABLE_PAGE_CAP * ROWS_PER_PAGE

	if *t.numRows == maxRows {
		return fmt.Errorf("max table row count of %d exceeded", maxRows)
	}

	t.appendRow(statement.RowToInsert)

	return nil
}

type BTree struct {
	Root *Node
}

type nodeKind = const (
	ref = iota
	val
)

type ref_ = string


type Node struct {
	kind nodeKind
	ref *ref_
	val *tinydb.Row
	Children []*Node
}


// initially, as we fill the tree, it consists of a single node containing nodes of val kind
// we keep everything in this node, until an insert would result in the root node being greater than
// on page (4KB). At this moment, we split the tree, the result is a total of 3 nodes. The root containing nodes
// of kind ref, and the two children containing nodes of kind val
