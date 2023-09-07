package naivetable

import (
	"fmt"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb"
	"io"
	"encoding/gob"
	"log"
	"os"
)

// begin region: NaiveTable implementation
const ROWS_PER_PAGE = 3
const TABLE_PAGE_CAP = 10

type Page = [ROWS_PER_PAGE]*tinydb.Row

type Pager struct {
	filePointer *os.File
	pages       [TABLE_PAGE_CAP]*Page
	numRows     int
}

type NaiveTable struct {
	NumRows *int
	pager   *Pager
}
// end region: table structs

func NewNaiveTable(filename string) (NaiveTable, error) {
	p, err := pagerOpen(filename)
	t:= NaiveTable {}
	
	if err != nil {
		fmt.Println(err)
		return t, err
	}

	t.NumRows = &p.numRows
	t.pager = p
	
	return t, nil
}

func pagerOpen(filename string) (*Pager, error) {
	// open file
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}

	var pages [TABLE_PAGE_CAP]*Page

	p := &Pager{
		filePointer: file,
		pages:       pages,
		numRows:     0,
	}

	//TMP: write all data to page cache
	// currently the data is a row array
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
	p.pages[0] = &page

	fmt.Println("NaiveTable Loaded.")

	return p, nil
}

func (NaiveTable) ExecuteSelect(statement tinydb.Statement, w io.Writer) error {
	//if *table.NumRows == 0 {
	//	fmt.Fprintln(w, "No rows in this table")
	//}
	//for i := 0; i < *table.NumRows; i++ {
	//	fmt.Fprintf(w, "TODO: print row %d \n", i)
	//	// _ := int(math.Floor(float64(i) / float64(ROWS_PER_PAGE)))
	//	// _ := i % ROWS_PER_PAGE
	//
	//	// fmt.Fprintln(w, table.Pages[targetPage][pageIx])
	//}

	fmt.Fprintln(w, "NaiveTable.ExecuteSelect()")
	fmt.Fprintln(w, statement)

	return nil
}

func (NaiveTable) ExecuteInsert(statement tinydb.Statement, w io.Writer) error {
	fmt.Fprintln(w, "NaiveTable.ExecuteInsert()")
	fmt.Fprintln(w, statement)

	return nil
}
