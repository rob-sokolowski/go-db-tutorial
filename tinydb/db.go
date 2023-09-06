package tinydb

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
)

// begin region: table structs
const USERNAME_MAX = 32
const EMAIL_MAX = 255
const ROWS_PER_PAGE = 3
const TABLE_PAGE_CAP = 10

type Row struct {
	Id       int
	Username string
	Email    string
}

type Page = [ROWS_PER_PAGE]*Row

type Statement struct {
	Stmnt       string
	RowToInsert *Row
}

type Table struct {
	NumRows *int
	pager   *Pager
}

type Pager struct {
	filePointer *os.File
	pages       [TABLE_PAGE_CAP]*Page
	numRows     int
}

// end region: table structs

func DbOpen(filename string) (*Table, error) {
	p, err := pagerOpen(filename)

	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	t := &Table{
		NumRows: &p.numRows,
		pager:   p,
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

	var pages [TABLE_PAGE_CAP]*Page

	p := &Pager{
		filePointer: file,
		pages:       pages,
		numRows:     0,
	}

	//TMP: write all data to page cache
	// currently the data is a row array
	var page Page
	var decodedRows []*Row

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&decodedRows)

	if err != nil {
		fmt.Println("Decoding Error:", err)
		return nil, err
	}
	for i := 0; i < len(decodedRows); i++ {
		page[i] = decodedRows[i]
	}
	p.numRows = len(decodedRows)

	// we need the data to be pages
	p.pages[0] = &page

	fmt.Println("Table Loaded.")

	return p, nil
}

func (t *Table) AppendRow(row *Row) error {
	// _ := int(math.Floor(float64(t.NumRows) / float64(ROWS_PER_PAGE)))
	// _ := t.NumRows % ROWS_PER_PAGE

	// t.Pages[targetPage][pageIx] = row

	*t.NumRows += 1
	return nil
}

// end region: table structs
