package main

import (
	"bufio"
	"encoding/gob"
	"fmt"
	"io"
	"log"
	// "math"
	"os"
	"strings"
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
	stmnt       string
	rowToInsert *Row
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

func dbOpen(filename string) *Table {
	p, err := pagerOpen(filename)

	if err != nil {
		fmt.Println(err)
	}
	t := &Table{
		NumRows: &p.numRows,
		pager:   p,
	}

	return t
}

func pagerOpen(filename string) (*Pager, error) {
	// open file
	//file, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	file, err := os.Open(filename)

	if err != nil {
		log.Fatal(err)
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

func (t *Table) appendRow(row *Row) error {
	// _ := int(math.Floor(float64(t.NumRows) / float64(ROWS_PER_PAGE)))
	// _ := t.NumRows % ROWS_PER_PAGE

	// t.Pages[targetPage][pageIx] = row

	*t.NumRows += 1
	return nil
}

// end region: table structs

func validateMetaCommand(cmd string) error {
	switch cmd {
	case ".exit":
		return nil
	}

	return fmt.Errorf("unrecognized meta command: %s", cmd)
}

// doMetaCommand does the meta command, and returns a boolean value you can think of as "shouldQuit".
// It is the responsibility of the caller to handle graceful quiting. While an os.Exit(0) can be done here
// it has ramification on unit tests, as it closes the tests themselves!
func doMetaCommand(cmd string, w io.Writer) bool {
	switch cmd {
	// Note: the meta command ".exit" is handled outside
	case ".exit":
		fmt.Fprintln(w, "adios!")
		return true
	}

	return false
}

func prepareStatement(cmd string) (*Statement, error) {
	args := strings.Split(cmd, " ")
	cmd_ := strings.Join(args[1:], " ")
	switch args[0] {
	case "select":
		statement := &Statement{
			stmnt:       "select",
			rowToInsert: nil,
		}

		return statement, nil

	case "insert":
		row := &Row{}
		nRead, err := fmt.Sscanf(cmd_, "%d %s %s", &row.Id, &row.Username, &row.Email)
		if err != nil {
			return nil, fmt.Errorf("I read %d things but expected 3", nRead)
		}

		statement := &Statement{
			stmnt:       "insert",
			rowToInsert: row,
		}

		return statement, nil
	}

	return nil, fmt.Errorf("unrecognized statement: %s", cmd)
}

func executeStatement(table *Table, statement Statement, w io.Writer) error {
	switch statement.stmnt {
	case "select":
		err := executeSelect(table, statement, w)
		if err != nil {
			fmt.Errorf("cannot execute select")
			return err
		}

	case "insert":
		err := executeInsert(table, statement)
		if err != nil {
			fmt.Errorf("cannot execute insert: %s", err)
			return err
		}
	}
	fmt.Fprintln(w, "statement executed.")
	return nil
}

func executeInsert(table *Table, statement Statement) error {
	maxRows := TABLE_PAGE_CAP * ROWS_PER_PAGE

	if *table.NumRows == maxRows {
		return fmt.Errorf("max table row count of %d exceeded", maxRows)
	}

	table.appendRow(statement.rowToInsert)

	return nil
}

func executeSelect(table *Table, statement Statement, w io.Writer) error {
	if *table.NumRows == 0 {
		fmt.Fprintln(w, "No rows in this table")
	}
	for i := 0; i < *table.NumRows; i++ {
		fmt.Fprintf(w, "TODO: print row %d \n", i)
		// _ := int(math.Floor(float64(i) / float64(ROWS_PER_PAGE)))
		// _ := i % ROWS_PER_PAGE

		// fmt.Fprintln(w, table.Pages[targetPage][pageIx])
	}

	return nil
}

func cli(reader io.Reader, writer io.Writer, filename string) {

	theTable := dbOpen(filename)

	scanner := bufio.NewScanner(reader)

	for {
		fmt.Fprint(writer, "db > ")
		scanner.Scan()
		input := scanner.Text()

		if len(input) > 0 && input[0] == '.' {
			err := validateMetaCommand(input)
			if err != nil {
				fmt.Fprintln(writer, err)
				continue
			}
			if doMetaCommand(input, writer) {
				// we've received a true value for "shouldQuit"
				break
			}
			continue
		}

		statement, err := prepareStatement(input)
		if err != nil {
			fmt.Fprintln(writer, err)
			continue
		}
		executeStatement(theTable, *statement, writer)
	}
}

func main() {
	cli(os.Stdin, os.Stdout, "file123.data")
}
