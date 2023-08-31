package main

import (
	"bufio"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
)

// begin region: table structs
const USERNAME_MAX = 32
const EMAIL_MAX = 255
const ROWS_PER_PAGE = 3
const TABLE_PAGE_CAP = 10

type Page = [ROWS_PER_PAGE]*Row

type Row struct {
	Id       int
	Username string
	Email    string
}

func writeToDisk(filename string, row Row) {

}

func (r *Row) setUsername(u string) error {
	if len(u) > USERNAME_MAX {
		return fmt.Errorf("maximum length of Username is %d", USERNAME_MAX)
	}

	r.Username = u
	return nil
}

func (r *Row) setEmail(e string) error {
	if len(e) > EMAIL_MAX {
		return fmt.Errorf("maximum length of Username is %d", EMAIL_MAX)
	}

	r.Username = e
	return nil
}

type Statement struct {
	stmnt       string
	rowToInsert *Row
}

type Table struct {
	NumRows int
	Pages   [TABLE_PAGE_CAP]Page
}

func NewTable() *Table {
	var pages [TABLE_PAGE_CAP][ROWS_PER_PAGE]*Row

	return &Table{
		NumRows: 0,
		Pages:   pages,
	}
}

func (t *Table) appendRow(row *Row) error {
	targetPage := int(math.Floor(float64(t.NumRows) / float64(ROWS_PER_PAGE)))
	pageIx := t.NumRows % ROWS_PER_PAGE

	t.Pages[targetPage][pageIx] = row
	t.NumRows += 1
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

	if table.NumRows == maxRows {
		return fmt.Errorf("max table row count of %d exceeded", maxRows)
	}

	table.appendRow(statement.rowToInsert)

	return nil
}

func executeSelect(table *Table, statement Statement, w io.Writer) error {
	if table.NumRows == 0 {
		fmt.Fprintln(w, "No rows in this table")
	}
	for i := 0; i < table.NumRows; i++ {
		targetPage := int(math.Floor(float64(i) / float64(ROWS_PER_PAGE)))
		pageIx := i % ROWS_PER_PAGE

		fmt.Fprintln(w, table.Pages[targetPage][pageIx])
	}

	return nil
}

func cli(reader io.Reader, writer io.Writer) {
	theTable := NewTable()
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
	cli(os.Stdin, os.Stdout)
}
