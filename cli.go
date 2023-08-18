package main

import (
	"bufio"
	"fmt"
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
	id       int
	username string
	email    string
}

func (r *Row) setUsername(u string) error {
	if len(u) > USERNAME_MAX {
		return fmt.Errorf("maximum length of username is %d", USERNAME_MAX)
	}

	r.username = u
	return nil
}

func (r *Row) setEmail(e string) error {
	if len(e) > EMAIL_MAX {
		return fmt.Errorf("maximum length of username is %d", EMAIL_MAX)
	}

	r.username = e
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

func doMetaCommand(cmd string) {
	switch cmd {
	case ".exit":
		fmt.Println("adios!")
		os.Exit(0)
	}
}

func prepareStatement(cmd string) (*Statement, error) {
	args := strings.Split(cmd, " ")
	cmd_ := strings.Join(args[1:], " ")
	switch args[0] {
	case "select":
		return nil, fmt.Errorf("TODO: Implement select")

	case "insert":
		row := &Row{}
		nRead, err := fmt.Sscanf(cmd_, "%d %s %s", &row.id, &row.username, &row.email)
		if err != nil {
			fmt.Printf("I read %d things", nRead)
		}

		statement := &Statement{
			stmnt:       "insert",
			rowToInsert: row,
		}

		return statement, nil
	}

	return nil, fmt.Errorf("unrecognized statement: %s", cmd)
}

func executeStatement(table *Table, statement Statement) error {
	switch statement.stmnt {
	case "select":
		fmt.Println("TODO: select handling goes here!")
	case "insert":
		err := executeInsert(table, statement)
		if err != nil {
			fmt.Errorf("cannot execute insert: %s", err)
			return err
		}
	}

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

func main() {
	theTable := NewTable()

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("db > ")
		scanner.Scan()
		input := scanner.Text()

		if input[0] == '.' {
			err := validateMetaCommand(input)
			if err != nil {
				fmt.Println(err)
				continue
			}
			doMetaCommand(input)
			continue
		}

		statement, err := prepareStatement(input)
		if err != nil {
			fmt.Println(err)
			continue
		}
		executeStatement(theTable, *statement)
	}
}
