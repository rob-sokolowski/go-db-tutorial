package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// begin region: table structs
const USERNAME_MAX = 32
const EMAIL_MAX = 255
const ROWS_PER_PAGE = 1024

type Page = [ROWS_PER_PAGE]Row

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
	Pages   []Page
}

func NewTable(pageCap int) *Table {
	return &Table{
		NumRows: 0,
		Pages:   make([][ROWS_PER_PAGE]Row, 0, pageCap),
	}
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

func prepareStatement(cmd string) (Statement, error) {
	args := strings.Split(cmd, " ")
	cmd_ := strings.Join(args[1:], " ")
	switch args[0] {
	case "select":
		return nil

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

		return statement
	}

	return fmt.Errorf("unrecognized statement: %s", cmd)
}

func doStatement(cmd string) {
	switch cmd {
	case "select":
		fmt.Println("TODO: select handling goes here!")
	case "insert":
		fmt.Println("TODO: insert handling goes here!")
	}
}

func main() {
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

		err := prepareStatement(input)
		if err != nil {
			fmt.Println(err)
			continue
		}
		doStatement(input)
	}
}
