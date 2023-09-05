package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

type Row struct {
	id       int
	username string
	email    string
}

type Statement struct {
	stmnt       string
	rowToInsert *Row
}

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
		nRead, err := fmt.Sscanf(cmd_, "%d %s %s", &row.id, &row.username, &row.email)
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


func executeStatement(t map[int]Row, statement Statement, w io.Writer) error {
	switch statement.stmnt {
	case "select":
		err := executeSelect(t, statement, w)

		if err != nil {
			fmt.Errorf("cannot execute select")
			return err
		}

	case "insert":
		err := executeInsert(t, statement)
		if err != nil {
			fmt.Errorf("cannot execute insert: %s", err)
			return err
		}
	}
	fmt.Fprintln(w, "statement executed.")
	return nil
}

func executeInsert(t map[int]Row, statement Statement) error {
	maxRows := 1000 // arbitrarily assigning for now

	if len(t) == maxRows {
		// throw an error for now
		return fmt.Errorf("max table row count of %d exceeded", maxRows)
	}

	row := *statement.rowToInsert

	_ , exists := t[row.id]

	if !exists {
		t[row.id] = row
	}
	return nil
}

func executeSelect(t map[int]Row, statement Statement, w io.Writer) error {
	if len(t) == 0 {
		fmt.Println("No rows in this table")
	}
	for _, v := range t {
		fmt.Println(v)
	}

	return nil
}

func cli(reader io.Reader, writer io.Writer) {
	theTable := make(map[int]Row) // create a memtable 
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
