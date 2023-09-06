package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb"
	"io"
	"log"
	"os"
	"strings"
)

func main() {
	dbpath := flag.String("dbpath", "db.data", "path to file persisting DB data")
	flag.Parse()

	Cli(os.Stdin, os.Stdout, *dbpath)
}

func Cli(reader io.Reader, writer io.Writer, filename string) {
	theTable, err := tinydb.DbOpen(filename)
	if err != nil {
		log.Fatal(err)
	}

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

func prepareStatement(cmd string) (*tinydb.Statement, error) {
	args := strings.Split(cmd, " ")
	cmd_ := strings.Join(args[1:], " ")
	switch args[0] {
	case "select":
		statement := &tinydb.Statement{
			Stmnt:       "select",
			RowToInsert: nil,
		}

		return statement, nil

	case "insert":
		row := &tinydb.Row{}
		nRead, err := fmt.Sscanf(cmd_, "%d %s %s", &row.Id, &row.Username, &row.Email)
		if err != nil {
			return nil, fmt.Errorf("I read %d things but expected 3", nRead)
		}

		statement := &tinydb.Statement{
			Stmnt:       "insert",
			RowToInsert: row,
		}

		return statement, nil
	}

	return nil, fmt.Errorf("unrecognized statement: %s", cmd)
}

func executeStatement(table *tinydb.Table, statement tinydb.Statement, w io.Writer) error {
	switch statement.Stmnt {
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

func executeInsert(table *tinydb.Table, statement tinydb.Statement) error {
	maxRows := tinydb.TABLE_PAGE_CAP * tinydb.ROWS_PER_PAGE

	if *table.NumRows == maxRows {
		return fmt.Errorf("max table row count of %d exceeded", maxRows)
	}

	table.AppendRow(statement.RowToInsert)

	return nil
}

func executeSelect(table *tinydb.Table, statement tinydb.Statement, w io.Writer) error {
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
