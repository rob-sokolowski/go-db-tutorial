package main

import (
	"bufio"
	"flag"
	"fmt"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb/naivetable"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb/sstable"
	"io"
	"log"
	"os"
	"strings"
)

func main() {
	dbpath := flag.String("dbpath", "db.data", "path to file persisting DB data")
	tableType := flag.String("tableType", "SSTable", "type of table")
	flag.Parse()

	Cli(os.Stdin, os.Stdout, *dbpath, *tableType)
}

func Cli(reader io.Reader, writer io.Writer, filename string, tableType string) error {
	var t tinydb.DbTable
	var err error

	switch {
	case tableType == "NaiveTable":
		t, err = naivetable.NewNaiveTable(filename)
	// TODO add more here
	case tableType == "SSTable":
		t, err = sstable.NewSSTable()
	default:
		return fmt.Errorf("Unknown table type %s", tableType)
	}

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
		executeStatement(t, *statement, writer)
	}
	return nil
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

func executeStatement(table tinydb.DbTable, statement tinydb.Statement, w io.Writer) error {
	switch statement.Stmnt {
	case "select":
		err := table.ExecuteSelect(statement, w)
		if err != nil {
			fmt.Errorf("cannot execute select")
			return err
		}

	case "insert":
		err := table.ExecuteInsert(statement, w)
		if err != nil {
			fmt.Errorf("cannot execute insert: %s", err)
			return err
		}
	}
	fmt.Fprintln(w, "statement executed.")
	return nil
}

//func executeInsert(table *tinydb.NaiveTable, statement tinydb.Statement) error {
//	maxRows := tinydb.TABLE_PAGE_CAP * tinydb.ROWS_PER_PAGE
//
//	if *table.NumRows == maxRows {
//		return fmt.Errorf("max table row count of %d exceeded", maxRows)
//	}
//
//	table.AppendRow(statement.RowToInsert)
//
//	return nil
//}
