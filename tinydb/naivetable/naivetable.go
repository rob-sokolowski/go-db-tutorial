package naivetable

import (
	"fmt"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb"
	"io"
)

type NaiveTable struct {
	NumRows *int
	pager   *tinydb.Pager
}

func NewNaiveTable() (*NaiveTable, error) {
	return &NaiveTable{}, nil
}

func (*NaiveTable) ExecuteSelect(statement tinydb.Statement, w io.Writer) error {
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

func (*NaiveTable) ExecuteInsert(statement tinydb.Statement, w io.Writer) error {
	fmt.Fprintln(w, "NaiveTable.ExecuteInsert()")
	fmt.Fprintln(w, statement)

	return nil
}
