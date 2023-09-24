package naivetable

import (
	"fmt"
	"github.com/rob-sokolowski/go-db-tutorial/tinydb"
	"io"
	"os"
)

const ROWS_PER_PAGE = 3
const TABLE_PAGE_CAP = 10

type Page = [ROWS_PER_PAGE]*tinydb.Row

type Pager struct {
	filePointer *os.File
	pages       [TABLE_PAGE_CAP]Page
	numRows     int
}

type NaiveTable struct {
	tree      BTree
	tableName string
	file      *os.File
}

// TODO: New naive table should create directory file if not exists, noop if it does
func NewNaiveTable(filepath, tableName string) (*NaiveTable, error) {
	// Check if file exists, create it if not.
	var file *os.File

	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		file, err = os.Create(filepath)
		if err != nil {
			return nil, fmt.Errorf("failed to create file: %w", err)
		}
	} else if err != nil {
		// Another error occurred while checking file existence
		return nil, fmt.Errorf("failed to check file existence: %w", err)
	} else {
		// File exists, open it
		file, err = os.Open(filepath)
		if err != nil {
			return nil, fmt.Errorf("failed to open file: %w", err)
		}
	}

	// TODO: Load b-tree?? Or, am I keeping everything file-based for now?
	tree := BTree{
		Path: filepath,
	}

	return &NaiveTable{
		tree:      tree,
		tableName: tableName,
		file:      file,
	}, nil
}

func (t *NaiveTable) ExecuteSelect(statement tinydb.Statement, w io.Writer) error {
	// noop until I swap in  b-tree
	_, _ = fmt.Fprintln(w, "ExecuteSelect: NOOP")

	return nil
}

func (t *NaiveTable) ExecuteInsert(statement tinydb.Statement, w io.Writer) error {
	// noop until I swap in b-tree
	_, _ = fmt.Fprintln(w, "ExecuteInsert: NOOP")

	return t.insert(statement.RowToInsert.Id, *statement.RowToInsert)
}

func (t *NaiveTable) insert(key int, row tinydb.Row) error {

	return nil
}

func (t *NaiveTable) Persist(w io.Writer) error {
	// noop until I swap in b-tree
	_, _ = fmt.Fprintln(w, "Persist: NOOP")
	return nil
}

type BTree struct {
	Root *Node
	Path string
	Vals map[int]tinydb.Row
}

type ref = string

type Node struct {
	kind     string // ref of val
	ref      *ref
	val      *tinydb.Row
	Children []*Node
}

// initially, as we fill the tree, it consists of a single node containing nodes of val kind
// we keep everything in this node, until an insert would result in the root node being greater than
// on page (4KB). At this moment, we split the tree, the result is a total of 3 nodes. The root containing nodes
// of kind ref, and the two children containing nodes of kind val
