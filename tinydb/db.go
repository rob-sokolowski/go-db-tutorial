package tinydb

import "io"

type Row struct {
	Id       int
	Username string
	Email    string
}

type Statement struct {
	Stmnt       string
	RowToInsert *Row
}

type DbTable interface {
	ExecuteSelect(statement Statement, w io.Writer) error
	ExecuteInsert(statement Statement, w io.Writer) error
	Persist(w io.Writer) error
}
