package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestCli(t *testing.T) {
	reader := strings.NewReader("insert 1 rob rob@example.com\nselect\n.exit\n")
	out := bytes.Buffer{}
	cli(reader, &out)
	out.String()

	want := "db > statement executed.\ndb > &{1 rob rob@example.com}\nstatement executed.\ndb > adios!\n"
	if out.String() != want {
		t.Errorf("unexpected output")
	}
}
