package main

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestCli(t *testing.T) {
	reader := strings.NewReader("insert 1 rob rob@example.com\nselect\n.exit\n")
	out := bytes.Buffer{}
	cli(reader, &out)

	fmt.Println("TODO: more test cases")
}
