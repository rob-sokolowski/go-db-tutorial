package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"os"
	// "strings"
	"testing"
)

// func TestCli(t *testing.T) {
// 	reader := strings.NewReader("insert 1 rob rob@example.com\nselect\n.exit\n")
// 	out := bytes.Buffer{}
// 	cli(reader, &out, "textcli.data")
// 	out.String()

// 	want := "db > statement executed.\ndb > &{1 rob rob@example.com}\nstatement executed.\ndb > adios!\n"
// 	if out.String() != want {
// 		t.Errorf("unexpected output")
// 	}
// }

func TestWriteThenReadBytes(t *testing.T) {
	rows := make([]*Row, 0, 10)

	rows = append(rows, &Row{
		Id:       1,
		Username: "Simon",
		Email:    "Simon@cat.com",
	})

	rows = append(rows, &Row{
		Id:       2,
		Username: "Jing",
		Email:    "Jing@cat.com",
	})

	// Encode the struct into a gob
	var buffer bytes.Buffer
	encoder := gob.NewEncoder(&buffer)
	err := encoder.Encode(rows)
	if err != nil {
		fmt.Println("Encoding Error:", err)
		return
	}

	err = os.WriteFile("file123.data", buffer.Bytes(), 0666)
	if err != nil {
		log.Fatal(err)
	}

	// Decode the gob back into a struct
	file, err := os.Open("file123.data")
	var decodedRows []*Row

	decoder := gob.NewDecoder(file)
	err = decoder.Decode(&decodedRows)

	if err != nil {
		fmt.Println("Decoding Error:", err)
		return
	}

	for i := 0; i < len(decodedRows); i++ {
		fmt.Println(*decodedRows[i])
	}
	
	fmt.Println(decodedRows)

	//// Decode the gob back into a struct
	//var decodedInstance Row
	//decoder := gob.NewDecoder(&buffer)
	//err = decoder.Decode(&decodedInstance)
	//fmt.Println("Decoded Struct:", decodedInstance)

}
