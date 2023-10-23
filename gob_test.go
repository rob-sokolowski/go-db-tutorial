package main

import (
	"encoding/gob"
	"fmt"
	"os"
	"reflect"
	"testing"
	"unsafe"
)

type MyStruct struct {
	Id  uint64
	Msg string
}

type MyIx struct {
	Offsets []uint16
}

func persistData(filename string, data []MyStruct) error {
	file, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(data); err != nil {
		return err
	}

	return nil
}

func readAt(filename string, offset int64) (*MyStruct, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var val MyStruct
	valPtr := reflect.New(reflect.TypeOf(val))

	_, err = file.Seek(offset, 0)
	decoder := gob.NewDecoder(file)
	if err != nil {
		return nil, err
	}

	err = decoder.DecodeValue(valPtr.Elem())
	if err != nil {
		return nil, err
	}

	return &val, nil
}

func TestGobValueOffset(t *testing.T) {
	filename := "gob-offset-test.gob"

	// Note: The string values have varying lengths
	data := []MyStruct{
		{Id: 1, Msg: "And done!"},
		{Id: 2, Msg: "Is overdue!"},
		{Id: 3, Msg: "Go climb a tree!"},
	}
	totalSize := int(unsafe.Sizeof(data[0]) + unsafe.Sizeof(data[1]) + unsafe.Sizeof(data[2]))

	err := persistData(filename, data)
	if err != nil {
		t.Error("Failed to create file")
		t.FailNow()
	}

	for offset := 0; offset < totalSize; offset++ {
		readVal, err := readAt(filename, int64(offset))

		if err == nil {
			fmt.Print("WIN!")
		}
		fmt.Println(readVal)
	}
}
