package main

import (
	"fmt"
	"testing"
	"unsafe"
)

func TestSizeOfInt(t *testing.T) {
	var x int

	x = 10

	s := unsafe.Sizeof(x)
	fmt.Println(s)
}

func TestRow(t *testing.T) {
	row := &Row{
		id:       1,
		username: "jing",
		email:    "jing@gmail.com",
	}

	var name = "x"
	var name2 = "abc"
	var name3 = "abcd"

	fmt.Println(unsafe.Sizeof(name))
	fmt.Println(unsafe.Sizeof(name2))
	fmt.Println(unsafe.Sizeof(name3))
	fmt.Println(row)

}
