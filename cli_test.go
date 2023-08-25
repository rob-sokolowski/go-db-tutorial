package main

import (
	"bytes"
	"fmt"
	// "os/exec"
	"strings"
	"testing"
	"github.com/stretchr/testify/assert"
	// "github.com/stretchr/testify/mock"
)

// Does testify work?
func TestSomething(t *testing.T) {

  // assert equality
  assert.Equal(t, 123, 123, "they should be equal")

}

// Q: Why does this work even tho cli() is not capitalized?
func TestCli(t *testing.T) {
	reader := strings.NewReader("insert 1 rob rob@example.com\nselect\n.exit\n")
	out := bytes.Buffer{}
	cli(reader, &out)

	fmt.Println("TODO: more test cases")
}

// TODO: it should insert one row, return that row, and exit

// it should throw an err if usernames or emails are longer than column size
func TestsetUsername(t *testing.T) {
	assert := assert.New(t)

	r := &Row{} 
	u := "liUdaBpkVLatMxcsRpyfOjiaNUDQebvot"

	if assert.NotNil(t, r.setUsername(u)) {
		assert.Equal(fmt.Errorf("maximum length of username is %d", USERNAME_MAX), r.setUsername(u))
	}
}

func TestSetEmail(t *testing.T) {
	assert := assert.New(t)

	r := &Row{}
	e := "Lorem ipsum dolor sit amet, consectetur adipiscing elit. Nulla auctor hendrerit neque vel varius. Sed vel turpis nec arcu condimentum hendrerit id ut nulla. Vestibulum lacinia ipsum ac tellus sollicitudin, eget tempor eros bibendum. Integer congue lobortis velit at malesuada. Nullam semper dolor eu aliquet luctus. Duis consectetur nec sapien vel efficitur. Sed scelerisque libero sed justo vestibulum, vel ullamcorper odio dictum."

	if assert.NotNil(t, r.setEmail(e)) {
		// only passes if we don't pass t - why?
		assert.Equal(fmt.Errorf("maximum length of email is %d", EMAIL_MAX), r.setEmail(e))
	}
}	

// it should throw an error if any required field is missing on insert
func TestPrepareStatement (t *testing.T) {
	// select
	s := &Statement{
		stmnt: "select",
		rowToInsert: nil,
	}
	val1, val2 := PrepareStatement("select")
	assert.Equal(t, s, val1)
	assert.Equal(t, val2, nil)

	// insert
	res, err := PrepareStatement("insert 1 liz liz@pup.com")
	// expected row & statement
	r:= &Row{
		id: 1,
		username: "liz",
		email: "liz@pup.com",
	}
	s2 := &Statement{
		stmnt: "insert",
		rowToInsert: r,
	}
	assert.Equal(t, s2, res)
	assert.Nil(t, err)

	// insert with args are of wrong type
	s3, err2 := PrepareStatement("insert liz liz@pup.com")
	// why does this work when we pass t, but other error msgs do not?
	assert.Equal(t, fmt.Errorf("I read 0 things but expected 3"), err2)
	assert.Nil(t, s3)
}


// TODO: it should throw an err if exceeds max rows for table
// TODO: it should not overwrite data on insert
// Q: how to mock a table and test behavior?

// it should exit or throw error for metacommands
func TestValidateMetaCommand(t *testing.T) {
	assert := assert.New(t)

	// should return nil if command is .exit
	// what does passing t do? why does test pass if we don't pass t? why does it fail if we do?
	assert.Nil(validateMetaCommand(".exit"))

	// TODO: should exit if command is .exit

	// should return error is command not .exit
	if assert.NotNil(validateMetaCommand(".")) {
		assert.Equal(fmt.Errorf("unrecognized meta command: ."), validateMetaCommand("."))
	}
}
