package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// StatementType represents the type of SQL statement
type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

// Statement holds a parsed SQL statement
type Statement struct {
	Type StatementType
}

// MetaCommandResult represents the result of executing a meta command
type MetaCommandResult int

const (
    META_COMMAND_SUCCESS MetaCommandResult = iota
    META_COMMAND_UNRECOGNIZED_COMMAND
)

// PrepareResult represents the result of preparing a statement
type PrepareResult int

const (
	PREPARE_SUCCESS PrepareResult = iota
	PREPARE_UNRECOGNIZED_STATEMENT
)

type InputBuffer struct {
    buffer string
}

func NewInputBuffer() *InputBuffer {
    return &InputBuffer{}
}

func printPrompt() {
	fmt.Print("db > ")
}

func (ib *InputBuffer) readInput(reader *bufio.Reader) error {
    input, err := reader.ReadString('\n')
    if err != nil {
        return fmt.Errorf("Error reading input: %v", err)
    }

    // Remove the trailing newline
    ib.buffer = strings.TrimRight(input, "\n\r")
    return nil
}

func doMetaCommand(inputBuffer *InputBuffer) MetaCommandResult {
	if inputBuffer.buffer == ".exit" {
		fmt.Println("Bye!")
		os.Exit(0)
	}

	return META_COMMAND_UNRECOGNIZED_COMMAND
}

func prepareStatement(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	if strings.HasPrefix(inputBuffer.buffer, "insert") {
		statement.Type = STATEMENT_INSERT
		return PREPARE_SUCCESS
	}

	if inputBuffer.buffer == "select" {
		statement.Type = STATEMENT_SELECT
		return PREPARE_SUCCESS
	}

	return PREPARE_UNRECOGNIZED_STATEMENT
}

func executeStatement(statement *Statement) {
	switch statement.Type {
		case STATEMENT_INSERT:
			fmt.Println("TODO: Insert")
		case STATEMENT_SELECT:
			fmt.Println("TODO: SELECT")
	}
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	inputBuffer := NewInputBuffer()

	for {
		printPrompt()

		err := inputBuffer.readInput(reader)

		if err != nil {
			fmt.Println(err)
			continue
		}

		// Check if it's a meta-command
		if strings.HasPrefix(inputBuffer.buffer, ".") {
			switch doMetaCommand(inputBuffer) {
			case META_COMMAND_SUCCESS:
				continue
			case META_COMMAND_UNRECOGNIZED_COMMAND:
				fmt.Printf("Unrecognized command '%s'\n", inputBuffer.buffer)
				continue
			}
		}

		// Otherwise, it's a SQL statement
		var statement Statement
		switch prepareStatement(inputBuffer, &statement) {
		case PREPARE_SUCCESS:
			// Statement prepared successfully
		case PREPARE_UNRECOGNIZED_STATEMENT:
			fmt.Printf("Unrecognized keyword at start of '%s'.\n", inputBuffer.buffer)
			continue
		}

		executeStatement(&statement)
		fmt.Println("Executed.")
	}
}
