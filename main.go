package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// hardcoded DB
const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE = 255
	PAGE_SIZE = 4096
	TALBE_MAX_PAGES = 100
)

const (
	ID_SIZE = 4 // size of uint32
	USERNAME_SIZE = COLUMN_USERNAME_SIZE
	EMAIL_SIZE = COLUMN_EMAIL_SIZE
	ID_OFFSET = 0
	USERNAME_OFFSET = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
	ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS = ROWS_PER_PAGE * TALBE_MAX_PAGES
)

// Row represents a single row in our table
type Row struct {
	ID uint32
	Username [COLUMN_USERNAME_SIZE]byte
	Email [COLUMN_EMAIL_SIZE]byte
}

// Table represent our in-memory table structure
type Table struct {
	NumRows uint32
	Pages [TALBE_MAX_PAGES][]byte
}

// StatementType represents the type of SQL statement
type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

// Statement holds a parsed SQL statement
type Statement struct {
	Type StatementType
	RowToInsert Row // Add this field to hold the row data for INSERT statements
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
	PREPARE_SYNTAX_ERROR
	PREPARE_NEGATIVE_ID
	PREPARE_STRING_TOO_LONG
	PREPARE_UNRECOGNIZED_STATEMENT
)

// ExecuteResult represents the result of executing a statement
type ExecuteResult int

const (
	EXECUTE_SUCCESS ExecuteResult = iota
	EXECUTE_TABLE_FULL
)

type InputBuffer struct {
    buffer string
}

func NewInputBuffer() *InputBuffer {
    return &InputBuffer{}
}

func NewTable() *Table {
	return &Table{
		NumRows: 0,
	}
}

/**
 * +-------------------------------------------------------------+
 |      (byte(ID), byte(ID>>8), byte(ID>>16), byte(ID>>24))     |
 |   +---------------------------------------------------------+
 |   | 0x01 | 0x02 | 0x03 | 0x04 | ..... (Array)               |
 |   +---------------------------------------------------------+
 |     Array:                                                |
 |     `destination`  ->  [0, 1, 2, 3, 4, 5, 6, 7, ..., N]     |
 +-------------------------------------------------------------+
 */
func serializeRow(source *Row, destination []byte) {
	destination[0] = byte(source.ID)
	destination[1] = byte(source.ID >> 8)
	destination[2] = byte(source.ID >> 16)
	destination[3] = byte(source.ID >> 24)

	copy(destination[USERNAME_OFFSET:], source.Username[:])
	copy(destination[EMAIL_OFFSET:], source.Email[:])
}

func deserializeRow(source []byte, destination *Row) {
     destination.ID = uint32(source[0]) |
        uint32(source[1])<<8 |
        uint32(source[2])<<16 |
        uint32(source[3])<<24

    copy(destination.Username[:], source[USERNAME_OFFSET:USERNAME_OFFSET+USERNAME_SIZE])

    copy(destination.Email[:], source[EMAIL_OFFSET:EMAIL_OFFSET+EMAIL_SIZE])
}

func (t *Table) rowSlot(rowNum uint32) []byte {
	pageNum := rowNum / ROWS_PER_PAGE

	if t.Pages[pageNum] == nil {
		t.Pages[pageNum] = make([]byte, PAGE_SIZE)
	}

	rowOffset := rowNum % ROWS_PER_PAGE
	byteOffset := rowOffset * ROW_SIZE

	return t.Pages[pageNum][byteOffset : byteOffset+ROW_SIZE]
}

func printRow(row *Row) {
    username := strings.TrimRight(string(row.Username[:]), "\x00")
    email := strings.TrimRight(string(row.Email[:]), "\x00")
    fmt.Printf("(%d, %s, %s)\n", row.ID, username, email)
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

func prepareInsert(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	statement.Type = STATEMENT_INSERT

	tokens := strings.Fields(inputBuffer.buffer)

	if len(tokens) < 4 {
		return PREPARE_SYNTAX_ERROR
	}

	var id int

	_, err := fmt.Sscanf(tokens[1], "%d", &id)

	if err != nil {
		return PREPARE_SYNTAX_ERROR
	}

	if id < 0 {
		return PREPARE_NEGATIVE_ID
	}

	statement.RowToInsert.ID = uint32(id)

	username := tokens[2]

	if len(username) > COLUMN_USERNAME_SIZE {
		return PREPARE_STRING_TOO_LONG
	}

	copy(statement.RowToInsert.Username[:], username)

	email := tokens[3]

    if len(email) > COLUMN_EMAIL_SIZE {
        return PREPARE_STRING_TOO_LONG
    }

    copy(statement.RowToInsert.Email[:], email)

    return PREPARE_SUCCESS
}

func prepareStatement(inputBuffer *InputBuffer, statement *Statement) PrepareResult {
	tokens := strings.Fields(inputBuffer.buffer)

    if len(tokens) == 0 {
        return PREPARE_UNRECOGNIZED_STATEMENT
    }

    switch tokens[0] {
    case "insert":
        return prepareInsert(inputBuffer, statement)
    case "select":
        statement.Type = STATEMENT_SELECT
        return PREPARE_SUCCESS
    default:
        return PREPARE_UNRECOGNIZED_STATEMENT
    }
}

func executeInsert(statement *Statement, table *Table) ExecuteResult {
	if table.NumRows >= TABLE_MAX_ROWS {
		return EXECUTE_TABLE_FULL
	}

	rowToInsert := &statement.RowToInsert

	serializeRow(rowToInsert, table.rowSlot(table.NumRows))
	table.NumRows++
	return EXECUTE_SUCCESS
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
    var row Row
    for i := uint32(0); i < table.NumRows; i++ {
        deserializeRow(table.rowSlot(i), &row)
        printRow(&row)
    }
    return EXECUTE_SUCCESS
}

func executeStatement(statement *Statement, table *Table) ExecuteResult {
    switch statement.Type {
    case STATEMENT_INSERT:
        return executeInsert(statement, table)
    case STATEMENT_SELECT:
        return executeSelect(statement, table)
    default:
        return EXECUTE_SUCCESS
    }
}

func main() {
	table := NewTable()
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
			case PREPARE_STRING_TOO_LONG:
			    fmt.Println("String is too long.")
			    continue
			case PREPARE_NEGATIVE_ID:
			    fmt.Println("ID must be positive.")
			    continue
			case PREPARE_SYNTAX_ERROR:
			    fmt.Println("Syntax error. Could not parse statement.")
			    continue
			case PREPARE_UNRECOGNIZED_STATEMENT:
			    fmt.Printf("Unrecognized keyword at start of '%s'.\n", inputBuffer.buffer)
			    continue
		}

		result := executeStatement(&statement, table)
		switch result {
		case EXECUTE_SUCCESS:
            fmt.Println("Executed.")
        case EXECUTE_TABLE_FULL:
            fmt.Println("Error: Table full.")
        }
	}
}
