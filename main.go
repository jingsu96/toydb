package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
)

// hardcoded DB
const (
	COLUMN_USERNAME_SIZE = 32
	COLUMN_EMAIL_SIZE    = 255
	PAGE_SIZE            = 4096
	TABLE_MAX_PAGES      = 100
)

const (
	ID_SIZE         = 4 // size of uint32
	USERNAME_SIZE   = COLUMN_USERNAME_SIZE
	EMAIL_SIZE      = COLUMN_EMAIL_SIZE
	ID_OFFSET       = 0
	USERNAME_OFFSET = ID_OFFSET + ID_SIZE
	EMAIL_OFFSET    = USERNAME_OFFSET + USERNAME_SIZE
	ROW_SIZE        = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE
	ROWS_PER_PAGE   = PAGE_SIZE / ROW_SIZE
	TABLE_MAX_ROWS  = ROWS_PER_PAGE * TABLE_MAX_PAGES
)

type Cursor struct {
	Table      *Table
	RowNum     uint32
	EndOfTable bool // Indicates a position one past the last element
}

// Pager handles reading/writing pages to disk
type Pager struct {
	FileDescriptor *os.File
	FileLength     int64
	Pages          [TABLE_MAX_PAGES][]byte
}

// Row represents a single row in our table
type Row struct {
	ID       uint32
	Username [COLUMN_USERNAME_SIZE]byte
	Email    [COLUMN_EMAIL_SIZE]byte
}

// Table represent our in-memory table structure
type Table struct {
	NumRows uint32
	Pager   *Pager
}

// StatementType represents the type of SQL statement
type StatementType int

const (
	STATEMENT_INSERT StatementType = iota
	STATEMENT_SELECT
)

// Statement holds a parsed SQL statement
type Statement struct {
	Type        StatementType
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

// tableStart creates a cursor at the beginning of the table
func tableStart(table *Table) *Cursor {
	cursor := &Cursor{
		Table:      table,
		RowNum:     0,
		EndOfTable: (table.NumRows == 0),
	}

	return cursor
}

// tableEnd creates a cursor past the end of the table
func tableEnd(table *Table) *Cursor {
	cursor := &Cursor{
		Table:      table,
		RowNum:     table.NumRows,
		EndOfTable: true,
	}

	return cursor
}

// cursorValue returns a slice pointing to the position described by the cursor
func cursorValue(cursor *Cursor) ([]byte, error) {
	rowNum := cursor.RowNum
	pageNum := rowNum / ROWS_PER_PAGE

	page, err := cursor.Table.Pager.getPage(pageNum)
	if err != nil {
		return nil, err
	}

	rowOffset := rowNum % ROWS_PER_PAGE
	byteOffset := rowOffset * ROW_SIZE

	return page[byteOffset : byteOffset+ROW_SIZE], nil
}

// cursorAdvance moves the cursor to the next row
func cursorAdvance(cursor *Cursor) {
	cursor.RowNum++
	if cursor.RowNum >= cursor.Table.NumRows {
		cursor.EndOfTable = true
	}
}

// pagerOpen opens the database file and initializes the pager
func pagerOpen(filename string) (*Pager, error) {
	// Open file with read/write permissions, create if doesn't exist
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, fmt.Errorf("Unable to open file: %v", err)
	}

	// Get file size
	fileInfo, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("Unable to get file info: %v", err)
	}

	pager := &Pager{
		FileDescriptor: file,
		FileLength:     fileInfo.Size(),
	}

	// Initialize all pages to nil
	for i := 0; i < TABLE_MAX_PAGES; i++ {
		pager.Pages[i] = nil
	}

	return pager, nil
}

// dbOpen opens a database connection
func dbOpen(filename string) (*Table, error) {
	pager, err := pagerOpen(filename)
	if err != nil {
		return nil, err
	}

	numRows := uint32(pager.FileLength / ROW_SIZE)

	table := &Table{
		Pager:   pager,
		NumRows: numRows,
	}

	return table, nil
}

func (p *Pager) getPage(pageNum uint32) ([]byte, error) {
	if pageNum > TABLE_MAX_PAGES {
		return nil, fmt.Errorf("Tried to fetch page number out of bounds, %d > %d", pageNum, TABLE_MAX_PAGES)
	}

	if p.Pages[pageNum] == nil {
		// Cache miss, Allocate meemory and load from file
		page := make([]byte, PAGE_SIZE)
		numPages := p.FileLength / PAGE_SIZE

		// We might have a partial page at the end of the file
		if p.FileLength%PAGE_SIZE != 0 {
			numPages++
		}

		if int64(pageNum) < numPages {
			// Seek to the correct position in the file
			_, err := p.FileDescriptor.Seek(int64(pageNum)*PAGE_SIZE, 0)
			if err != nil {
				return nil, fmt.Errorf("Error seeking file: %v", err)
			}

			// Read the page
			bytesRead, err := p.FileDescriptor.Read(page)
			if err != nil && err != io.EOF {
				return nil, fmt.Errorf("Error reading file: %v", err)
			}

			// If we read less than a full page, that's okay
			_ = bytesRead
		}

		p.Pages[pageNum] = page
	}

	return p.Pages[pageNum], nil
}

// pagerFlush writes a page to disk
func (p *Pager) pagerFlush(pageNum uint32, size uint32) error {
	if p.Pages[pageNum] == nil {
		return fmt.Errorf("Tried to flush null page")
	}

	// Seek to the correct position
	_, err := p.FileDescriptor.Seek(int64(pageNum)*PAGE_SIZE, 0)
	if err != nil {
		return fmt.Errorf("Error seeking: %v", err)
	}

	// Write the page
	bytesWritten, err := p.FileDescriptor.Write(p.Pages[pageNum][:size])
	if err != nil {
		return fmt.Errorf("Error writing: %v", err)
	}

	if uint32(bytesWritten) != size {
		return fmt.Errorf("Wrote %d bytes, expected %d", bytesWritten, size)
	}

	return nil
}

// dbClose flushes all pages to disk and closes the database
func dbClose(table *Table) error {
	pager := table.Pager
	numFullPages := table.NumRows / ROWS_PER_PAGE

	for i := uint32(0); i < numFullPages; i++ {
		if pager.Pages[i] == nil {
			continue
		}

		err := pager.pagerFlush(i, PAGE_SIZE)

		if err != nil {
			return err
		}

		pager.Pages[i] = nil
	}

	numAdditionalRows := table.NumRows % ROWS_PER_PAGE
	if numAdditionalRows > 0 {
		pageNum := numFullPages

		if pager.Pages[pageNum] != nil {
			err := pager.pagerFlush(pageNum, numAdditionalRows*ROW_SIZE)
			if err != nil {
				return err
			}
			pager.Pages[pageNum] = nil
		}
	}

	// Close the file
	err := pager.FileDescriptor.Close()
	if err != nil {
		return fmt.Errorf("Error closing db file: %v", err)
	}

	return nil
}

/*
*
  - +-------------------------------------------------------------+
    |      (byte(ID), byte(ID>>8), byte(ID>>16), byte(ID>>24))     |
    |   +---------------------------------------------------------+
    |   | 0x01 | 0x02 | 0x03 | 0x04 | ..... (Array)               |
    |   +---------------------------------------------------------+
    |     Array:                                                |
    |     `destination`  ->  [0, 1, 2, 3, 4, 5, 6, 7, ..., N]     |
    +-------------------------------------------------------------+
*/
func serializeRow(source *Row, destination []byte) {
	binary.LittleEndian.PutUint32(destination[ID_OFFSET:], source.ID)

	copy(destination[USERNAME_OFFSET:], source.Username[:])
	copy(destination[EMAIL_OFFSET:], source.Email[:])
}

func deserializeRow(source []byte, destination *Row) {
	destination.ID = binary.LittleEndian.Uint32(source[ID_OFFSET:])

	copy(destination.Username[:], source[USERNAME_OFFSET:USERNAME_OFFSET+USERNAME_SIZE])
	copy(destination.Email[:], source[EMAIL_OFFSET:EMAIL_OFFSET+EMAIL_SIZE])
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

func doMetaCommand(inputBuffer *InputBuffer, table *Table) MetaCommandResult {
	if inputBuffer.buffer == ".exit" {
		err := dbClose(table)

		if err != nil {
			fmt.Printf("Error closing database %v\n", err)
		}

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
	cursor := tableEnd(table)
	slot, err := cursorValue(cursor)

	if err != nil {
		fmt.Printf("Error getting row slot: %v\n", err)
		return EXECUTE_TABLE_FULL
	}

	serializeRow(rowToInsert, slot)
	table.NumRows++

	return EXECUTE_SUCCESS
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	cursor := tableStart(table)

	var row Row

	for !cursor.EndOfTable {
		slot, err := cursorValue(cursor)
		if err != nil {
			fmt.Printf("Error getting cursor value: %v\n", err)
			cursorAdvance(cursor)
			continue
		}
		deserializeRow(slot, &row)
		printRow(&row)
		cursorAdvance(cursor)
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
	if len(os.Args) < 2 {
		fmt.Println("Must supply a database filename.")
		os.Exit(1)
	}

	filename := os.Args[1]
	table, err := dbOpen(filename)
	if err != nil {
		fmt.Printf("Error opening database: %v\n", err)
		os.Exit(1)
	}
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
			switch doMetaCommand(inputBuffer, table) {
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
