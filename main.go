package main

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"strings"
	"toydb/btree"
	"toydb/constants"
)

// hardcoded DB
const (
	ID_OFFSET       = 0
	USERNAME_OFFSET = ID_OFFSET + constants.ID_SIZE
	EMAIL_OFFSET    = USERNAME_OFFSET + constants.COLUMN_USERNAME_SIZE
)

type Cursor struct {
	Table      *Table
	PageNum    uint32
	CellNum    uint32
	EndOfTable bool // Indicates a position one past the last element
}

// Pager handles reading/writing pages to disk
type Pager struct {
	FileDescriptor *os.File
	FileLength     int64
	NumPages       uint32
	Pages          [constants.TABLE_MAX_PAGES][]byte
}

// Row represents a single row in our table
type Row struct {
	ID       uint32
	Username [constants.COLUMN_USERNAME_SIZE]byte
	Email    [constants.COLUMN_EMAIL_SIZE]byte
}

// Table represent our in-memory table structure
type Table struct {
	RootPageNum uint32
	Pager       *Pager
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
	EXECUTE_DUPLICATE_KEY
	EXECUTE_TABLE_FULL
)

type InputBuffer struct {
	buffer string
}

func NewInputBuffer() *InputBuffer {
	return &InputBuffer{}
}

// tableStart creates a cursor at the beginning of the table
func tableStart(table *Table) (*Cursor, error) {
	cursor := &Cursor{
		Table:   table,
		CellNum: 0,
		PageNum: table.RootPageNum,
	}

	rootNode, err := table.Pager.getPage(table.RootPageNum)

	if err != nil {
		return nil, err
	}

	numCells := btree.LeafNodeNumCells(rootNode)
	cursor.EndOfTable = numCells == 0

	return cursor, nil
}

func tableFind(table *Table, key uint32) (*Cursor, error) {
	rootPageNum := table.RootPageNum
	rootNode, err := table.Pager.getPage(rootPageNum)

	if err != nil {
		return nil, err
	}

	if btree.GetNodeType(rootNode) == btree.NODE_LEAF {
		return leafNodeFind(table, rootPageNum, key)
	} else {
		return internalNodeFind(table, rootPageNum, key)
	}
}

func internalNodeFind(table *Table, pageNum uint32, key uint32) (*Cursor, error) {
	node, err := table.Pager.getPage(pageNum)

	if err != nil {
		return nil, err
	}

	numKeys := btree.InternalNodeNumKeys(node)

	minIdx := uint32(0)
	maxIdx := numKeys

	for minIdx != maxIdx {
		idx := (minIdx + maxIdx) / 2
		keyToRight := btree.InternalNodeKey(node, idx)
		if keyToRight >= key {
			maxIdx = idx
		} else {
			minIdx = idx + 1
		}
	}

	childNum := btree.InternalNodeChild(node, minIdx)
	child, err := table.Pager.getPage(childNum)

	if err != nil {
		return nil, err
	}

	// Recursively search the child
    switch btree.GetNodeType(child) {
    case btree.NODE_LEAF:
        return leafNodeFind(table, childNum, key)
    case btree.NODE_INTERNAL:
        return internalNodeFind(table, childNum, key)
    default:
        return nil, fmt.Errorf("Unknown node type")
    }
}

// getUnusedPageNum returns the next available page number
func getUnusedPageNum(pager *Pager) uint32 {
	// For now, we just append to the end of the file
	return pager.NumPages
}

// getNodeMaxKey returns the max key in a node
func getNodeMaxKey(node []byte) uint32 {
	switch btree.GetNodeType(node) {
	case btree.NODE_INTERNAL:
		numKeys := btree.InternalNodeNumKeys(node)
		return btree.InternalNodeKey(node, numKeys-1)
	case btree.NODE_LEAF:
		numCells := btree.LeafNodeNumCells(node)
		return btree.LeafNodeKey(node, numCells-1)
	default:
		panic("Unknown node type")
	}
}

func createNewRoot(table *Table, rightChildPageNum uint32) error {
	root, err := table.Pager.getPage(table.RootPageNum)
	if err != nil {
		return err
	}

	rightChild, err := table.Pager.getPage(rightChildPageNum)
	if err != nil {
		return err
	}

	leftChildPageNum := getUnusedPageNum(table.Pager)
	leftChild, err := table.Pager.getPage(leftChildPageNum)
	if err != nil {
		return err
	}

	// Left child has data copied from old root
	copy(leftChild, root)
	btree.SetNodeRoot(leftChild, false)

	// Root node is a new internal node with one key and two children
	btree.InitializeInternalNode(root)
	btree.SetNodeRoot(root, true)
	btree.SetInternalNodeNumKeys(root, 1)
	btree.SetInternalNodeChild(root, 0, leftChildPageNum)

	leftChildMaxKey := getNodeMaxKey(leftChild)
	btree.SetInternalNodeKey(root, 0, leftChildMaxKey)
	btree.SetInternalNodeRightChild(root, rightChildPageNum)

	// Update parent pointers
	setNodeParent(leftChild, table.RootPageNum)
	setNodeParent(rightChild, table.RootPageNum)

	return nil
}

// Node parent functions (we'll use these later)
func nodeParent(node []byte) uint32 {
	return binary.LittleEndian.Uint32(node[btree.PARENT_POINTER_OFFSET:])
}

func setNodeParent(node []byte, parent uint32) {
	binary.LittleEndian.PutUint32(node[btree.PARENT_POINTER_OFFSET:], parent)
}

func leafNodeFind(table *Table, pageNum uint32, key uint32) (*Cursor, error) {
	node, err := table.Pager.getPage(pageNum)

	if err != nil {
		return nil, err
	}

	numCells := btree.LeafNodeNumCells(node)

	cursor := &Cursor{
		Table:   table,
		PageNum: pageNum,
	}

	minIndex := uint32(0)
	onePastMaxIndex := numCells

	for onePastMaxIndex != minIndex {
		idx := (minIndex + onePastMaxIndex) / 2
		keyAtIndex := btree.LeafNodeKey(node, idx)

		if key == keyAtIndex {
			cursor.CellNum = idx
			return cursor, nil
		}

		if key < keyAtIndex {
			onePastMaxIndex = idx
		} else {
			minIndex = idx + 1
		}
	}

	cursor.CellNum = minIndex
	return cursor, nil
}

// cursorValue returns a slice pointing to the position described by the cursor
func cursorValue(cursor *Cursor) ([]byte, error) {
	page, err := cursor.Table.Pager.getPage(cursor.PageNum)
	if err != nil {
		return nil, err
	}

	return btree.LeafNodeValue(page, cursor.CellNum), nil
}

// cursorAdvance moves the cursor to the next row
func cursorAdvance(cursor *Cursor) error {
	pageNum := cursor.PageNum
	node, err := cursor.Table.Pager.getPage(pageNum)
	if err != nil {
		return err
	}

	cursor.CellNum++
	if cursor.CellNum >= btree.LeafNodeNumCells(node) {
		cursor.EndOfTable = true
	}

	return nil
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

	fileLength := fileInfo.Size()
	numPages := uint32(fileLength / constants.PAGE_SIZE)

	if fileLength%constants.PAGE_SIZE != 0 {
		file.Close()
		return nil, fmt.Errorf("Db file is not a whole number of pages. Corrupt file.")
	}

	pager := &Pager{
		FileDescriptor: file,
		FileLength:     fileLength,
		NumPages:       numPages,
	}

	// Initialize all pages to nil
	for i := 0; i < constants.TABLE_MAX_PAGES; i++ {
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

    table := &Table{
        Pager:       pager,
        RootPageNum: 0,
    }

    if pager.NumPages == 0 {
        // New database file. Initialize page 0 as leaf node.
        rootNode, err := pager.getPage(0)
        if err != nil {
            return nil, err
        }

        btree.InitializeLeafNode(rootNode)
        btree.SetNodeRoot(rootNode, true)
    }

    return table, nil
}

func (p *Pager) getPage(pageNum uint32) ([]byte, error) {
	if pageNum > constants.TABLE_MAX_PAGES {
		return nil, fmt.Errorf("Tried to fetch page number out of bounds, %d > %d", pageNum, constants.TABLE_MAX_PAGES)
	}

	if p.Pages[pageNum] == nil {
		// Cache miss, Allocate meemory and load from file
		page := make([]byte, constants.PAGE_SIZE)
		numPages := p.FileLength / constants.PAGE_SIZE

		// We might have a partial page at the end of the file
		if p.FileLength%constants.PAGE_SIZE != 0 {
			numPages++
		}

		if int64(pageNum) < numPages {
			// Seek to the correct position in the file
			_, err := p.FileDescriptor.Seek(int64(pageNum)*constants.PAGE_SIZE, 0)
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

		if pageNum >= p.NumPages {
			p.NumPages = pageNum + 1
		}
	}

	return p.Pages[pageNum], nil
}

// pagerFlush writes a page to disk
func (p *Pager) pagerFlush(pageNum uint32, size uint32) error {
	if p.Pages[pageNum] == nil {
		return fmt.Errorf("Tried to flush null page")
	}

	// Seek to the correct position
	_, err := p.FileDescriptor.Seek(int64(pageNum)*constants.PAGE_SIZE, 0)
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

	// Flush all pages that are in memory
	for i := uint32(0); i < pager.NumPages; i++ {
		if pager.Pages[i] == nil {
			continue
		}

		err := pager.pagerFlush(i, constants.PAGE_SIZE)
		if err != nil {
			return err
		}

		pager.Pages[i] = nil
	}

	// Close the file
	err := pager.FileDescriptor.Close()
	if err != nil {
		return fmt.Errorf("Error closing db file: %v", err)
	}

	return nil
}

func leafNodeSplitAndInsert(cursor *Cursor, key uint32, value *Row) error {
	oldNode, err := cursor.Table.Pager.getPage(cursor.PageNum)
	if err != nil {
		return err
	}

	newPageNum := getUnusedPageNum(cursor.Table.Pager)
	newNode, err := cursor.Table.Pager.getPage(newPageNum)
	if err != nil {
		return err
	}

	btree.InitializeLeafNode(newNode)
	setNodeParent(newNode, nodeParent(oldNode))

	// All existing keys plus new key should be divided
	// evenly between old (left) and new (right) nodes.
	// Starting from the right, move each key to correct position.
	for i := int32(btree.LEAF_NODE_MAX_CELLS); i >= 0; i-- {
		var destinationNode []byte
		if i >= int32(btree.LEAF_NODE_LEFT_SPLIT_COUNT) {
			destinationNode = newNode
		} else {
			destinationNode = oldNode
		}

		indexWithinNode := uint32(i % int32(btree.LEAF_NODE_LEFT_SPLIT_COUNT))
		destination := btree.LeafNodeCell(destinationNode, indexWithinNode)

		if i == int32(cursor.CellNum) {
			// This is where the new cell goes
			btree.SetLeafNodeKey(destinationNode, indexWithinNode, key)
			serializeRow(value, btree.LeafNodeValue(destinationNode, indexWithinNode))
		} else if i > int32(cursor.CellNum) {
			// Move existing cell
			source := btree.LeafNodeCell(oldNode, uint32(i-1))
			copy(destination, source)
		} else {
			// Move existing cell
			source := btree.LeafNodeCell(oldNode, uint32(i))
			copy(destination, source)
		}
	}

	// Update cell counts
	btree.SetLeafNodeNumCells(oldNode, btree.LEAF_NODE_LEFT_SPLIT_COUNT)
	btree.SetLeafNodeNumCells(newNode, btree.LEAF_NODE_RIGHT_SPLIT_COUNT)

	if btree.IsNodeRoot(oldNode) {
		return createNewRoot(cursor.Table, newPageNum)
	} else {
		// We'll implement this in a later part
		return fmt.Errorf("Need to implement updating parent after split")
	}
}

func leafNodeInsert(cursor *Cursor, key uint32, value *Row) error {
	node, err := cursor.Table.Pager.getPage(cursor.PageNum)
	if err != nil {
		return err
	}

	numCells := btree.LeafNodeNumCells(node)
	if numCells >= btree.LEAF_NODE_MAX_CELLS {
		// Node full - split it
		return leafNodeSplitAndInsert(cursor, key, value)
	}

	if cursor.CellNum < numCells {
		// Make room for new cell
		for i := numCells; i > cursor.CellNum; i-- {
			destCell := btree.LeafNodeCell(node, i)
			srcCell := btree.LeafNodeCell(node, i-1)
			copy(destCell, srcCell)
		}
	}

	btree.SetLeafNodeNumCells(node, numCells+1)
	btree.SetLeafNodeKey(node, cursor.CellNum, key)
	serializeRow(value, btree.LeafNodeValue(node, cursor.CellNum))

	return nil
}

func serializeRow(source *Row, destination []byte) {
	binary.LittleEndian.PutUint32(destination[ID_OFFSET:], source.ID)

	copy(destination[USERNAME_OFFSET:], source.Username[:])
	copy(destination[EMAIL_OFFSET:], source.Email[:])
}

func deserializeRow(source []byte, destination *Row) {
	destination.ID = binary.LittleEndian.Uint32(source[ID_OFFSET:])

	copy(destination.Username[:], source[USERNAME_OFFSET:USERNAME_OFFSET+constants.COLUMN_USERNAME_SIZE])
	copy(destination.Email[:], source[EMAIL_OFFSET:EMAIL_OFFSET+constants.COLUMN_EMAIL_SIZE])
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
	switch inputBuffer.buffer {
	case ".exit":
		err := dbClose(table)
		if err != nil {
			fmt.Printf("Error closing database: %v\n", err)
		}
		fmt.Println("Bye!")
		os.Exit(0)
		return META_COMMAND_UNRECOGNIZED_COMMAND
	case ".btree":
		fmt.Println("Tree:")
		err := printTree(table.Pager, 0, 0)
		if err != nil {
			fmt.Printf("Error printing tree: %v\n", err)
		}
		return META_COMMAND_SUCCESS
	case ".constants":
		fmt.Println("Constants:")
		printConstants()
		return META_COMMAND_SUCCESS
	default:
		return META_COMMAND_UNRECOGNIZED_COMMAND
	}
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

	if len(username) > constants.COLUMN_USERNAME_SIZE {
		return PREPARE_STRING_TOO_LONG
	}

	copy(statement.RowToInsert.Username[:], username)

	email := tokens[3]

	if len(email) > constants.COLUMN_EMAIL_SIZE {
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
	node, err := table.Pager.getPage(table.RootPageNum)
	if err != nil {
		fmt.Printf("Error getting root page: %v\n", err)
		return EXECUTE_TABLE_FULL
	}

	numCells := btree.LeafNodeNumCells(node)

	rowToInsert := &statement.RowToInsert
	keyToInsert := rowToInsert.ID

	cursor, err := tableFind(table, keyToInsert)
	if err != nil {
		fmt.Printf("Error finding key: %v\n", err)
		return EXECUTE_TABLE_FULL
	}

	if cursor.CellNum < numCells {
		keyAtIndex := btree.LeafNodeKey(node, cursor.CellNum)
		if keyAtIndex == keyToInsert {
			return EXECUTE_DUPLICATE_KEY
		}
	}

	err = leafNodeInsert(cursor, rowToInsert.ID, rowToInsert)
	if err != nil {
		fmt.Printf("Error inserting: %v\n", err)
		return EXECUTE_TABLE_FULL
	}

	return EXECUTE_SUCCESS
}

func executeSelect(statement *Statement, table *Table) ExecuteResult {
	cursor, err := tableStart(table)
	if err != nil {
		fmt.Printf("Error getting cursor: %v\n", err)
		return EXECUTE_SUCCESS
	}

	var row Row
	for !cursor.EndOfTable {
		slot, err := cursorValue(cursor)
		if err != nil {
			fmt.Printf("Error getting cursor value: %v\n", err)
			cursor.EndOfTable = true
			continue
		}

		deserializeRow(slot, &row)
		printRow(&row)

		err = cursorAdvance(cursor)
		if err != nil {
			fmt.Printf("Error advancing cursor: %v\n", err)
			cursor.EndOfTable = true
		}
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

// =========
// DEBUG
// =========
func printConstants() {
	fmt.Printf("ROW_SIZE: %d\n", constants.ROW_SIZE)
	fmt.Printf("COMMON_NODE_HEADER_SIZE: %d\n", btree.COMMON_NODE_HEADER_SIZE)
	fmt.Printf("LEAF_NODE_HEADER_SIZE: %d\n", btree.LEAF_NODE_HEADER_SIZE)
	fmt.Printf("LEAF_NODE_CELL_SIZE: %d\n", btree.LEAF_NODE_CELL_SIZE)
	fmt.Printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", btree.LEAF_NODE_SPACE_FOR_CELLS)
	fmt.Printf("LEAF_NODE_MAX_CELLS: %d\n", btree.LEAF_NODE_MAX_CELLS)
}

func indent(level uint32) {
	for i := uint32(0); i < level; i++ {
		fmt.Print("  ")
	}
}

func printTree(pager *Pager, pageNum uint32, indentationLevel uint32) error {
	node, err := pager.getPage(pageNum)
	if err != nil {
		return err
	}

	switch btree.GetNodeType(node) {
	case btree.NODE_LEAF:
		numCells := btree.LeafNodeNumCells(node)
		indent(indentationLevel)
		fmt.Printf("- leaf (size %d)\n", numCells)
		for i := uint32(0); i < numCells; i++ {
			indent(indentationLevel + 1)
			fmt.Printf("  - key %d\n", btree.LeafNodeKey(node, i))
		}

	case btree.NODE_INTERNAL:
		numKeys := btree.InternalNodeNumKeys(node)
		indent(indentationLevel)
		fmt.Printf("- internal (size %d)\n", numKeys)
		for i := uint32(0); i < numKeys; i++ {
			child := btree.InternalNodeChild(node, i)
			err = printTree(pager, child, indentationLevel+1)
			if err != nil {
				return err
			}

			indent(indentationLevel + 1)
			fmt.Printf("- key %d\n", btree.InternalNodeKey(node, i))
		}

		rightChild := btree.InternalNodeRightChild(node)
		err = printTree(pager, rightChild, indentationLevel+1)
		if err != nil {
			return err
		}
	}

	return nil
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
		case EXECUTE_DUPLICATE_KEY:
			fmt.Println("Error: Duplicate key.")
		case EXECUTE_TABLE_FULL:
			fmt.Println("Error: Table full.")
		}
	}
}
