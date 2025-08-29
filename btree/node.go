package btree

import (
	"encoding/binary"
	"toydb/constants"
)

// Node types
type NodeType uint8

const (
	NODE_INTERNAL NodeType = iota
	NODE_LEAF
)

// Common Node Header Layout
const (
	NODE_TYPE_SIZE          = 1 // size of uint8
	NODE_TYPE_OFFSET        = 0
	IS_ROOT_SIZE            = 1 // size of uint8 (boolean)
	IS_ROOT_OFFSET          = NODE_TYPE_SIZE
	PARENT_POINTER_SIZE     = 4 // size of uint32
	PARENT_POINTER_OFFSET   = IS_ROOT_OFFSET + IS_ROOT_SIZE
	COMMON_NODE_HEADER_SIZE = NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE
)

// Leaf Node Header Layout
const (
	LEAF_NODE_NUM_CELLS_SIZE   = 4 // size of uint32
	LEAF_NODE_NUM_CELLS_OFFSET = COMMON_NODE_HEADER_SIZE
	LEAF_NODE_HEADER_SIZE      = COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE
)

// Leaf Node Body Layout
const (
	LEAF_NODE_KEY_SIZE        = 4 // size of uint32
	LEAF_NODE_KEY_OFFSET      = 0
	LEAF_NODE_VALUE_SIZE      = constants.ROW_SIZE
	LEAF_NODE_VALUE_OFFSET    = LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE
	LEAF_NODE_CELL_SIZE       = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE
	LEAF_NODE_SPACE_FOR_CELLS = constants.PAGE_SIZE - LEAF_NODE_HEADER_SIZE
	LEAF_NODE_MAX_CELLS       = LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE
)

// Node header access functions
func GetNodeType(node []byte) NodeType {
	return NodeType(node[NODE_TYPE_OFFSET])
}

func SetNodeType(node []byte, nodeType NodeType) {
	node[NODE_TYPE_OFFSET] = uint8(nodeType)
}

func IsNodeRoot(node []byte) bool {
	return node[IS_ROOT_OFFSET] != 0
}

func SetNodeRoot(node []byte, isRoot bool) {
	if isRoot {
		node[IS_ROOT_OFFSET] = 1
	} else {
		node[IS_ROOT_OFFSET] = 0
	}
}

// Leaf node access functions
func LeafNodeNumCells(node []byte) uint32 {
	return binary.LittleEndian.Uint32(node[LEAF_NODE_NUM_CELLS_OFFSET:])
}

func SetLeafNodeNumCells(node []byte, numCells uint32) {
	binary.LittleEndian.PutUint32(node[LEAF_NODE_NUM_CELLS_OFFSET:], numCells)
}

func LeafNodeCell(node []byte, cellNum uint32) []byte {
	offset := LEAF_NODE_HEADER_SIZE + cellNum*LEAF_NODE_CELL_SIZE
	return node[offset : offset+LEAF_NODE_CELL_SIZE]
}

func LeafNodeKey(node []byte, cellNum uint32) uint32 {
	cell := LeafNodeCell(node, cellNum)
	return binary.LittleEndian.Uint32(cell[LEAF_NODE_KEY_OFFSET:])
}

func SetLeafNodeKey(node []byte, cellNum uint32, key uint32) {
	cell := LeafNodeCell(node, cellNum)
	binary.LittleEndian.PutUint32(cell[LEAF_NODE_KEY_OFFSET:], key)
}

func LeafNodeValue(node []byte, cellNum uint32) []byte {
	cell := LeafNodeCell(node, cellNum)
	return cell[LEAF_NODE_VALUE_OFFSET : LEAF_NODE_VALUE_OFFSET+LEAF_NODE_VALUE_SIZE]
}

func InitializeLeafNode(node []byte) {
	SetNodeType(node, NODE_LEAF)
	SetNodeRoot(node, false)
	SetLeafNodeNumCells(node, 0)
}
