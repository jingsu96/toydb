package btree

import (
	"encoding/binary"
	"toydb/constants"
	"fmt"
)

// Node types
type NodeType uint8

const (
	NODE_INTERNAL NodeType = iota
	NODE_LEAF
)

// Internal Node Header Layout
const (
	INTERNAL_NODE_NUM_KEYS_SIZE      = 4
	INTERNAL_NODE_NUM_KEYS_OFFSET    = COMMON_NODE_HEADER_SIZE
	INTERNAL_NODE_RIGHT_CHILD_SIZE   = 4
	INTERNAL_NODE_RIGHT_CHILD_OFFSET = INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE
	INTERNAL_NODE_HEADER_SIZE        = COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE
)

// Internal Node Body Layout
const (
	INTERNAL_NODE_KEY_SIZE   = 4
	INTERNAL_NODE_CHILD_SIZE = 4
	INTERNAL_NODE_CELL_SIZE  = INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE
	INTERNAL_NODE_MAX_CELLS  = (constants.PAGE_SIZE - INTERNAL_NODE_HEADER_SIZE) / INTERNAL_NODE_CELL_SIZE
)

// Split counts for leaf nodes
const (
	LEAF_NODE_RIGHT_SPLIT_COUNT = (LEAF_NODE_MAX_CELLS + 1) / 2
	LEAF_NODE_LEFT_SPLIT_COUNT  = (LEAF_NODE_MAX_CELLS + 1) - LEAF_NODE_RIGHT_SPLIT_COUNT
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

func InternalNodeNumKeys(node []byte) uint32 {
	return binary.LittleEndian.Uint32(node[INTERNAL_NODE_NUM_KEYS_OFFSET:])
}

func SetInternalNodeNumKeys(node []byte, numKeys uint32) {
	binary.LittleEndian.PutUint32(node[INTERNAL_NODE_NUM_KEYS_OFFSET:], numKeys)
}

func InternalNodeRightChild(node []byte) uint32 {
	return binary.LittleEndian.Uint32(node[INTERNAL_NODE_RIGHT_CHILD_OFFSET:])
}

func SetInternalNodeRightChild(node []byte, pageNum uint32) {
	binary.LittleEndian.PutUint32(node[INTERNAL_NODE_RIGHT_CHILD_OFFSET:], pageNum)
}

func InternalNodeCell(node []byte, cellNum uint32) []byte {
	offset := INTERNAL_NODE_HEADER_SIZE + cellNum*INTERNAL_NODE_CELL_SIZE
	return node[offset : offset+INTERNAL_NODE_CELL_SIZE]
}

func InternalNodeChild(node []byte, childNum uint32) uint32 {
	numKeys := InternalNodeNumKeys(node)

	if childNum > numKeys {
		panic(fmt.Sprintf("Tried to access child_num %d > num_keys %d", childNum, numKeys))
	} else if childNum == numKeys {
		return InternalNodeRightChild(node)
	} else {
		cell := InternalNodeCell(node, childNum)
		return binary.LittleEndian.Uint32(cell)
	}
}

func SetInternalNodeChild(node []byte, childNum uint32, child uint32) {
	numKeys := InternalNodeNumKeys(node)
	if childNum > numKeys {
		panic(fmt.Sprintf("Tried to set child_num %d > num_keys %d", childNum, numKeys))
	} else if childNum == numKeys {
		SetInternalNodeRightChild(node, child)
	} else {
		cell := InternalNodeCell(node, childNum)
		binary.LittleEndian.PutUint32(cell, child)
	}
}

func InternalNodeKey(node []byte, keyNum uint32) uint32 {
	cell := InternalNodeCell(node, keyNum)
	return binary.LittleEndian.Uint32(cell[INTERNAL_NODE_CHILD_SIZE:])
}

func SetInternalNodeKey(node []byte, keyNum uint32, key uint32) {
	cell := InternalNodeCell(node, keyNum)
	binary.LittleEndian.PutUint32(cell[INTERNAL_NODE_CHILD_SIZE:], key)
}

func InitializeInternalNode(node []byte) {
	SetNodeType(node, NODE_INTERNAL)
	SetNodeRoot(node, false)
	SetInternalNodeNumKeys(node, 0)
}
