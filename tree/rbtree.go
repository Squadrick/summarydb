/*
* The MIT License (MIT)
* =====================
*
* Copyright (c) 2015, Cagatay Dogan
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in
* all copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
* THE SOFTWARE.
 */

package tree

import "sync"

type KeyComparison int8

const (
	// KeyIsLess is returned as result of key comparison if the first key is less than the second key
	KeyIsLess KeyComparison = iota - 1
	// KeysAreEqual is returned as result of key comparison if the first key is equal to the second key
	KeysAreEqual
	// KeyIsGreater is returned as result of key comparison if the first key is greater than the second key
	KeyIsGreater
)

const (
	red   = byte(0)
	black = byte(1)
)

type RbKey interface {
	ComparedTo(key RbKey) KeyComparison
}

type rbNode struct {
	key    RbKey
	value  interface{}
	colour byte
	left   *rbNode
	right  *rbNode
}

type RbTree struct {
	root    *rbNode
	count   int
	version uint32
	mutex   *sync.RWMutex
}

func NewRbTree() *RbTree {
	return &RbTree{
		mutex: &sync.RWMutex{},
	}
}

func newRbNode(key RbKey, value interface{}) *rbNode {
	return &rbNode{
		key:    key,
		value:  value,
		colour: red,
	}
}

func isRed(node *rbNode) bool {
	return node != nil && node.colour == red
}

func isBlack(node *rbNode) bool {
	return node != nil && node.colour == black
}

func min(node *rbNode) *rbNode {
	if node != nil {
		for node.left != nil {
			node = node.left
		}
	}
	return node
}

func max(node *rbNode) *rbNode {
	if node != nil {
		for node.right != nil {
			node = node.right
		}
	}
	return node
}

func floor(node *rbNode, key RbKey) *rbNode {
	if node == nil {
		return node
	}

	switch key.ComparedTo(node.key) {
	case KeysAreEqual:
		return node
	case KeyIsLess:
		return floor(node.left, key)
	default: // KeyIsGreater
		fn := floor(node.right, key)
		if fn != nil {
			return fn
		}

		return node
	}
}

func ceiling(node *rbNode, key RbKey) *rbNode {
	if node == nil {
		return nil
	}

	switch key.ComparedTo(node.key) {
	case KeysAreEqual:
		return node
	case KeyIsGreater:
		return ceiling(node.right, key)
	default: // KeyIsLess
		cn := ceiling(node.left, key)
		if cn != nil {
			return cn
		}
		return node
	}
}

func higher(node *rbNode, key RbKey) *rbNode {
	if node == nil {
		return nil
	}

	switch key.ComparedTo(node.key) {
	case KeysAreEqual:
		fallthrough
	case KeyIsGreater:
		return higher(node.right, key)
	default:
		cn := higher(node.left, key)
		if cn != nil {
			return cn
		}
		return node
	}
}

func flipSingleNodeColour(node *rbNode) {
	if node.colour == black {
		node.colour = red
	} else {
		node.colour = black
	}
}

// Flips the colours of node, and its two children
func colourFlip(node *rbNode) {
	flipSingleNodeColour(node)
	flipSingleNodeColour(node.left)
	flipSingleNodeColour(node.right)
}

func rotateLeft(node *rbNode) *rbNode {
	child := node.right
	node.right = child.left
	child.left = node
	child.colour = node.colour
	node.colour = red
	return child
}

func rotateRight(node *rbNode) *rbNode {
	child := node.left
	node.left = child.right
	child.right = node
	child.colour = node.colour
	node.colour = red
	return child
}

// moveRedLeft makes node.left or one of its children red,
// assuming that node is red and both children are black.
func moveRedLeft(node *rbNode) *rbNode {
	colourFlip(node)

	if isRed(node.right.left) {
		node.right = rotateRight(node.right)
		node = rotateLeft(node)
		colourFlip(node)
	}
	return node
}

// moveRedRight makes node.right or one of its children red,
// assuming that node is red and both children are black.
func moveRedRight(node *rbNode) *rbNode {
	colourFlip(node)
	if isRed(node.left.left) {
		node = rotateRight(node)
		colourFlip(node)
	}
	return node
}

func balance(node *rbNode) *rbNode {
	if isRed(node.right) {
		node = rotateLeft(node)
	}

	if isRed(node.left) && isRed(node.left.left) {
		node = rotateRight(node)
	}
	if isRed(node.left) && isRed(node.right) {
		colourFlip(node)
	}
	return node
}

func deleteMin(node *rbNode) *rbNode {
	if node.left == nil {
		return nil
	}

	if isBlack(node.left) && !isRed(node.left.left) {
		node = moveRedLeft(node)
	}
	node.left = deleteMin(node.left)
	return balance(node)
}

func (tree *RbTree) Count() int {
	tree.mutex.RLock()
	defer tree.mutex.RUnlock()
	return tree.count
}

func (tree *RbTree) IsEmpty() bool {
	return tree.root == nil
}

func (tree *RbTree) Min() (RbKey, interface{}) {
	tree.mutex.RLock()
	defer tree.mutex.RUnlock()
	if tree.root != nil {
		result := min(tree.root)
		return result.key, result.value
	}
	return nil, nil
}

func (tree *RbTree) Max() (RbKey, interface{}) {
	tree.mutex.RLock()
	defer tree.mutex.RUnlock()
	if tree.root != nil {
		result := max(tree.root)
		return result.key, result.value
	}
	return nil, nil
}

// Floor returns the largest key in the tree less than or equal to key
func (tree *RbTree) Floor(key RbKey) (RbKey, interface{}) {
	tree.mutex.RLock()
	defer tree.mutex.RUnlock()
	if key != nil && tree.root != nil {
		node := floor(tree.root, key)
		if node == nil {
			return nil, nil
		}
		return node.key, node.value
	}
	return nil, nil
}

// Ceiling returns the smallest key in the tree greater than or equal to key
func (tree *RbTree) Ceiling(key RbKey) (RbKey, interface{}) {
	tree.mutex.RLock()
	defer tree.mutex.RUnlock()
	if key != nil && tree.root != nil {
		node := ceiling(tree.root, key)
		if node == nil {
			return nil, nil
		}
		return node.key, node.value
	}
	return nil, nil
}

// Higher returns the smallest key in the tree strictly greater than key
func (tree *RbTree) Higher(key RbKey) (RbKey, interface{}) {
	tree.mutex.RLock()
	defer tree.mutex.RUnlock()
	if key != nil && tree.root != nil {
		node := higher(tree.root, key)
		if node == nil {
			return nil, nil
		}
		return node.key, node.value
	}
	return nil, nil
}

func (tree *RbTree) find(key RbKey) *rbNode {
	for node := tree.root; node != nil; {
		switch key.ComparedTo(node.key) {
		case KeyIsLess:
			node = node.left
		case KeyIsGreater:
			node = node.right
		default:
			return node
		}
	}
	return nil
}

func (tree *RbTree) Get(key RbKey) (interface{}, bool) {
	tree.mutex.RLock()
	defer tree.mutex.RUnlock()
	if key != nil && tree.root != nil {
		node := tree.find(key)
		if node != nil {
			return node.value, true
		}
	}
	return nil, false
}

func (tree *RbTree) Exists(key RbKey) bool {
	_, found := tree.Get(key)
	return found
}

// insertNode adds the given key and value into the node
func (tree *RbTree) insertNode(node *rbNode, key RbKey, value interface{}) *rbNode {
	if node == nil {
		tree.count++
		return newRbNode(key, value)
	}

	switch key.ComparedTo(node.key) {
	case KeyIsLess:
		node.left = tree.insertNode(node.left, key, value)
	case KeyIsGreater:
		node.right = tree.insertNode(node.right, key, value)
	default:
		node.value = value
	}
	return balance(node)
}

// Insert inserts the given key and value into the tree
func (tree *RbTree) Insert(key RbKey, value interface{}) {
	tree.mutex.Lock()
	defer tree.mutex.Unlock()
	if key != nil {
		tree.version++
		tree.root = tree.insertNode(tree.root, key, value)
		tree.root.colour = black
	}
}

// deleteNode deletes the given key from the node
func (tree *RbTree) deleteNode(node *rbNode, key RbKey) *rbNode {
	if node == nil {
		return nil
	}

	cmp := key.ComparedTo(node.key)
	if cmp == KeyIsLess {
		if isBlack(node.left) && !isRed(node.left.left) {
			node = moveRedLeft(node)
		}
		node.left = tree.deleteNode(node.left, key)
	} else {
		if isRed(node.left) {
			node = rotateRight(node)
		}

		if isBlack(node.right) && !isRed(node.right.left) {
			node = moveRedRight(node)
		}

		if key.ComparedTo(node.key) != KeysAreEqual {
			node.right = tree.deleteNode(node.right, key)
		} else {
			if node.right == nil {
				return nil
			}

			rm := min(node.right)
			node.key = rm.key
			node.value = rm.value
			node.right = deleteMin(node.right)

			rm.left = nil
			rm.right = nil
		}
	}
	return balance(node)
}

// Delete deletes the given key from the tree
func (tree *RbTree) Delete(key RbKey) {
	if !tree.Exists(key) {
		// adds an expensive look-up overhead
		// this is only needed to calculate count
		return
	}
	tree.mutex.Lock()
	defer tree.mutex.Unlock()

	tree.version++
	tree.count--
	tree.root = tree.deleteNode(tree.root, key)
	if tree.root != nil {
		tree.root.colour = black
	}
}

type OrderedMap struct {
	m    map[RbKey]interface{}
	keys []RbKey
}

func NewOrderedMap() *OrderedMap {
	return &OrderedMap{
		m:    make(map[RbKey]interface{}),
		keys: make([]RbKey, 0),
	}
}

func (oMap *OrderedMap) Set(key RbKey, value interface{}) {
	oMap.m[key] = value
	oMap.keys = append(oMap.keys, key)
}

func (oMap *OrderedMap) Get(key RbKey) (interface{}, bool) {
	value, ok := oMap.m[key]
	return value, ok
}

func (oMap *OrderedMap) GetMap() map[RbKey]interface{} {
	return oMap.m
}

func (oMap *OrderedMap) GetKeys() []RbKey {
	return oMap.keys
}

type RbTreeCallback func(RbKey, interface{}) bool

func traverseAll(node *rbNode, callback RbTreeCallback) bool {
	if node == nil {
		return false
	}

	if node.left != nil {
		shouldTerminate := traverseAll(node.left, callback)
		if shouldTerminate {
			return true
		}
	}

	shouldTerminate := callback(node.key, node.value)
	if shouldTerminate {
		return true
	}

	if node.right != nil {
		shouldTerminate := traverseAll(node.right, callback)
		if shouldTerminate {
			return true
		}
	}
	return false
}

func (tree *RbTree) Map(fn RbTreeCallback) {
	if tree.IsEmpty() {
		return
	}

	tree.mutex.RLock()
	defer tree.mutex.RUnlock()

	traverseAll(tree.root, fn)
}

func (tree *RbTree) GetDenseMap() *OrderedMap {
	if tree.IsEmpty() {
		return nil
	}
	tree.mutex.RLock()
	defer tree.mutex.RUnlock()

	denseMap := NewOrderedMap()
	traverseAll(tree.root, func(key RbKey, value interface{}) bool {
		denseMap.Set(key, value)
		return false
	})
	return denseMap
}

// TODO(squadrick): Replace RbTree API with int64 instead of RbKey interface
type Int64Key int64

// ComparedTo compares the given RbKey with its self
func (ikey *Int64Key) ComparedTo(key RbKey) KeyComparison {
	key1 := int64(*ikey)
	key2 := int64(*key.(*Int64Key))
	switch {
	case key1 > key2:
		return KeyIsGreater
	case key1 < key2:
		return KeyIsLess
	default:
		return KeysAreEqual
	}
}
