package tree

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
}

func NewRbTree() *RbTree {
	return &RbTree{}
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
	return tree.count
}

func (tree *RbTree) IsEmpty() bool {
	return tree.root == nil
}

func (tree *RbTree) Min() (RbKey, interface{}) {
	if tree.root != nil {
		result := min(tree.root)
		return result.key, result.value
	}
	return nil, nil
}

func (tree *RbTree) Max() (RbKey, interface{}) {
	if tree.root != nil {
		result := max(tree.root)
		return result.key, result.value
	}
	return nil, nil
}

// Floor returns the largest key in the tree less than or equal to key
func (tree *RbTree) Floor(key RbKey) (RbKey, interface{}) {
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
	if key != nil && tree.root != nil {
		node := ceiling(tree.root, key)
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

	tree.version++
	tree.count--
	tree.root = tree.deleteNode(tree.root, key)
	if tree.root != nil {
		tree.root.colour = black
	}
}

type Uint64Key uint64

// ComparedTo compares the given RbKey with its self
func (ikey *Uint64Key) ComparedTo(key RbKey) KeyComparison {
	key1 := uint64(*ikey)
	key2 := uint64(*key.(*Uint64Key))
	switch {
	case key1 > key2:
		return KeyIsGreater
	case key1 < key2:
		return KeyIsLess
	default:
		return KeysAreEqual
	}
}
