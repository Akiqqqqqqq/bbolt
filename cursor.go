package bbolt

import (
	"bytes"
	"fmt"
	"sort"
)

// Cursor represents an iterator that can traverse over all key/value pairs in a bucket
// in lexicographical order.
// Cursors see nested buckets with value == nil.
// Cursors can be obtained from a transaction and are valid as long as the transaction is open.
//
// Keys and values returned from the cursor are only valid for the life of the transaction.
//
// Changing data while traversing with a cursor may cause it to be invalidated
// and return unexpected keys and/or values. You must reposition your cursor
// after mutating data.
type Cursor struct {
	bucket *Bucket
	stack  []elemRef // Cursor.stack 中保存了查找对应 key 的路径, 栈顶保存了 key 所在的结点和位置
}

// Bucket returns the bucket that this cursor was created from.
func (c *Cursor) Bucket() *Bucket {
	return c.bucket
}

// First moves the cursor to the first item in the bucket and returns its key and value.
// If the bucket is empty then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
func (c *Cursor) First() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	k, v, flags := c.first()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

func (c *Cursor) first() (key []byte, value []byte, flags uint32) {
	c.stack = c.stack[:0]
	p, n := c.bucket.pageNode(c.bucket.root)
	c.stack = append(c.stack, elemRef{page: p, node: n, index: 0})
	c.goToFirstElementOnTheStack()

	// If we land on an empty page then move to the next value.
	// https://github.com/boltdb/bolt/issues/450
	if c.stack[len(c.stack)-1].count() == 0 {
		c.next()
	}

	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil, flags
	}
	return k, v, flags
}

// Last moves the cursor to the last item in the bucket and returns its key and value.
// If the bucket is empty then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
func (c *Cursor) Last() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	c.stack = c.stack[:0]
	p, n := c.bucket.pageNode(c.bucket.root)
	ref := elemRef{page: p, node: n}
	ref.index = ref.count() - 1
	c.stack = append(c.stack, ref)
	c.last()

	// If this is an empty page (calling Delete may result in empty pages)
	// we call prev to find the last page that is not empty
	for len(c.stack) > 0 && c.stack[len(c.stack)-1].count() == 0 {
		c.prev()
	}

	if len(c.stack) == 0 {
		return nil, nil
	}

	k, v, flags := c.keyValue()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Next moves the cursor to the next item in the bucket and returns its key and value.
// If the cursor is at the end of the bucket then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
func (c *Cursor) Next() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	k, v, flags := c.next()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Prev moves the cursor to the previous item in the bucket and returns its key and value.
// If the cursor is at the beginning of the bucket then a nil key and value are returned.
// The returned key and value are only valid for the life of the transaction.
func (c *Cursor) Prev() (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")
	k, v, flags := c.prev()
	if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Seek moves the cursor to a given key using a b-tree search and returns it.
// If the key does not exist then the next key is used. If no keys
// follow, a nil key is returned.
// The returned key and value are only valid for the life of the transaction.
func (c *Cursor) Seek(seek []byte) (key []byte, value []byte) {
	_assert(c.bucket.tx.db != nil, "tx closed")

	k, v, flags := c.seek(seek)

	// If we ended up after the last element of a page then move to the next one.
	if ref := &c.stack[len(c.stack)-1]; ref.index >= ref.count() {
		k, v, flags = c.next()
	}

	if k == nil {
		return nil, nil
	} else if (flags & uint32(bucketLeafFlag)) != 0 {
		return k, nil
	}
	return k, v
}

// Delete removes the current key/value under the cursor from the bucket.
// Delete fails if current key/value is a bucket or if the transaction is not writable.
func (c *Cursor) Delete() error {
	if c.bucket.tx.db == nil {
		return ErrTxClosed
	} else if !c.bucket.Writable() {
		return ErrTxNotWritable
	}

	key, _, flags := c.keyValue()
	// Return an error if current value is a bucket.
	if (flags & bucketLeafFlag) != 0 {
		return ErrIncompatibleValue
	}
	c.node().del(key)

	return nil
}

// seek moves the cursor to a given key and returns it.
// If the key does not exist then the next key is used.
func (c *Cursor) seek(seek []byte) (key []byte, value []byte, flags uint32) {
	// Start from root page/node and traverse to correct page.
	c.stack = c.stack[:0]         // 清空stack
	c.search(seek, c.bucket.root) // c.bucket.root = 3; search只是拿到seek的index；c.bucket.root其实是c.Bucket.bucket.root

	// If this is a bucket then return a nil value.
	return c.keyValue()
}

// first moves the cursor to the first leaf element under the last page in the stack.
func (c *Cursor) goToFirstElementOnTheStack() {
	for {
		// Exit when we hit a leaf page.
		var ref = &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		// Keep adding pages pointing to the first element to the stack.
		var pgId pgid
		if ref.node != nil {
			pgId = ref.node.inodes[ref.index].pgid
		} else {
			pgId = ref.page.branchPageElement(uint16(ref.index)).pgid
		}
		p, n := c.bucket.pageNode(pgId)
		c.stack = append(c.stack, elemRef{page: p, node: n, index: 0})
	}
}

// last moves the cursor to the last leaf element under the last page in the stack.
func (c *Cursor) last() {
	for {
		// Exit when we hit a leaf page.
		ref := &c.stack[len(c.stack)-1]
		if ref.isLeaf() {
			break
		}

		// Keep adding pages pointing to the last element in the stack.
		var pgId pgid
		if ref.node != nil {
			pgId = ref.node.inodes[ref.index].pgid
		} else {
			pgId = ref.page.branchPageElement(uint16(ref.index)).pgid
		}
		p, n := c.bucket.pageNode(pgId)

		var nextRef = elemRef{page: p, node: n}
		nextRef.index = nextRef.count() - 1
		c.stack = append(c.stack, nextRef)
	}
}

// next moves to the next leaf element and returns the key and value.
// If the cursor is at the last leaf element then it stays there and returns nil.
func (c *Cursor) next() (key []byte, value []byte, flags uint32) {
	for {
		// Attempt to move over one element until we're successful.
		// Move up the stack as we hit the end of each page in our stack.
		var i int
		for i = len(c.stack) - 1; i >= 0; i-- {
			elem := &c.stack[i]
			if elem.index < elem.count()-1 {
				elem.index++
				break
			}
		}

		// If we've hit the root page then stop and return. This will leave the
		// cursor on the last element of the last page.
		if i == -1 {
			return nil, nil, 0
		}

		// Otherwise start from where we left off in the stack and find the
		// first element of the first leaf page.
		c.stack = c.stack[:i+1]
		c.goToFirstElementOnTheStack()

		// If this is an empty page then restart and move back up the stack.
		// https://github.com/boltdb/bolt/issues/450
		if c.stack[len(c.stack)-1].count() == 0 {
			continue
		}

		return c.keyValue()
	}
}

// prev moves the cursor to the previous item in the bucket and returns its key and value.
// If the cursor is at the beginning of the bucket then a nil key and value are returned.
func (c *Cursor) prev() (key []byte, value []byte, flags uint32) {
	// Attempt to move back one element until we're successful.
	// Move up the stack as we hit the beginning of each page in our stack.
	for i := len(c.stack) - 1; i >= 0; i-- {
		elem := &c.stack[i]
		if elem.index > 0 {
			elem.index--
			break
		}
		c.stack = c.stack[:i]
	}

	// If we've hit the end then return nil.
	if len(c.stack) == 0 {
		return nil, nil, 0
	}

	// Move down the stack to find the last element of the last leaf under this branch.
	c.last()
	return c.keyValue()
}

// search recursively performs a binary search against a given page/node until it finds a given key.
func (c *Cursor) search(key []byte, pgId pgid) { // (seek, c.Bucket.bucket.root(pgid=3,就是指向leafPage))
	p, n := c.bucket.pageNode(pgId)                               // 拿到leafPage和node(nil)
	if p != nil && (p.flags&(branchPageFlag|leafPageFlag)) == 0 { // 必须是branchPage 或 leafPage
		panic(fmt.Sprintf("invalid page type: %d: %x", p.id, p.flags))
	}
	e := elemRef{page: p, node: n}
	c.stack = append(c.stack, e) // 压到栈顶（往后递归会一直append到栈顶）最后一个e就是那个seek的e，e.p就是seek的page，e.n就是seek的node

	// If we're on a leaf page/node then find the specific node.
	if e.isLeaf() { // 如果是一个叶子 页/node，那就执行一个非递归的简单二分搜索（因为已经到叶子了，不能再往下走了）这里是递归终止的地方
		c.nsearch(key)
		return // 找到leaf，搜索后，不管结果，一定会返回
	}

	if n != nil { // 有node就搜node
		c.searchNode(key, n) // 递归搜索
		return
	} // 没node就搜page
	c.searchPage(key, p) // 递归搜索
}

func (c *Cursor) searchNode(key []byte, n *node) {
	var exact bool
	index := sort.Search(len(n.inodes), func(i int) bool {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		ret := bytes.Compare(n.inodes[i].key, key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// Recursively search to the next page.
	c.search(key, n.inodes[index].pgid) // 递归
}

func (c *Cursor) searchPage(key []byte, p *page) {
	// Binary search for the correct range.
	inodes := p.branchPageElements()

	var exact bool
	index := sort.Search(int(p.count), func(i int) bool {
		// TODO(benbjohnson): Optimize this range search. It's a bit hacky right now.
		// sort.Search() finds the lowest index where f() != -1 but we need the highest index.
		ret := bytes.Compare(inodes[i].key(), key)
		if ret == 0 {
			exact = true
		}
		return ret != -1
	})
	if !exact && index > 0 {
		index--
	}
	c.stack[len(c.stack)-1].index = index

	// Recursively search to the next page.
	c.search(key, inodes[index].pgid) // 递归，pgid从root=3 变成了branchPageElements[index].pgid
}

// nsearch searches the leaf node on the top of the stack for a key.
func (c *Cursor) nsearch(key []byte) {
	e := &c.stack[len(c.stack)-1] // 这里面有一个root的elemRef
	p, n := e.page, e.node        // node是nil

	// If we have a node then search its inodes.
	if n != nil { // 有node就搜node
		index := sort.Search(len(n.inodes), func(i int) bool {
			return bytes.Compare(n.inodes[i].key, key) != -1
		})
		e.index = index
		return
	}

	// If we have a page then search its leaf elements. // 没node就搜page
	inodes := p.leafPageElements() // 取回在p后面的一个列表的leafPageElement，也就是pageElement数组
	index := sort.Search(int(p.count), func(i int) bool {
		return bytes.Compare(inodes[i].key(), key) != -1 // 搜索这个key
	}) // 搜索这个pageElement数组，找到key相同的
	e.index = index // 赋值到e(elemRef.index)，也就是stack的顶层元素
}

// keyValue returns the key and value of the current leaf element.
func (c *Cursor) keyValue() ([]byte, []byte, uint32) { // 如果啥也没找到，就返回 nil, nil, 0
	ref := &c.stack[len(c.stack)-1] // 取出elemRef，也就是seek，就是stack的顶层元素

	// If the cursor is pointing to the end of page/node then return nil.
	if ref.count() == 0 || ref.index >= ref.count() { // index是seek的索引，如果index大于kv数量，则是搜到尾巴了
		return nil, nil, 0 // 没找到
	}

	// Retrieve value from node.
	if ref.node != nil { // 如果node不为空，则从node拿
		inode := &ref.node.inodes[ref.index]
		return inode.key, inode.value, inode.flags // 直接从node获取目标kv值
	}

	// Or retrieve value from page. 如果node为空，则从page拿
	elem := ref.page.leafPageElement(uint16(ref.index))
	return elem.key(), elem.value(), elem.flags // 从page获取目标kv值
}

// node returns the node that the cursor is currently positioned on.
func (c *Cursor) node() *node {
	_assert(len(c.stack) > 0, "accessing a node with a zero-length cursor stack")

	// If the top of the stack is a leaf node then just return it.
	if ref := &c.stack[len(c.stack)-1]; ref.node != nil && ref.isLeaf() { // 拿到栈顶元素
		return ref.node // 如果是node且是leaf，就直接返回node
	}

	// Start from root and traverse down the hierarchy.
	var n = c.stack[0].node // 从根(root=3)开始；栈底，第一个进去的，就是root
	if n == nil {
		n = c.bucket.node(c.stack[0].page.id, nil) // 根都没有，创建一个node；比如最开始的时候；这里很重要！   每一个node的inode，都指向对应page的mmap内存区
	}
	for _, ref := range c.stack[:len(c.stack)-1] { // 从0到len(c.stack)-1遍历stack上的元素；相当于再走一遍stack上面的路径
		_assert(!n.isLeaf, "expected branch node")
		n = n.childAt(ref.index)
	}
	_assert(n.isLeaf, "expected leaf node")
	return n
}

// elemRef represents a reference to an element on a given page/node.
type elemRef struct {
	page  *page
	node  *node
	index int
}

// isLeaf returns whether the ref is pointing at a leaf page/node.
func (r *elemRef) isLeaf() bool {
	if r.node != nil {
		return r.node.isLeaf
	}
	return (r.page.flags & leafPageFlag) != 0 // 如果flags==leafPageFlag，则返回true
}

// count returns the number of inodes or page elements.
func (r *elemRef) count() int {
	if r.node != nil {
		return len(r.node.inodes)
	}
	return int(r.page.count) // 叶子节点页count代表本节点存储的kv对数量
}
