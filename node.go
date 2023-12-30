package bbolt

import (
	"bytes"
	"fmt"
	"sort"
	"unsafe"
)

// node represents an in-memory, deserialized page.
type node struct {
	bucket     *Bucket // 指针
	isLeaf     bool    // 区分 branch node 和 leaf node
	unbalanced bool
	spilled    bool
	key        []byte // 该 node 的起始 key
	pgid       pgid
	parent     *node  // 父节点
	children   nodes  // 子结点
	inodes     inodes // 存放 node 的数据，指向mmap
}

// root returns the top-level node this node is attached to.
func (n *node) root() *node {
	if n.parent == nil {
		return n
	}
	return n.parent.root() // 递归回溯找到root
}

// minKeys returns the minimum number of inodes this node should have.
func (n *node) minKeys() int {
	if n.isLeaf {
		return 1
	}
	return 2
}

// size returns the size of the node after serialization.
func (n *node) size() int {
	sz, elsz := pageHeaderSize, n.pageElementSize() // pageHeader, element
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + uintptr(len(item.key)) + uintptr(len(item.value)) // 看上去是：一个pageHeader + N个（pageElement + key + value）
	}
	return int(sz)
}

// sizeLessThan returns true if the node is less than a given size.
// This is an optimization to avoid calculating a large node when we only need
// to know if it fits inside a certain page size.
func (n *node) sizeLessThan(v uintptr) bool {
	sz, elsz := pageHeaderSize, n.pageElementSize()
	for i := 0; i < len(n.inodes); i++ {
		item := &n.inodes[i]
		sz += elsz + uintptr(len(item.key)) + uintptr(len(item.value))
		if sz >= v {
			return false
		}
	}
	return true
}

// pageElementSize returns the size of each page element based on the type of node.
func (n *node) pageElementSize() uintptr {
	if n.isLeaf {
		return leafPageElementSize
	}
	return branchPageElementSize
}

// childAt returns the child node at a given index.
func (n *node) childAt(index int) *node {  // 这里也会去填充bucket.nodes
	if n.isLeaf {  // n不能是leaf
		panic(fmt.Sprintf("invalid childAt(%d) on a leaf node", index))
	}
	return n.bucket.node(n.inodes[index].pgid, n) // 根据pgId、和node本身，得到、或者创建并存入这个node到bucket
}

// childIndex returns the index of a given child node.
func (n *node) childIndex(child *node) int {
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, child.key) != -1 })
	return index
}

// numChildren returns the number of children.
func (n *node) numChildren() int {
	return len(n.inodes) // inodes和nodes数量一样？
}

// nextSibling returns the next node with the same parent.
func (n *node) nextSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index >= n.parent.numChildren()-1 {
		return nil
	}
	return n.parent.childAt(index + 1)
}

// prevSibling returns the previous node with the same parent.
func (n *node) prevSibling() *node {
	if n.parent == nil {
		return nil
	}
	index := n.parent.childIndex(n)
	if index == 0 {
		return nil
	}
	return n.parent.childAt(index - 1)
}

// put inserts a key/value.  写入node.inodes
func (n *node) put(oldKey, newKey, value []byte, pgId pgid, flags uint32) {  // 在leaf上put k,v
	if pgId >= n.bucket.tx.meta.pgid { // 所以pgId不能高于tx.meta.pgid
		panic(fmt.Sprintf("pgId (%d) above high water mark (%d)", pgId, n.bucket.tx.meta.pgid))
	} else if len(oldKey) <= 0 {
		panic("put: zero-length old key")
	} else if len(newKey) <= 0 {
		panic("put: zero-length new key")
	}

	// Find insertion index.
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, oldKey) != -1 })   // 在这个inodes（leaf）上再搜一次key

	// Add capacity and shift nodes if we don't have an exact match and need to insert.
	exact := (len(n.inodes) > 0 && index < len(n.inodes) && bytes.Equal(n.inodes[index].key, oldKey))
	if !exact {
		n.inodes = append(n.inodes, inode{})
		copy(n.inodes[index+1:], n.inodes[index:]) // 腾出n.inodes[index]这个空
	}

	inode := &n.inodes[index] // 插入，inode本身在堆上
	inode.flags = flags
	inode.key = newKey  // inode.key指向key这个堆上内存地址
	inode.value = value // inode.value指向value这个堆上内存地址
	inode.pgid = pgId
	_assert(len(inode.key) > 0, "put: zero-length inode key")
}

// del removes a key from the node.
func (n *node) del(key []byte) {
	// Find index of key.
	index := sort.Search(len(n.inodes), func(i int) bool { return bytes.Compare(n.inodes[i].key, key) != -1 })

	// Exit if the key isn't found.
	if index >= len(n.inodes) || !bytes.Equal(n.inodes[index].key, key) {
		return
	}

	// Delete inode from the node.
	n.inodes = append(n.inodes[:index], n.inodes[index+1:]...)

	// Mark the node as needing rebalancing.
	n.unbalanced = true
}

// node 和 page 的相互转换通过 node.read(p *page) 和 node.write(p *page)
// read initializes the node from a page. page --> node 前提是有一个已经建立好的page
func (n *node) read(p *page) { // 更像是用一个page来初始化一个node
	n.pgid = p.id
	n.isLeaf = ((p.flags & leafPageFlag) != 0) // 各种赋值
	n.inodes = make(inodes, int(p.count))      // 首先inode数组是在堆上

	for i := 0; i < int(p.count); i++ { // 填充inodes，这里可以清晰看到node和page的结构对应关系
		inode := &n.inodes[i]
		if n.isLeaf {
			elem := p.leafPageElement(uint16(i))
			inode.flags = elem.flags
			inode.key = elem.key()
			inode.value = elem.value() // 由于page是mmap内存区传过来的指针，所以这里可以看到inode的字段指向了mmap
		} else {
			elem := p.branchPageElement(uint16(i))
			inode.pgid = elem.pgid
			inode.key = elem.key()
		}
		_assert(len(inode.key) > 0, "read: zero-length inode key")
	}

	// Save first key so we can find the node in the parent when we spill.
	if len(n.inodes) > 0 {
		n.key = n.inodes[0].key
		_assert(len(n.key) > 0, "read: zero-length node key")
	} else {
		n.key = nil
	}
}

// node 和 page 的相互转换通过 node.read(p *page) 和 node.write(p *page)
// write writes the items onto one or more pages.
// The page should have p.id (might be 0 for meta or bucket-inline page) and p.overflow set
// and the rest should be zeroed.    node ---> page 前提是有一个已经建立的node
func (n *node) write(p *page) { // 更像是用一个node去实例化一个page
	_assert(p.count == 0 && p.flags == 0, "node cannot be written into a not empty page") // 总结来说就是不断拷贝node的inode的key和value过来到p处

	// Initialize page.
	if n.isLeaf {
		p.flags = leafPageFlag
	} else {
		p.flags = branchPageFlag
	}

	if len(n.inodes) >= 0xFFFF {
		panic(fmt.Sprintf("inode overflow: %d (pgid=%d)", len(n.inodes), p.id))
	}
	p.count = uint16(len(n.inodes))

	// Stop here if there are no items to write.
	if p.count == 0 {
		return
	}

	// Loop over each item and write it to the page.
	// off tracks the offset into p of the start of the next data.
	off := unsafe.Sizeof(*p) + n.pageElementSize()*uintptr(len(n.inodes)) // |page| + |(pageElement) * N|
	for i, item := range n.inodes {                                       // 遍历node的所有inode
		_assert(len(item.key) > 0, "write: zero-length inode key")

		// Create a slice to write into of needed size and advance
		// byte pointer for next iteration.
		sz := len(item.key) + len(item.value)
		b := unsafeByteSlice(unsafe.Pointer(p), off, 0, sz) // 从p + off处转成一个byte slice，然后切[0:sz]
		off += uintptr(sz)                                  // off增加sz

		// Write the page element.
		if n.isLeaf { // node是叶子
			elem := p.leafPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.flags = item.flags
			elem.ksize = uint32(len(item.key))
			elem.vsize = uint32(len(item.value))
		} else { // node是分支
			elem := p.branchPageElement(uint16(i))
			elem.pos = uint32(uintptr(unsafe.Pointer(&b[0])) - uintptr(unsafe.Pointer(elem)))
			elem.ksize = uint32(len(item.key))
			elem.pgid = item.pgid
			_assert(elem.pgid != p.id, "write: circular dependency occurred")
		}

		// Write data for the element to the end of the page.
		l := copy(b, item.key)  // 拷贝key
		copy(b[l:], item.value) // 拷贝value
	}

	// DEBUG ONLY: n.dump()
}

// split breaks up a node into multiple smaller nodes, if appropriate.
// This should only be called from the spill() function.
func (n *node) split(pageSize uintptr) []*node {
	var nodes []*node

	node := n
	for {
		// Split node into two.
		a, b := node.splitTwo(pageSize) // 拆分成两个node，但是并不是对半拆分
		nodes = append(nodes, a)

		// If we can't split then exit the loop.
		if b == nil {
			break
		}

		// Set node to b so it gets split on the next iteration.
		node = b
	}

	return nodes
}

// splitTwo breaks up a node into two smaller nodes, if appropriate.
// This should only be called from the split() function.
func (n *node) splitTwo(pageSize uintptr) (*node, *node) {
	// Ignore the split if the page doesn't have at least enough nodes for
	// two pages or if the nodes can fit in a single page.
	if len(n.inodes) <= (minKeysPerPage*2) || n.sizeLessThan(pageSize) {
		return n, nil
	}

	// Determine the threshold before starting a new node.
	var fillPercent = n.bucket.FillPercent
	if fillPercent < minFillPercent {
		fillPercent = minFillPercent
	} else if fillPercent > maxFillPercent {
		fillPercent = maxFillPercent
	}
	threshold := int(float64(pageSize) * fillPercent)

	// Determine split position and sizes of the two pages.
	splitIndex, _ := n.splitIndex(threshold) // 用于找到一个页面应该填充给定阈值的位置。它返回分裂的索引位置以及第一页的大小

	// Split node into two separate nodes.
	// If there's no parent then we'll need to create one.
	if n.parent == nil {
		n.parent = &node{bucket: n.bucket, children: []*node{n}}
	}

	// Create a new node and add it to the parent.
	next := &node{bucket: n.bucket, isLeaf: n.isLeaf, parent: n.parent}
	n.parent.children = append(n.parent.children, next)

	// Split inodes across two nodes.
	next.inodes = n.inodes[splitIndex:] // 分成两个node
	n.inodes = n.inodes[:splitIndex]

	// Update the statistics.
	n.bucket.tx.stats.IncSplit(1)

	return n, next
}

// splitIndex finds the position where a page will fill a given threshold.
// It returns the index as well as the size of the first page.
// This is only be called from split().
func (n *node) splitIndex(threshold int) (index, sz uintptr) {
	sz = pageHeaderSize

	// Loop until we only have the minimum number of keys required for the second page.
	for i := 0; i < len(n.inodes)-minKeysPerPage; i++ {
		index = uintptr(i)
		inode := n.inodes[i]
		elsize := n.pageElementSize() + uintptr(len(inode.key)) + uintptr(len(inode.value))

		// If we have at least the minimum number of keys and adding another
		// node would put us over the threshold then exit and return.
		if index >= minKeysPerPage && sz+elsize > uintptr(threshold) { // 一定是minKeysPerPage之后的元素
			break
		} // 如果当前索引大于等于每页最小键数，并且如果添加当前元素的大小会使总大小超过阈值，那么循环就会结束。这意味着找到了分裂的位置。

		// Add the element size to the total size.
		sz += elsize
	}

	return
}

// spill writes the nodes to dirty pages and splits nodes as it goes.
// Returns an error if dirty pages cannot be allocated.
func (n *node) spill() error { // spill感觉像是把一个node拆成很多小的node，然后给他们分配page，然后插入到parent
	var tx = n.bucket.tx
	if n.spilled {
		return nil
	}

	// Spill child nodes first. Child nodes can materialize sibling nodes in
	// the case of split-merge so we cannot use a range loop. We have to check
	// the children size on every loop iteration.
	sort.Sort(n.children)
	for i := 0; i < len(n.children); i++ {
		if err := n.children[i].spill(); err != nil {
			return err
		}
	}

	// We no longer need the child list because it's only used for spill tracking.
	n.children = nil

	// Split nodes into appropriate sizes. The first node will always be n.
	var nodes = n.split(uintptr(tx.db.pageSize)) // 拆成一个个node组成的list
	for _, node := range nodes {
		// Add node's page to the freelist if it's not new.
		if node.pgid > 0 {
			tx.db.freelist.free(tx.meta.txid, tx.page(node.pgid))
			node.pgid = 0
		}

		// Allocate contiguous space for the node.
		p, err := tx.allocate((node.size() + tx.db.pageSize - 1) / tx.db.pageSize) // 根据node，分配一个页
		if err != nil {
			return err
		}

		// Write the node.
		if p.id >= tx.meta.pgid {
			panic(fmt.Sprintf("pgid (%d) above high water mark (%d)", p.id, tx.meta.pgid))
		}
		node.pgid = p.id
		node.write(p) // 用一个node去实例化一个page
		node.spilled = true

		// Insert into parent inodes.
		if node.parent != nil {
			var key = node.key
			if key == nil {
				key = node.inodes[0].key
			}

			node.parent.put(key, node.inodes[0].key, nil, node.pgid, 0)
			node.key = node.inodes[0].key
			_assert(len(node.key) > 0, "spill: zero-length node key")
		}

		// Update the statistics.
		tx.stats.IncSpill(1)
	}

	// If the root node split and created a new root then we need to spill that
	// as well. We'll clear out the children to make sure it doesn't try to respill.
	if n.parent != nil && n.parent.pgid == 0 {
		n.children = nil
		return n.parent.spill()
	}

	return nil
}

// rebalance attempts to combine the node with sibling nodes if the node fill
// size is below a threshold or if there are not enough keys.
func (n *node) rebalance() {
	if !n.unbalanced {
		return
	}
	n.unbalanced = false

	// Update statistics.
	n.bucket.tx.stats.IncRebalance(1)

	// Ignore if node is above threshold (25%) and has enough keys.
	var threshold = n.bucket.tx.db.pageSize / 4
	if n.size() > threshold && len(n.inodes) > n.minKeys() {
		return
	}

	// Root node has special handling.
	if n.parent == nil {
		// If root node is a branch and only has one node then collapse it.
		if !n.isLeaf && len(n.inodes) == 1 {
			// Move root's child up.
			child := n.bucket.node(n.inodes[0].pgid, n)
			n.isLeaf = child.isLeaf
			n.inodes = child.inodes[:]
			n.children = child.children

			// Reparent all child nodes being moved.
			for _, inode := range n.inodes {
				if child, ok := n.bucket.nodes[inode.pgid]; ok {
					child.parent = n
				}
			}

			// Remove old child.
			child.parent = nil
			delete(n.bucket.nodes, child.pgid)
			child.free()
		}

		return
	}

	// If node has no keys then just remove it.
	if n.numChildren() == 0 {
		n.parent.del(n.key)
		n.parent.removeChild(n)
		delete(n.bucket.nodes, n.pgid)
		n.free()
		n.parent.rebalance()
		return
	}

	_assert(n.parent.numChildren() > 1, "parent must have at least 2 children")

	// Destination node is right sibling if idx == 0, otherwise left sibling.
	var target *node
	var useNextSibling = (n.parent.childIndex(n) == 0)
	if useNextSibling {
		target = n.nextSibling()
	} else {
		target = n.prevSibling()
	}

	// If both this node and the target node are too small then merge them.
	if useNextSibling {
		// Reparent all child nodes being moved.
		for _, inode := range target.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				child.parent.removeChild(child)
				child.parent = n
				child.parent.children = append(child.parent.children, child)
			}
		}

		// Copy over inodes from target and remove target.
		n.inodes = append(n.inodes, target.inodes...)
		n.parent.del(target.key)
		n.parent.removeChild(target)
		delete(n.bucket.nodes, target.pgid)
		target.free()
	} else {
		// Reparent all child nodes being moved.
		for _, inode := range n.inodes {
			if child, ok := n.bucket.nodes[inode.pgid]; ok {
				child.parent.removeChild(child)
				child.parent = target
				child.parent.children = append(child.parent.children, child)
			}
		}

		// Copy over inodes to target and remove node.
		target.inodes = append(target.inodes, n.inodes...)
		n.parent.del(n.key)
		n.parent.removeChild(n)
		delete(n.bucket.nodes, n.pgid)
		n.free()
	}

	// Either this node or the target node was deleted from the parent so rebalance it.
	n.parent.rebalance()
}

// removes a node from the list of in-memory children.
// This does not affect the inodes.
func (n *node) removeChild(target *node) {
	for i, child := range n.children {
		if child == target {
			n.children = append(n.children[:i], n.children[i+1:]...)
			return
		}
	}
}

// dereference causes the node to copy all its inode key/value references to heap memory.
// This is required when the mmap is reallocated so inodes are not pointing to stale data. 这个方法的主要作用是将 node 中的键值数据从内存映射区（mmap）拷贝到堆内存中，确保在 mmap 重新分配时，这些节点不会指向无效的数据
func (n *node) dereference() {
	if n.key != nil {
		key := make([]byte, len(n.key)) // 在堆上分配内存，因为inode原本是指向mmap内存区的
		copy(key, n.key)                // 拷贝 key 数据到新分配的内存
		n.key = key                     // 更新节点的 key 引用到新内存
		_assert(n.pgid == 0 || len(n.key) > 0, "dereference: zero-length node key on existing node")
	}

	for i := range n.inodes {
		inode := &n.inodes[i]

		key := make([]byte, len(inode.key)) // 把inode.key拷贝到另一端内存key中（和上面一样）
		copy(key, inode.key)
		inode.key = key // 再inode.key指向key
		_assert(len(inode.key) > 0, "dereference: zero-length inode key")

		value := make([]byte, len(inode.value)) // value同理
		copy(value, inode.value)
		inode.value = value
	}

	// Recursively dereference children.
	for _, child := range n.children {
		child.dereference()
	}

	// Update statistics.
	n.bucket.tx.stats.IncNodeDeref(1)
}

// free adds the node's underlying page to the freelist.
func (n *node) free() {
	if n.pgid != 0 {
		n.bucket.tx.db.freelist.free(n.bucket.tx.meta.txid, n.bucket.tx.page(n.pgid))
		n.pgid = 0
	}
}

// dump writes the contents of the node to STDERR for debugging purposes.
/*
func (n *node) dump() {
	// Write node header.
	var typ = "branch"
	if n.isLeaf {
		typ = "leaf"
	}
	warnf("[NODE %d {type=%s count=%d}]", n.pgid, typ, len(n.inodes))

	// Write out abbreviated version of each item.
	for _, item := range n.inodes {
		if n.isLeaf {
			if item.flags&bucketLeafFlag != 0 {
				bucket := (*bucket)(unsafe.Pointer(&item.value[0]))
				warnf("+L %08x -> (bucket root=%d)", trunc(item.key, 4), bucket.root)
			} else {
				warnf("+L %08x -> %08x", trunc(item.key, 4), trunc(item.value, 4))
			}
		} else {
			warnf("+B %08x -> pgid=%d", trunc(item.key, 4), item.pgid)
		}
	}
	warn("")
}
*/

func compareKeys(left, right []byte) int {
	return bytes.Compare(left, right)
}

type nodes []*node

func (s nodes) Len() int      { return len(s) }
func (s nodes) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s nodes) Less(i, j int) bool {
	return bytes.Compare(s[i].inodes[0].key, s[j].inodes[0].key) == -1
}

// inode represents an internal node inside of a node.
// It can be used to point to elements in a page or point
// to an element which hasn't been added to a page yet.
type inode struct {
	flags uint32 // 用于 leaf node，区分是正常 value 还是 subbucket
	pgid  pgid   // 用于 branch node, 子节点的 page id
	key   []byte
	value []byte
}

type inodes []inode
