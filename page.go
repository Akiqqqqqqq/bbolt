package bbolt

import (
	"fmt"
	"os"
	"sort"
	"unsafe"
)

const pageHeaderSize = unsafe.Sizeof(page{})

const minKeysPerPage = 2

const branchPageElementSize = unsafe.Sizeof(branchPageElement{})
const leafPageElementSize = unsafe.Sizeof(leafPageElement{})

const (
	branchPageFlag   = 0x01 // 存放 branch node 的数据
	leafPageFlag     = 0x02 // 存放 leaf node 的数据
	metaPageFlag     = 0x04 // 存放 db 的 meta data   0100
	freelistPageFlag = 0x10 // 存放 db 的空闲 page    1 0000
)

const (
	bucketLeafFlag = 0x01
)

type pgid uint64

type page struct {
	id       pgid   // page id (8 bytes) uint64类型
	flags    uint16 // 区分不同类型的 page (2 bytes)
	count    uint16 // 不同类型page, count含义不同: 分支节点页count代表子节点数量；叶子节点页count代表本节点存储的kv对数量；空闲列表页count代表空闲页的数量。
	overflow uint32 // 若单个 page 大小不够，会分配多个 page; overflow字段代表Page Body往后延伸占用了多少个物理页，也就是该逻辑page除本物理页还占用了几个连续物理页。 (4 bytes)
} // 上述4个字段一共16 bytes， pageSize为4096 bytes，所以其实有很多的空闲区域

// typ returns a human readable page type string used for debugging.
func (p *page) typ() string {
	if (p.flags & branchPageFlag) != 0 {
		return "branch"
	} else if (p.flags & leafPageFlag) != 0 {
		return "leaf"
	} else if (p.flags & metaPageFlag) != 0 {
		return "meta"
	} else if (p.flags & freelistPageFlag) != 0 {
		return "freelist"
	}
	return fmt.Sprintf("unknown<%02x>", p.flags)
}

// meta returns a pointer to the metadata section of the page.
func (p *page) meta() *meta {
	return (*meta)(unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))) // meta部分紧接在page结构体后面? 对，page结构体不大,小于pageSize
}

func (p *page) fastCheck(id pgid) {
	_assert(p.id == id, "Page expected to be: %v, but self identifies as %v", id, p.id)
	// Only one flag of page-type can be set.
	_assert(p.flags == branchPageFlag ||
		p.flags == leafPageFlag ||
		p.flags == metaPageFlag ||
		p.flags == freelistPageFlag,
		"page %v: has unexpected type/flags: %x", p.id, p.flags)
}

// leafPageElement retrieves the leaf node by index; 这个函数的目的是根据提供的索引，计算出对应的leafPageElement在page中的位置，并返回一个指向该元素的指针。
func (p *page) leafPageElement(index uint16) *leafPageElement {
	return (*leafPageElement)(unsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p),
		leafPageElementSize, int(index)))
}

// leafPageElements retrieves a list of leaf nodes.
func (p *page) leafPageElements() []leafPageElement {
	if p.count == 0 {
		return nil
	}
	var elems []leafPageElement                             // {flags uint32 ,pos   uint32 ,ksize uint32 ,vsize uint32}
	data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p)) // data在root这个page尾巴上 (data变量将指向p之后的内存位置)
	unsafeSlice(unsafe.Pointer(&elems), data, int(p.count)) // 直接修改elems这个slice，使它指向data，并且长度为int(p.count)
	return elems
}

// branchPageElement retrieves the branch node by index
func (p *page) branchPageElement(index uint16) *branchPageElement {
	return (*branchPageElement)(unsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p),
		unsafe.Sizeof(branchPageElement{}), int(index)))
}

// branchPageElements retrieves a list of branch nodes.
func (p *page) branchPageElements() []branchPageElement {
	if p.count == 0 {
		return nil
	}
	var elems []branchPageElement
	data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
	unsafeSlice(unsafe.Pointer(&elems), data, int(p.count))
	return elems
}

// dump writes n bytes of the page to STDERR as hex output.
func (p *page) hexdump(n int) {
	buf := unsafeByteSlice(unsafe.Pointer(p), 0, 0, n)
	fmt.Fprintf(os.Stderr, "%x\n", buf)
}

type pages []*page

func (s pages) Len() int           { return len(s) }
func (s pages) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pages) Less(i, j int) bool { return s[i].id < s[j].id }

// branchPageElement represents a node on a branch page.
type branchPageElement struct { // 树节点数组元素
	pos   uint32
	ksize uint32
	pgid  pgid // 所在页的page id
}

// key returns a byte slice of the node key.
func (n *branchPageElement) key() []byte {
	return unsafeByteSlice(unsafe.Pointer(n), 0, int(n.pos), int(n.pos)+int(n.ksize)) // 返回了一个从n开始，长度为int(n.pos)+int(n.ksize)的slice
}

// leafPageElement represents a node on a leaf page.
type leafPageElement struct { // 叶子数组元素
	flags uint32
	pos   uint32
	ksize uint32
	vsize uint32
}

// key returns a byte slice of the node key.
func (n *leafPageElement) key() []byte {
	i := int(n.pos)
	j := i + int(n.ksize)
	return unsafeByteSlice(unsafe.Pointer(n), 0, i, j)
}

// value returns a byte slice of the node value.
func (n *leafPageElement) value() []byte {
	i := int(n.pos) + int(n.ksize)
	j := i + int(n.vsize)
	return unsafeByteSlice(unsafe.Pointer(n), 0, i, j)
}

// PageInfo represents human readable information about a page.
type PageInfo struct {
	ID            int
	Type          string
	Count         int
	OverflowCount int
}

type pgids []pgid

func (s pgids) Len() int           { return len(s) }
func (s pgids) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s pgids) Less(i, j int) bool { return s[i] < s[j] }

// merge returns the sorted union of a and b.
func (a pgids) merge(b pgids) pgids {
	// Return the opposite slice if one is nil.
	if len(a) == 0 {
		return b
	}
	if len(b) == 0 {
		return a
	}
	merged := make(pgids, len(a)+len(b))
	mergepgids(merged, a, b)
	return merged
}

// mergepgids函数，它将两个已排序的pgids类型切片合并到目标切片dst中。如果dst切片没有足够的空间来容纳合并后的元素，程序将会触发panic。
// mergepgids copies the sorted union of a and b into dst.
// If dst is too small, it panics.
func mergepgids(dst, a, b pgids) {
	if len(dst) < len(a)+len(b) {
		panic(fmt.Errorf("mergepgids bad len %d < %d + %d", len(dst), len(a), len(b)))
	}
	// Copy in the opposite slice if one is nil.
	if len(a) == 0 {
		copy(dst, b)
		return
	}
	if len(b) == 0 {
		copy(dst, a)
		return
	}

	// Merged will hold all elements from both lists.
	merged := dst[:0] // 这里使用了一个技巧，创建一个长度为0但容量等于dst的切片merged，用于存储合并后的元素。这样做可以避免分配新的内存，直接在dst的底层数组上操作。

	// Assign lead to the slice with a lower starting value, follow to the higher value.
	lead, follow := a, b
	if b[0] < a[0] { // 这段代码确定了两个切片中哪个切片的第一个元素更小，更小的那个将作为lead，另一个作为follow
		lead, follow = b, a
	}

	// Continue while there are elements in the lead.
	for len(lead) > 0 {
		// Merge largest prefix of lead that is ahead of follow[0].
		n := sort.Search(len(lead), func(i int) bool { return lead[i] > follow[0] })
		merged = append(merged, lead[:n]...)
		if n >= len(lead) {
			break
		}

		// Swap lead and follow.
		lead, follow = follow, lead[n:]
	}

	// Append what's left in follow.
	_ = append(merged, follow...)
}
