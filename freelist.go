package bbolt

import (
	"fmt"
	"sort"
	"unsafe"
)

// txPending holds a list of pgids and corresponding allocation txns
// that are pending to be freed.
type txPending struct {
	ids              []pgid
	alloctx          []txid // txids allocating the ids
	lastReleaseBegin txid   // beginning txid of last matching releaseRange
}

// pidSet holds the set of starting pgids which have the same span size
type pidSet map[pgid]struct{}

// freelist represents a list of all pages that are available for allocation. 空闲页列表
// It also tracks pages that have been freed but are still in use by open transactions.
type freelist struct {
	freelistType   FreelistType                // freelist type
	ids            []pgid                      // all free and available free page ids.  所有空闲页 page id
	allocs         map[pgid]txid               // mapping of txid that allocated a pgid.
	pending        map[txid]*txPending         // mapping of soon-to-be free page ids by tx. 即将释放的页的tx
	cache          map[pgid]struct{}           // fast lookup of all free and pending page ids.
	freemaps       map[uint64]pidSet           // key is the size of continuous pages(span), value is a set which contains the starting pgids of same size ;key是空闲块的连续页数量，value是所有起始页id的集合
	forwardMap     map[pgid]uint64             // key is start pgid, value is its span size   key是空闲块的起始页id, value是空闲块的连续页数量
	backwardMap    map[pgid]uint64             // key is end pgid, value is its span size     key是空闲块的末尾页id, value是空闲块的连续页数量
	allocate       func(txid txid, n int) pgid // the freelist allocate func
	free_count     func() int                  // the function which gives you free page number
	mergeSpans     func(ids pgids)             // the mergeSpan func
	getFreePageIDs func() []pgid               // get free pgids func
	readIDs        func(pgids []pgid)          // readIDs func reads list of pages and init the freelist
}

// newFreelist returns an empty, initialized freelist.
func newFreelist(freelistType FreelistType) *freelist {
	f := &freelist{
		freelistType: freelistType,
		allocs:       make(map[pgid]txid),
		pending:      make(map[txid]*txPending), // mapping of soon-to-be free page ids by tx.
		cache:        make(map[pgid]struct{}),
		freemaps:     make(map[uint64]pidSet), // key是空闲块的连续页数量，value是所有起始页id的集合
		forwardMap:   make(map[pgid]uint64),   // key是空闲块的起始页id, value是空闲块的连续页数量
		backwardMap:  make(map[pgid]uint64),   // key是空闲块的末尾页id, value是空闲块的连续页数量
	}
	// default type is array
	if freelistType == FreelistMapType { // 这里所有函数围绕f.ids展开
		f.allocate = f.hashmapAllocate
		f.free_count = f.hashmapFreeCount
		f.mergeSpans = f.hashmapMergeSpans
		f.getFreePageIDs = f.hashmapGetFreePageIDs
		f.readIDs = f.hashmapReadIDs
	} else {
		f.allocate = f.arrayAllocate
		f.free_count = f.arrayFreeCount
		f.mergeSpans = f.arrayMergeSpans
		f.getFreePageIDs = f.arrayGetFreePageIDs
		f.readIDs = f.arrayReadIDs
	}

	return f
}

// size returns the size of the page after serialization.  返回freelist page的大小？
func (f *freelist) size() int {
	n := f.count()  // f.free_count() + f.pending_count()
	if n >= 0xFFFF {
		// The first element will be used to store the count. See freelist.write. 当超过0xFFF后，由于freelist page的count字段是2byte，不能表示count了；于是就用数组第一个pgid（4byte）会用来存真正count
		n++
	}
	return int(pageHeaderSize) + (int(unsafe.Sizeof(pgid(0))) * n)
}

// count returns count of pages on the freelist
func (f *freelist) count() int {
	return f.free_count() + f.pending_count()  // len(f.ids) + sum(len(pending.txPending.ids))
}

// arrayFreeCount returns count of free pages(array version)
func (f *freelist) arrayFreeCount() int {
	return len(f.ids)
}

// pending_count returns count of pending pages
func (f *freelist) pending_count() int {
	var count int
	for _, txp := range f.pending {
		count += len(txp.ids)
	}
	return count
}

// copyall copies a list of all free ids and all pending ids in one sorted list.
// f.count returns the minimum length required for dst.
func (f *freelist) copyall(dst []pgid) {
	m := make(pgids, 0, f.pending_count())
	for _, txp := range f.pending {
		m = append(m, txp.ids...)
	}
	sort.Sort(m)
	mergepgids(dst, f.getFreePageIDs(), m)
}

// arrayAllocate returns the starting page id of a contiguous list of pages of a given size.  返回一段连续页的起始页pgid
// If a contiguous block cannot be found then 0 is returned. 这个方法搜索freelist中的连续页面ID块，并尝试为给定的大小分配它们。如果成功分配，则这些页面ID从freelist中移除，反之返回0。
func (f *freelist) arrayAllocate(txid txid, n int) pgid {  // 遍历整个数组，寻找连续的N个空闲[物理页]
	if len(f.ids) == 0 {
		return 0
	}

	var initial, previd pgid
	for i, id := range f.ids {
		if id <= 1 {
			panic(fmt.Sprintf("invalid page allocation: %d", id))
		}

		// Reset initial page if this is not contiguous.
		if previd == 0 || id-previd != 1 { // 注意 f.ids里面的id的数值不一定连续，可能是 1023, 1024, 1030, 1031
			initial = id
		}

		// If we found a contiguous block then remove it and return it.
		if (id-initial)+1 == pgid(n) { // 找到一个连续大小为n的pages
			// If we're allocating off the beginning then take the fast path
			// and just adjust the existing slice. This will use extra memory
			// temporarily but the append() in free() will realloc the slice
			// as is necessary.
			if (i + 1) == n {
				f.ids = f.ids[i+1:]
			} else {
				copy(f.ids[i-n+1:], f.ids[i+1:])
				f.ids = f.ids[:len(f.ids)-n] // 从f.ids中删除这些连续的页面ID
			}

			// Remove from the free cache.
			for i := pgid(0); i < pgid(n); i++ {
				delete(f.cache, initial+i) // 从f.cache中删除这些页面ID
			}
			f.allocs[initial] = txid // 在f.allocs中标记这些页面ID已被给定的事务ID分配。
			return initial
		}

		previd = id
	}
	return 0
}

// free releases a page and its overflow for a given transaction id.  释放一个指定的txid的page
// If the page is already free then a panic will occur.
func (f *freelist) free(txid txid, p *page) {  // 没看懂
	if p.id <= 1 {
		panic(fmt.Sprintf("cannot free page 0 or 1: %d", p.id))
	}

	// Free page and all its overflow pages.
	txp := f.pending[txid]   // <txid, txPending>
	if txp == nil {
		txp = &txPending{}
		f.pending[txid] = txp
	}
	allocTxid, ok := f.allocs[p.id]   // "已分配map"中是否有pgid
	if ok {
		delete(f.allocs, p.id)   
	} else if (p.flags & freelistPageFlag) != 0 {
		// Freelist is always allocated by prior tx.
		allocTxid = txid - 1
	}
	// overflow = len(page body)/4kb; 如果overflow = 2， 则释放[p.id, p.id+1, p.id+2]，则len(page body) = 8kb = 8*1024 byte，说明这个page body很长，这个逻辑页是3个物理页构成的; 但是p.id明明是逻辑页id啊
	for id := p.id; id <= p.id+pgid(p.overflow); id++ {   // 循环 [p.id, p.id + p.overflow] 
		// Verify that page is not already free.
		if _, ok := f.cache[id]; ok {
			panic(fmt.Sprintf("page %d already freed", id))
		}
		// Add to the freelist and cache.
		txp.ids = append(txp.ids, id)  // pgid加入到pending中
		txp.alloctx = append(txp.alloctx, allocTxid)
		f.cache[id] = struct{}{}
	}
}

// release moves all page ids for a transaction id (or older) to the freelist.
func (f *freelist) release(txid txid) {
	m := make(pgids, 0)
	for tid, txp := range f.pending {
		if tid <= txid {
			// Move transaction's pending pages to the available freelist.
			// Don't remove from the cache since the page is still free.
			m = append(m, txp.ids...)
			delete(f.pending, tid)  // 从pending里面删除比txid小的
		}
	}
	f.mergeSpans(m)  // 被删的传入，合并，加入回free ids
}

// releaseRange moves pending pages allocated within an extent [begin,end] to the free list.
func (f *freelist) releaseRange(begin, end txid) {
	if begin > end {
		return
	}
	var m pgids
	for tid, txp := range f.pending {
		if tid < begin || tid > end {
			continue
		}
		// Don't recompute freed pages if ranges haven't updated.
		if txp.lastReleaseBegin == begin {
			continue
		}
		for i := 0; i < len(txp.ids); i++ {
			if atx := txp.alloctx[i]; atx < begin || atx > end {
				continue
			}
			m = append(m, txp.ids[i])
			txp.ids[i] = txp.ids[len(txp.ids)-1]
			txp.ids = txp.ids[:len(txp.ids)-1]
			txp.alloctx[i] = txp.alloctx[len(txp.alloctx)-1]
			txp.alloctx = txp.alloctx[:len(txp.alloctx)-1]
			i--
		}
		txp.lastReleaseBegin = begin
		if len(txp.ids) == 0 {
			delete(f.pending, tid)
		}
	}
	f.mergeSpans(m)
}

// rollback removes the pages from a given pending tx.
func (f *freelist) rollback(txid txid) {
	// Remove page ids from cache.
	txp := f.pending[txid]
	if txp == nil {
		return
	}
	var m pgids
	for i, pgid := range txp.ids {
		delete(f.cache, pgid)
		tx := txp.alloctx[i]
		if tx == 0 {
			continue
		}
		if tx != txid {
			// Pending free aborted; restore page back to alloc list.
			f.allocs[pgid] = tx
		} else {
			// Freed page was allocated by this txn; OK to throw away.
			m = append(m, pgid)
		}
	}
	// Remove pages from pending list and mark as free if allocated by txid.
	delete(f.pending, txid)
	f.mergeSpans(m)
}

// freed returns whether a given page is in the free list.
func (f *freelist) freed(pgId pgid) bool {
	_, ok := f.cache[pgId]
	return ok
}

// read initializes the freelist from a freelist page. read方法从freelist page来初始化一个freelist
func (f *freelist) read(p *page) {  // 主要用于从一个标记为 freelistPageFlag 的页面初始化 freelist 结构，这里传入的p就是pgid为2的freelist的page
	if (p.flags & freelistPageFlag) == 0 {
		panic(fmt.Sprintf("invalid freelist page: %d, page type is %s", p.id, p.typ()))
	}
	// If the page.count is at the max uint16 value (64k) then it's considered
	// an overflow and the size of the freelist is stored as the first element.
	var idx, count = 0, int(p.count) // free page的数量
	if count == 0xFFFF {             // 最大值
		idx = 1
		c := *(*pgid)(unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p)))  // |pageheader| pgid(0) | pgid(1) | pgid(2) | ... 这里拿到pgid(0)的值，代表free page的数量
		count = int(c)
		if count < 0 {
			panic(fmt.Sprintf("leading element count %d overflows int", c))
		}
	}

	// Copy the list of page ids from the freelist.
	if count == 0 {
		f.ids = nil // 第一次进来
	} else {
		var ids []pgid  // ids数组
		data := unsafeIndex(unsafe.Pointer(p), unsafe.Sizeof(*p), unsafe.Sizeof(ids[0]), idx) // p + sizeof(p) + sizeof(pgid)*0 得到data指针位置, 也就是pgid(0)的位置
		unsafeSlice(unsafe.Pointer(&ids), data, count)                                        // 使ids这个slice指向新的内存地址data，并具有新的长度和容量count

		// copy the ids, so we don't modify on the freelist page directly
		idsCopy := make([]pgid, count)   // 在堆上分配
		copy(idsCopy, ids)               // 从mmap拷贝到堆上
		// Make sure they're sorted.
		sort.Sort(pgids(idsCopy))

		f.readIDs(idsCopy)
	}
}

// arrayReadIDs initializes the freelist from a given list of ids.
func (f *freelist) arrayReadIDs(ids []pgid) {
	f.ids = ids  // 赋值ids数组引用到freelist
	f.reindex()
}

func (f *freelist) arrayGetFreePageIDs() []pgid {
	return f.ids
}

// write writes the page ids onto a freelist page. All free and pending ids are
// saved to disk since in the event of a program crash, all pending ids will
// become free.
func (f *freelist) write(p *page) error {
	// Combine the old free pgids and pgids waiting on an open transaction.

	// Update the header flag.
	p.flags |= freelistPageFlag

	// The page.count can only hold up to 64k elements so if we overflow that
	// number then we handle it by putting the size in the first element.
	l := f.count()
	if l == 0 {
		p.count = uint16(l)
	} else if l < 0xFFFF {
		p.count = uint16(l)
		var ids []pgid
		data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		unsafeSlice(unsafe.Pointer(&ids), data, l)
		f.copyall(ids)
	} else {
		p.count = 0xFFFF
		var ids []pgid
		data := unsafeAdd(unsafe.Pointer(p), unsafe.Sizeof(*p))
		unsafeSlice(unsafe.Pointer(&ids), data, l+1)
		ids[0] = pgid(l)
		f.copyall(ids[1:])
	}

	return nil
}

// reload reads the freelist from a page and filters out pending items.
func (f *freelist) reload(p *page) {
	f.read(p)

	// Build a cache of only pending pages.
	pcache := make(map[pgid]bool)
	for _, txp := range f.pending {
		for _, pendingID := range txp.ids {
			pcache[pendingID] = true
		}
	}

	// Check each page in the freelist and build a new available freelist
	// with any pages not in the pending lists.
	var a []pgid
	for _, id := range f.getFreePageIDs() {
		if !pcache[id] {
			a = append(a, id)
		}
	}

	f.readIDs(a)
}

// noSyncReload reads the freelist from pgids and filters out pending items.
func (f *freelist) noSyncReload(pgids []pgid) {
	// Build a cache of only pending pages.
	pcache := make(map[pgid]bool)
	for _, txp := range f.pending {
		for _, pendingID := range txp.ids {
			pcache[pendingID] = true
		}
	}

	// Check each page in the freelist and build a new available freelist
	// with any pages not in the pending lists.
	var a []pgid
	for _, id := range pgids {
		if !pcache[id] {
			a = append(a, id)
		}
	}

	f.readIDs(a)
}

// reindex rebuilds the free cache based on available and pending free lists.
func (f *freelist) reindex() {
	ids := f.getFreePageIDs()   // 拿到f.ids
	f.cache = make(map[pgid]struct{}, len(ids))   // 制作一个hashset
	for _, id := range ids {
		f.cache[id] = struct{}{}    // 存空闲pgid到hashset
	}
	for _, txp := range f.pending {
		for _, pendingID := range txp.ids {
			f.cache[pendingID] = struct{}{}   // 存pending的pgid到hashset
		}
	}
}

// arrayMergeSpans try to merge list of pages(represented by pgids) with existing spans but using array
func (f *freelist) arrayMergeSpans(ids pgids) {
	sort.Sort(ids)
	f.ids = pgids(f.ids).merge(ids)
}
