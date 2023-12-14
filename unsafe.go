package bbolt

import (
	"reflect"
	"unsafe"
)

func unsafeAdd(base unsafe.Pointer, offset uintptr) unsafe.Pointer { // base 是一个unsafe.Pointer，代表基础指针；offset 是一个uintptr，代表要添加到基础指针上的字节偏移量。
	return unsafe.Pointer(uintptr(base) + offset) // 1. 将base转换为uintptr，这是一个无符号整数类型，其大小足以存储指针值；2.它将offset加到uintptr(base)上，得到新的地址
} // 3. 它将结果转换回unsafe.Pointer，因为在Go中指针运算必须在uintptr类型上进行

func unsafeIndex(base unsafe.Pointer, offset uintptr, elemsz uintptr, n int) unsafe.Pointer {
	return unsafe.Pointer(uintptr(base) + offset + uintptr(n)*elemsz)
} // 函数计算一个新的地址，该地址是基于base加上offset后，再加上n个元素的大小（n * elemsz）。这样就得到了第n个元素的地址。然后，它将计算出的地址转换回unsafe.Pointer。

func unsafeByteSlice(base unsafe.Pointer, offset uintptr, i, j int) []byte { // base是一个基础指针，offset是从base开始的偏移量，i和j分别是新切片的起始和结束索引
	// See: https://github.com/golang/go/wiki/cgo#turning-c-arrays-into-go-slices
	//
	// This memory is not allocated from C, but it is unmanaged by Go's  C分配的，但是是go的GC回收的
	// garbage collector and should behave similarly, and the compiler
	// should produce similar code.  Note that this conversion allows a
	// subslice to begin after the base address, with an optional offset,
	// while the URL above does not cover this case and only slices from
	// index 0.  However, the wiki never says that the address must be to
	// the beginning of a C allocation (or even that malloc was used at
	// all), so this is believed to be correct.
	return (*[maxAllocSize]byte)(unsafeAdd(base, offset))[i:j:j] // 先拿到base+offset处的指针，然后转成一个byte slice，然后切[i:j]  (同时j也是切片的容量,确保了切片的len和cap相同)
}

// unsafeSlice modifies the data, len, and cap of a slice variable pointed to by
// the slice parameter.  This helper should be used over other direct
// manipulation of reflect.SliceHeader to prevent misuse, namely, converting
// from reflect.SliceHeader to a Go slice type. 这个Go函数unsafeSlice使用unsafe包来修改一个切片的底层数据指针、长度（len）和容量（cap）。这个函数的目的是为了提供一种安全的方式去直接修改切片的内部结构，而不是通过直接操作reflect.SliceHeader来避免潜在的错误用法。
func unsafeSlice(slice, data unsafe.Pointer, len int) {
	s := (*reflect.SliceHeader)(slice) // 将slice参数（一个unsafe.Pointer）转换为一个指向reflect.SliceHeader的指针
	s.Data = uintptr(data)             // 设置reflect.SliceHeader的Data字段为data参数的值, 这样切片就会指向data指针指向的内存。
	s.Cap = len                        // 这两行代码将切片的容量和长度都设置为len参数的值。这意味着新的切片将拥有从data开始的、长度为len的连续内存块
	s.Len = len
} // 它的目的是直接修改一个已有切片的Data、Len和Cap字段，使得这个切片指向新的内存地址并具有新的长度和容量。
// reflect.SliceHeader是一个结构体，它精确地映射了Go切片的内部表示，包含了Data（数据指针）、Len（长度）、和Cap（容量）三个字段
