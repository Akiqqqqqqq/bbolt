package main

import (
	"fmt"
	"os"
	"reflect"
	"syscall"
	"unsafe"
)

func main() {
	// 打开文件
	file, err := os.OpenFile("example.dat", os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// 获取文件的大小
	fileInfo, err := file.Stat()
	if err != nil {
		panic(err)
	}
	size := fileInfo.Size()

	// 执行mmap
	mmap, err := syscall.Mmap(int(file.Fd()), 0, int(size), syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	defer syscall.Munmap(mmap)

	// 创建一个slice来读取mmap的内容
	sliceHeader := (*reflect.SliceHeader)(unsafe.Pointer(&mmap))
	slice := *(*[]byte)(unsafe.Pointer(sliceHeader))

	// 使用slice来访问文件内容
	fmt.Println(slice)

	// 修改文件内容（通过内存）
	copy(slice, []byte("Hello, World!"))
}
