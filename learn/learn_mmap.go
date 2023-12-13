package main

import (
	"golang.org/x/sys/unix"
	"log"
	"os"
	"syscall"
)

func main() {
	// 文件路径
	filePath := "example.db"

	// 打开文件，如果不存在则创建，以读写模式打开
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		log.Fatalf("failed to open file: %v", err)
	}
	defer file.Close()

	// 设置文件大小
	fileSize := 1024 // 1 KB
	err = file.Truncate(int64(fileSize))
	if err != nil {
		log.Fatalf("failed to set file size: %v", err)
	}

	// 使用mmap映射文件
	data, err := unix.Mmap(int(file.Fd()), 0, fileSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	if err != nil {
		log.Fatalf("failed to mmap: %v", err)
	}
	defer unix.Munmap(data)

	// 写入数据到映射区域
	copy(data, []byte("Hello mmap"))

	// 使用os.File.WriteAt更新文件特定部分（例如元数据）
	metaData := []byte("Metadata")
	_, err = file.WriteAt(metaData, 512) // 假设元数据放在文件中间位置
	if err != nil {
		log.Fatalf("failed to write metadata: %v", err)
	}

	// 使用unix.Fdatasync同步文件到磁盘
	err = syscall.Fdatasync(int(file.Fd()))
	if err != nil {
		log.Fatalf("failed to fdatasync: %v", err)
	}

	// 读取映射区域的数据
	readData := string(data[:10]) // 读取前10个字节
	log.Printf("Data read from memory-mapped file: %s\n", readData)

	// 读取文件中的元数据
	metaDataBuffer := make([]byte, len(metaData))
	_, err = file.ReadAt(metaDataBuffer, 512)
	if err != nil {
		log.Fatalf("failed to read metadata: %v", err)
	}

	log.Printf("Metadata read from file: %s\n", string(metaDataBuffer))

	log.Println("Data written, synced, and read successfully.")
}

//root@kube-ksc-sg-sg2-uat-10-129-110-5:~/go/src/test_mmap# go run main.go
//2023/12/13 14:47:37 Data read from memory-mapped file: Hello mmap
//2023/12/13 14:47:37 Metadata read from file: Metadata
//2023/12/13 14:47:37 Data written, synced, and read successfully.
//root@kube-ksc-sg-sg2-uat-10-129-110-5:~/go/src/test_mmap# ls
//example.db  go.mod  go.sum  main.go
//root@kube-ksc-sg-sg2-uat-10-129-110-5:~/go/src/test_mmap# cat example.db
//Hello mmapMetadata
