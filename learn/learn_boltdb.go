package main

import (
	"fmt"

	bolt "go.etcd.io/bbolt"
)

func main() {
	bopts := &bolt.Options{}
	db, err := bolt.Open("learn/my.db", 0600, bopts)
	if err != nil {
		panic(err)
	}
	tx, err := db.Begin(true)
	if err != nil {
		panic(err)
	}
	_, err = tx.CreateBucket([]byte("test_bucket"))
	if err != nil {
		panic(err)
	}
	err = tx.Commit()
	if err != nil {
		panic(err)
	}

	fmt.Print("-------------- put a kv ---------------")
	tx, err = db.Begin(true)
	if err != nil {
		panic(err)
	}
	b := tx.Bucket([]byte("test_bucket"))
	err = b.Put([]byte("test_key"), []byte("test_value"))
	if err != nil {
		return
	}

	// TODO: 看看只读事务、
	// 以及db.Update(func)的逻辑: 就是帮你写了begin和commit
}
