package main

import (
	"GoSQL/src/buffer"
	"GoSQL/src/storage"
)

func main() {
	pool := buffer.NewBufferPoolManager(5)
	pageManager := storage.NewPageManager()
	pool.Insert(pageManager.NewPage())
	pool.Insert(pageManager.NewPage())
	pool.Insert(pageManager.NewPage())
	pool.Insert(pageManager.NewPage())
	pool.Insert(pageManager.NewPage())
	pool.Insert(pageManager.NewPage())
	pool.Insert(pageManager.NewPage())
	pool.Insert(pageManager.NewPage())

	//Test()
	//fmt.Println("123")
}
