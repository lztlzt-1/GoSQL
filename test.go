package main

import (
	"GoSQL/src/algorithm/ExtendibleHash"
	"GoSQL/src/storage"
	"log"
)

func Test() {
	pageManager := storage.NewPageManager()
	diskManager, err := storage.NewDiskManager("test.db")
	bucket := ExtendibleHash.NewBucket(8, 0)
	bucket.Insert("123", "cnm")
	bucket.Insert(1.2, "ddd")
	bucket.Delete(1.2)
	if err != nil {
		log.Fatal(err.Error())
	}
	page := pageManager.NewPage()
	str := "hehe"
	err = page.Insert([]byte(str))
	if err != nil {
		log.Fatal(err.Error())
	}
	print(page)
	done, err1 := diskManager.WritePage(6, page)
	if err1 != nil {
		log.Fatal(err1.Error())
	}
	page2, err1 := diskManager.ReadPage(1)
	if err1 != nil {
		log.Fatal(err1.Error())
	}
	page2.GetPageId()
	print(done)
	//print(pageManager.GetNewPageId())
	//print(pageManager.GetNewPageId())
	//print(pageManager.GetNewPageId())
	//print(pageManager.GetNewPageId())

}
