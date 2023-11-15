package main

import (
	"GoSQL/src/Factory"
	"GoSQL/src/Records"
	"GoSQL/src/buffer"
	"GoSQL/src/msg"
	"GoSQL/src/storage/DiskManager"
	"GoSQL/src/storage/PageManager"
	"log"
)

var tableList []*Records.Table
var initPage PageManager.InitPage

func Init() {
	err := DiskManager.NewDiskManager(msg.DBName)
	if err != nil {
		log.Fatal(err)
	}
	initPage = PageManager.GetInitPage()
	err = buffer.NewBufferPoolManager(8)
	if err != nil {
		log.Fatal(err)
	}

	err = PageManager.NewPageManager(initPage.GetInitPageID(), &initPage)
	if err != nil {
		return
	}
}

func Test() {
	Init()
	defer func() {
		for _, item := range tableList {
			err := (*item).ToDisk()
			if err != nil {
				log.Fatal(err)
			}
		}
		err := initPage.SetInitPageToDisk()
		if err != nil {
			log.Fatal(err)
		}
	}()
	// 上面是持久化的固定操作
	//table, err := Records.NewTable("test222", "schoolName string classNum int", &tableList)
	//if err != nil {
	//	log.Fatal(err)
	//}
	table, err := Factory.LoadTableByName("test222", DiskManager.GlobalDiskManager, &tableList)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 18; i++ {
		err = table.Insert("hdu 7")
	}

	if err != nil {
		return
	}

	//err = table.Insert("hdu 100")
	//if err != nil {
	//	log.Fatal(err)
	//}
	//fmt.Println(table)
}
