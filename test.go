package main

import (
	"GoSQL/src/Factory"
	"GoSQL/src/Records"
	"GoSQL/src/buffer"
	"GoSQL/src/msg"
	"GoSQL/src/storage/diskMgr"
	"GoSQL/src/storage/pageMgr"
	"log"
)

var tableList []*Records.Table
var initPage pageMgr.InitPage

func Init() {
	err := diskMgr.NewDiskManager(msg.DBName)
	if err != nil {
		log.Fatal(err)
	}
	initPage = pageMgr.GetInitPage()
	err = buffer.NewBufferPoolManager(8)
	if err != nil {
		log.Fatal(err)
	}

	err = pageMgr.NewPageManager(initPage.GetInitPageID(), &initPage)
	if err != nil {
		return
	}
}

func Test() {
	Init()
	defer func() {
		for _, item := range tableList {
			err := (*item).ToDisk(diskMgr.GlobalDiskManager, pageMgr.GlobalPageManager)
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
	table, err := Factory.LoadTableByName("test222", diskMgr.GlobalDiskManager, &tableList)
	if err != nil {
		log.Fatal(err)
	}
	for i := 0; i < 30; i++ {
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
