package main

import (
	"GoSQL/src/Records"
	"GoSQL/src/buffer"
	"GoSQL/src/msg"
	"GoSQL/src/storage/diskMgr"
	"GoSQL/src/storage/pageMgr"
	"fmt"
	"log"
)

var tableList *[]*Records.Table

// var initPage *pageMgr.InitPage
var GlobalDiskManager *diskMgr.DiskManager
var GlobalPageManager *pageMgr.PageManager

func Init() {
	var err error
	GlobalDiskManager, err = diskMgr.NewDiskManager(msg.DBName)
	if err != nil {
		log.Fatal(err)
	}
	initPage := pageMgr.GetInitPage(GlobalDiskManager)
	err = buffer.NewBufferPoolManager(8)
	if err != nil {
		log.Fatal(err)
	}
	GlobalPageManager, err = pageMgr.NewPageManager(initPage.GetInitPageID(), initPage)
	if err != nil {
		return
	}
	tables := make([]*Records.Table, 0, 10)
	tableList = &tables
}

func Test() {
	Init()
	defer func() {

		err := GlobalDiskManager.DumpPageTable()
		if err != nil {
			return
		}
		for _, item := range *tableList {
			err := (*item).ToDisk(GlobalDiskManager, GlobalPageManager)
			if err != nil {
				log.Fatal(err)
			}
		}
		err = GlobalPageManager.GetInitPage().SetInitPageToDisk(GlobalDiskManager)
		if err != nil {
			log.Fatal(err)
		}
	}()
	// 上面是持久化的固定操作
	for i := 0; i < 300; i++ {
		str := fmt.Sprintf("test{%v}", i)
		_, err := Records.NewTable(str, "schoolName string classNum int", tableList, GlobalPageManager, GlobalDiskManager)
		if err != nil {
			log.Fatal(err)
		}
	}
	table, err := Records.NewTable("test222", "schoolName string classNum int", tableList, GlobalPageManager, GlobalDiskManager)
	if err != nil {
		log.Fatal(err)
	}
	//table, err := Factory.LoadTableByName("test222", diskMgr.GlobalDiskManager, &tableList)
	//if err != nil {
	//	log.Fatal(err)
	//}
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
