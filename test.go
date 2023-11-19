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
	initPage := diskMgr.GetInitPage(GlobalDiskManager)
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
		//for _, item := range *tableList {
		//	if item.Name == "test222" {
		//		d := 1
		//		print(d)
		//	}
		//	err := (*item).ToDisk(GlobalDiskManager, GlobalPageManager)
		//	if err != nil {
		//		log.Fatal(err)
		//	}
		//}
		err := GlobalDiskManager.DumpPageTable()
		if err != nil {
			return
		}
		err = GlobalPageManager.GetInitPage().SetInitPageToDisk(GlobalDiskManager)
		if err != nil {
			log.Fatal(err)
		}
	}()
	// 上面是持久化的固定操作
	//for i := 0; i < 300; i++ {
	//	str := fmt.Sprintf("test{%v}", i)
	//	tanle1, err := Records.NewTable(str, "schoolName string classNum int", tableList, GlobalPageManager, GlobalDiskManager)
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//	tanle1.Insert("hdu 7")
	//}
	str := ""
	for i := 0; i < 200; i++ {
		str += fmt.Sprint("test", i, " int ")
	}
	table, err := Records.NewTable("test222", str, tableList, GlobalPageManager, GlobalDiskManager)
	if err != nil {
		log.Fatal(err)
	}
	//table, err := Records.LoadTableByName("test222", GlobalDiskManager, tableList)
	//if err != nil {
	//	log.Fatal(err)
	//}
	fmt.Println(table)
	//str := ""
	//for i := 0; i < 200; i++ {
	//	str += fmt.Sprint(i, " ")
	//}
	//for i := 0; i < 60; i++ {
	//	if table.RecordSize<msg.PageRemainSize{
	//		err = table.Insert(str)
	//	}else {
	//		// todo: 当一个记录超过1页
	//	}
	//
	//}

	if err != nil {
		return
	}

	//err = table.Insert("hdu 100")
	//if err != nil {
	//	log.Fatal(err)
	//}
	//fmt.Println(table)
}
