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
		for _, item := range *tableList {
			if item.Name == "test222" {
				d := 1
				print(d)
			}
			_, err := GlobalDiskManager.WritePage(item.CurPage.GetPageId(), item.CurPage)
			if err != nil {
				log.Fatal(err)
			}
			err = item.SaveTableHead(GlobalDiskManager)
			if err != nil {
				log.Fatal(err)
			}
		}
		err := GlobalDiskManager.DumpPageTable()
		if err != nil {
			return
		}
		err = GlobalDiskManager.DumpInitPage()
		if err != nil {
			log.Fatal(err)
		}
		//err = GlobalPageManager.GetInitPage().SetInitPageToDisk(GlobalDiskManager)
		//if err != nil {
		//	log.Fatal(err)
		//}
	}()
	// 上面是持久化的固定操作
	str := ""

	//新增table
	for i := 0; i < 30; i++ {
		str += fmt.Sprint("test", i, " int ")
	}
	table, err := Records.NewTable("test222", str, tableList, GlobalPageManager, GlobalDiskManager)
	if err != nil {
		log.Fatal(err)
	}

	//加载测试
	//table, err := Records.LoadTableByName("test222", GlobalDiskManager, tableList)
	//if err != nil {
	//	log.Fatal(err)
	//}

	//插入测试
	str = ""
	for i := 0; i < 29; i++ {
		str += fmt.Sprint(i, " ")
	}
	str1 := str
	for i := 0; i < 60; i++ {
		str1 = str + fmt.Sprint(i+1000, " ")
		if table.RecordSize+1 < msg.PageRemainSize {
			err = table.Insert(str1, GlobalDiskManager)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			err := table.InsertBigRecord(str, GlobalDiskManager)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	//查询测试
	str3 := []string{"test2"}
	str2 := []any{2}
	_, err = table.Query(str3, str2, GlobalDiskManager)
	if err != nil {
		log.Fatal(err)
	}

	//err = table.Insert("hdu 100")
	//if err != nil {
	//	log.Fatal(err)
	//}
	page1, err := GlobalDiskManager.GetPageById(3)
	fmt.Println(page1)
	if err != nil {
		return
	}
	print(str)
	fmt.Println(table)
}
