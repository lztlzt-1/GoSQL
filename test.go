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

var openTableList *[]*Records.Table

// var initPage *pageMgr.InitPage
var GlobalDiskManager *diskMgr.DiskManager
var GlobalPageManager *pageMgr.PageManager
var GlobalBufferManager *buffer.BufferPoolManager

func Init() {
	var err error
	GlobalDiskManager, err = diskMgr.NewDiskManager(msg.DBName)
	if err != nil {
		log.Fatal(err)
	}
	initPage := diskMgr.GetInitPage(GlobalDiskManager)
	GlobalBufferManager = buffer.NewBufferPoolManager(msg.BufferBucketSize, GlobalDiskManager)
	if err != nil {
		log.Fatal(err)
	}
	GlobalPageManager, err = pageMgr.NewPageManager(initPage.GetInitPageID(), initPage)
	if err != nil {
		return
	}
	tables := make([]*Records.Table, 0, 10)
	openTableList = &tables

}

func Test() {
	Init()
	defer func() {
		for _, item := range *openTableList {
			if item.Name == "test222" {
				d := 1
				print(d)
			}
			//GlobalDiskManager.RefreshPages()
			err := item.SaveTableHead(GlobalDiskManager)
			if err != nil {
				log.Fatal(err)
			}
		}
		err := GlobalBufferManager.RefreshAll()
		if err != nil {
			return
		}
		err = GlobalDiskManager.DumpPageTable()
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
	table, err := Records.NewTable("test222", str, openTableList, GlobalPageManager, GlobalBufferManager, GlobalDiskManager)
	if err != nil {
		log.Fatal(err)
	}

	//加载测试
	//table, err := Records.LoadTableByName("test222", GlobalBufferManager, GlobalDiskManager, openTableList)
	//if err != nil {
	//	log.Fatal(err)
	//}

	//插入测试
	for i := 0; i < 100; i++ {
		str = ""
		for j := 0; j < 30; j++ {
			if j != 2 {
				str += fmt.Sprint(i, " ")
			} else {
				str += fmt.Sprint(2, " ")
			}

		}
		if table.RecordSize+1 < msg.PageRemainSize {
			err = table.Insert(str, GlobalDiskManager, GlobalBufferManager)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			err := table.InsertBigRecord(str, GlobalBufferManager, GlobalDiskManager)
			if err != nil {
				log.Fatal(err)
			}
		}
	}

	//查询测试
	//err = GlobalBufferManager.RefreshAll()
	//if err != nil {
	//	return
	//}
	//str3 := []string{"test2", "test3"}
	//str2 := []any{3, 100}
	//_, err = table.Query(str3, str2, GlobalBufferManager)
	//if err != nil {
	//	log.Fatal(err)
	//}

	//更新测试
	err = GlobalBufferManager.RefreshAll()
	str3 := []string{"test2"}
	str2 := []any{2}
	str6 := []any{3, 100}
	str5 := []string{"test2", "test3"}
	err = table.Update(str3, str2, str5, str6, GlobalBufferManager, GlobalDiskManager)
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
