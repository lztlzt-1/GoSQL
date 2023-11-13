package main

import (
	"GoSQL/src/Records"
	"GoSQL/src/msg"
	"GoSQL/src/storage"
	"fmt"
	"log"
)

func GlobalInit() (storage.DiskManager, storage.PageManager, []*Records.Table, *storage.InitPage) {
	diskManager, err := storage.NewDiskManager(msg.DBName)
	if err != nil {
		log.Fatal(err)
	}
	initPage := storage.GetInitPage(*diskManager)
	pageManager := storage.NewPageManager(initPage.GetInitPageID(), &initPage)
	var tableList []*Records.Table
	return *diskManager, pageManager, tableList, &initPage
}
func Test() {
	diskManager, pageManager, tableList, initPage := GlobalInit()
	defer func() {
		for _, item := range tableList {
			err := (*item).ToDisk(pageManager, diskManager)
			if err != nil {
				log.Fatal(err)
			}
		}
		err := initPage.SetInitPageToDisk(diskManager)
		if err != nil {
			log.Fatal(err)
		}
	}()
	// 上面是持久化的固定操作
	table, err := Records.NewTable("test222", "schoolName string classNum int", &tableList, diskManager)
	if err != nil {
		log.Fatal(err)
	}
	err = table.Insert("hdu 7")
	if err != nil {
		return
	}
	//table, err := Factory.LoadTableByName("test222", diskManager, &tableList)
	//if err != nil {
	//	log.Fatal(err)
	//}
	//err = table.Insert("hdu 100")
	//if err != nil {
	//	log.Fatal(err)
	//}
	fmt.Println(table)
}
