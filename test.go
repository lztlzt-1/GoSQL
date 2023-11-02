package main

import (
	"GoSQL/src/disk"
	"log"
)

func Test() {
	diskManager, err := disk.NewDiskManager("test.db")
	if err != nil {
		log.Fatal(err)
	}
	_, err := diskManager.GetData(0, 5)
	if err != nil {
		log.Fatal(err)
	}
	_, err = diskManager.WritePage(3, []byte("hello world"))
	if err != nil {
		log.Fatal(err)
	}
}
