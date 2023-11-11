package main

import (
	"GoSQL/src/Records"
	"log"
)

func main() {
	table, err := Records.NewTable("test", "name string age int isStudent bool")
	if err != nil {
		return
	}
	table.Insert("lzt 123 false")
	table.Insert("lzt1 12322 false")
	table.Insert("lzt1 1232 false")
	table.Insert("lzt1 12333 true")
	table.Insert("lzt1 11111 true")
	table.Insert("lzt1 11111 false")
	err = table.Delete([]string{"name"}, []any{"lzt1"})
	if err != nil {
		log.Fatal(err)
	}
	table = table
	d := 1
	d = d - 1
	print(1)
	//Test()
	//fmt.Println("123")
}
