package main

import (
	"GoSQL/src/Records"
)

func main() {
	table, err := Records.NewTable("name string age int isStudent bool")
	if err != nil {
		return
	}
	table.Insert("lzt 123 false")
	table.Insert("lzt1 12322 false")
	table.Insert("lzt1 1232 false")
	table.Insert("lzt1 12333 rrr")
	table = table
	d := 1
	d = d - 1
	print(1)
	//Test()
	//fmt.Println("123")
}
