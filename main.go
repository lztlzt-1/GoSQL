package main

import (
	"GoSQL/src/Records"
	"encoding/json"
	"fmt"
	"log"
	"os"
)

func main() {
	str := "123123"
	file, err := os.OpenFile("tt.db", os.O_RDWR, 0660)
	file.Write([]byte(str))
	file.Close()
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
	table.ToDisk()
	//err = table.Delete([]string{"name"}, []any{"lzt1"})
	marshal, err := json.Marshal(*table)
	if err != nil {
		return
	}
	print(marshal)
	if err != nil {
		log.Fatal(err)
	}
	var newTable Records.Table
	json.Unmarshal(marshal, &newTable)
	fmt.Println(newTable)
	table = table
	//d := 1
	//d = d - 1
	print(1)
	//Test()
	//fmt.Println("123")
}
