package main

import "GoSQL/src/utils"

func main() {
	d := []int{123, 5421, 5214, 62999}
	b := utils.ListIntToBytes(d)
	d = utils.BytesToIntList(b)
	print(d)

	//Test()
	//fmt.Println("123")
}
