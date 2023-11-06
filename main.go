package main

import (
	"GoSQL/src/algorithm/ExtendibleHash"
	"GoSQL/src/utils"
)

func ddd[T float64 | int](d1 T, d2 T) T {
	return d1 + d2
}
func main() {
	d := utils.GetHashValueSHA256ToInt(123)
	e := ExtendibleHash.NewExtendibleHash(2)
	e.Insert(1, 123)
	e.Insert(3, 123)
	e.Insert(5, 123)
	e.Insert(7, 123)
	e.Insert(9, 123)
	e.Insert(15, 123)
	print(d)
	//print(d)
	//Test()
	//fmt.Println("123")
}
