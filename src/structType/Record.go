package structType

// 这个目录记录一些可能会被循环调用的结构体

type Record struct {
	Value []any
}
