package utils

import (
	"GoSQL/src/msg"
	"crypto/sha256"
	"errors"
	"os"
)

// LazyGenerator 惰性生成器，利用协程提前生成下一个值，可以提供给不同函数
func LazyGenerator(calFunc func(any) any, initState any) func() any {
	funcChannel := make(chan any)
	genFunc := func() chan any {
		var lastValue any = initState
		//var nowValue any
		for {
			lastValue = calFunc(lastValue)
			funcChannel <- lastValue
		}
	}
	returnFunc := func() any {
		return <-funcChannel
	}
	go genFunc()
	return returnFunc
}

// InsertAndReplaceAtIndex 在切片slice的index地方插入一段切片，如果插入后长度超过则不插入,这个函数是用来替换原有数据的，如果没有则不能使用
func InsertAndReplaceAtIndex[T int | byte](slice []T, index int, values []T) ([]T, error) {
	if index < 0 || index+len(values) > len(slice) {
		return slice, errors.New("index out of range")
	}
	before := slice[:index]
	after := slice[index+len(values):]
	result := append(append(before, values...), after...)

	return result, nil
}

// ReadBytesFromPosition 从data偏移量pos后提取长度length的字节
func ReadBytesFromPosition(data []byte, pos int, length int) ([]byte, error) {
	if pos < 0 || pos >= len(data) || pos+length > len(data) || length < 0 {
		return nil, errors.New("Invalid position or length")
	}
	result := make([]byte, length)
	copy(result, data[pos:pos+length])
	return result, nil
}

// FastPow 快速幂，求value^N
func FastPow[T int | float64 | int32 | int64](value T, N int) T {
	base := value
	var res T = 1
	for N != 0 {
		if N%2 == 1 {
			res *= base
		}
		base *= base
		N = N >> 1
	}
	return res
}

// DeleteElement 删除列表的指定元素
func DeleteElement[T int | float64 | Pair](slice []T, index int) []T {
	return append(slice[:index], slice[index+1:]...)
}

func DeleteElementNormal(slice []any, index int) []any {
	return append(slice[:index], slice[index+1:]...)
}

// HashValueSHA256 求hash值
func HashValueSHA256(value any) []byte {
	hasher := sha256.New()
	valBytes := Any2BytesForPage(value)
	hasher.Write(valBytes)
	return hasher.Sum(nil)
}

// GetHashValueSHA256ToInt 对于一个值求hash并取前4B
func GetHashValueSHA256ToInt(value any) int {
	v := HashValueSHA256(value)
	bytes, err := ReadBytesFromPosition(v, 0, 4)
	if err != nil {
		return 0
	}
	return Bytes2Int(bytes)
}

// FixSliceLength 将一个byte切片填充null到cap
func FixSliceLength(slice []byte, length int) []byte {
	currentLength := len(slice)
	if currentLength >= length {
		return slice[:length]
	}
	return append(slice, make([]byte, length-len(slice))...)
}

func JudgeSize(itsType string) int {
	switch itsType {
	case "int":
		return msg.IntSize
	case "bool":
		return msg.BoolSize
	case "float":
		return msg.FloatSize
	case "string":
		return msg.StringSize
	default:
		return msg.ErrorType
	}
}

// RemoveTrailingNullBytes 去掉数组冲最后面的0x00
func RemoveTrailingNullBytes(input []byte) []byte {
	// 找到最后一个非零字节的位置
	lastNonZero := len(input) - 1
	for lastNonZero >= 0 && input[lastNonZero] == 0x00 {
		lastNonZero--
	}
	// 切片去掉尾部的零字节
	return input[:lastNonZero+1]
}

// FileExists 判断是否存在某个文件
func FileExists(filePath string) bool {
	_, err := os.Stat(filePath)
	return !os.IsNotExist(err)
}
