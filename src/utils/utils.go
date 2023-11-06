package utils

import "errors"

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

// InsertAndReplaceAtIndex 在切片slice的index地方插入一段切片，如果插入后长度超过则不插入
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
func FastPow[T int | float64 | int32](value T, N int) T {
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
