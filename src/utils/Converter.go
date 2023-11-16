package utils

import (
	"GoSQL/src/msg"
	"log"
)

func Int162Bytes(value int16) []byte {
	byteValue := make([]byte, 2)
	byteValue[1] = byte(value >> 8)
	byteValue[0] = byte(value)
	return byteValue
}

func Bytes2Int16(byteValue []byte) int16 {
	if len(byteValue) != 2 {
		return 0
	}
	value := int16(byteValue[1])<<8 | int16(byteValue[0])
	return value
}

func Int2Bytes(value int) []byte {
	byteValue := make([]byte, 4)
	byteValue[0] = byte(value & 0xFF)
	byteValue[1] = byte((value >> 8) & 0xFF)
	byteValue[2] = byte((value >> 16) & 0xFF)
	byteValue[3] = byte((value >> 24) & 0xFF)
	return byteValue
}

func Bytes2Int(byteValue []byte) int {
	if len(byteValue) != 4 {
		log.Fatal("error while change bytes to int")
		return 0 // 或者根据需要返回错误
	}

	var value int32
	for i := 0; i < 4; i++ {
		value |= int32(byteValue[i]) << (uint(i) * 8)
	}

	return int(value)
}

func Uint162Bytes(value uint16) []byte {
	byteValue := make([]byte, 2)
	byteValue[1] = byte(value & 0xFF)
	byteValue[0] = byte((value >> 8) & 0xFF)
	return byteValue
}

func Bytes2Uint16(byteValue []byte) uint16 {
	if len(byteValue) != 2 {
		return 0
	}
	value := uint16(byteValue[1]) | uint16(byteValue[0])<<8
	return value
}

func Bool2Bytes(value bool) []byte {
	if value {
		return []byte{1}
	}
	return []byte{0}
}

func Bytes2Bool(byteValue []byte) bool {
	if len(byteValue) == 0 {
		return false
	}
	return byteValue[0] != 0
}

func ListIntToBytes(myList []int) []byte {
	var myBytes []byte
	for _, num := range myList {
		myBytes = append(myBytes, Int2Bytes(num)...)
	}
	return myBytes
}

func BytesToIntList(myBytes []byte) []int {
	var myList []int
	for i := 0; i < len(myBytes)/msg.IntSize; i++ {
		bytes, err := ReadBytesFromPosition(myBytes, 4*i, 4)
		if err != nil {
			return nil
		}
		myList = append(myList, Bytes2Int(bytes))
	}
	return myList
}

func Uint322Bytes(value uint32) []byte {
	byteValue := make([]byte, 4)
	byteValue[0] = byte(value & 0xFF)
	byteValue[1] = byte((value >> 8) & 0xFF)
	byteValue[2] = byte((value >> 16) & 0xFF)
	byteValue[3] = byte((value >> 24) & 0xFF)
	return byteValue
}

func Bytes2Uint32(byteValue []byte) uint32 {
	if len(byteValue) != 4 {
		return 0
	}
	value := uint32(byteValue[0]) | uint32(byteValue[1])<<8 | uint32(byteValue[2])<<16 | uint32(byteValue[3])<<24
	return value
}

// Any2BytesForPage 给存页准备的任意类型转byte，其中string默认长度是msg.MaxStringLength
func Any2BytesForPage(value any) []byte {
	switch value.(type) {
	case int:
		return Int2Bytes(value.(int))
	case int16:
		return Int162Bytes(value.(int16))
	case uint16:
		return Uint162Bytes(value.(uint16))
	case bool:
		return Bool2Bytes(value.(bool))
	case string:
		bytes := []byte(value.(string))
		return FixSliceLength(bytes, msg.MaxStringLength)
	default:
		return nil
	}
}

func Bytes2Any(bytes []byte, itsType string) any {
	switch itsType {
	case "int":
		return Bytes2Int(bytes)
	case "bool":
		return Bytes2Bool(bytes)
	case "string":
		return string(RemoveTrailingNullBytes(bytes))
	default:
		return nil
	}
}
