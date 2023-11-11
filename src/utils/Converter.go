package utils

import "GoSQL/src/dataTypes"

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
		return 0
	}
	value := int(byteValue[0]) | int(byteValue[1])<<8 | int(byteValue[2])<<16 | int(byteValue[3])<<24
	return value
}

func Uint162Bytes(value uint16) []byte {
	byteValue := make([]byte, 2)
	byteValue[1] = byte(value & 0xFF)
	byteValue[0] = byte((value >> 8) & 0xFF)
	return byteValue
}

func BytesToUint16(byteValue []byte) uint16 {
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
	for i := 0; i < len(myBytes)/dataTypes.IntSize; i++ {
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

func Any2() {

}
