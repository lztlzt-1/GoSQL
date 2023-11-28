package structType

import (
	"GoSQL/src/msg"
)

type Page struct {
	pageId     msg.PageId // 这个也可以用作判断页有效性
	nextPageID msg.PageId
	freeSpace  msg.FreeSpaceTypeInTable
	pinCount   int // 不用写进内存
	//pageHeadPos int16 //指向头部已存数据最后一个
	//pageTailPos int16 //指向尾部已存数据最前一个
	isDirty bool // 不用写进内存
	data    []byte
}

func (this *Page) GetFreeSpace() msg.FreeSpaceTypeInTable {
	return this.freeSpace
}

func (this *Page) SetFreeSpace(values msg.FreeSpaceTypeInTable) {
	this.freeSpace = values
	this.isDirty = true
}

func (this *Page) GetData() []byte {
	slice := this.data[:]
	return slice
}

func (this *Page) SetData(values []byte) {
	this.data = values
	this.isDirty = true
	//this.pageHeadPos = int16(len(this.data))
}

//func (this *Page) AddData(values []byte) {
//	this.data = values
//	this.isDirty = true
//}

func (this *Page) GetPageId() msg.PageId {
	return this.pageId
}

func (this *Page) SetPageId(id msg.PageId) {
	this.pageId = id
	this.isDirty = true
}

func (this *Page) GetNextPageId() msg.PageId {
	return this.nextPageID
}

func (this *Page) SetNextPageId(id msg.PageId) {
	this.nextPageID = id
	this.isDirty = true
}

func (this *Page) GetPinCount() int {
	return this.pinCount
}

func (this *Page) Pin() int {
	this.pinCount++
	return this.pinCount
}

func (this *Page) UnPin() int {
	this.pinCount--
	return this.pinCount
}

func (this *Page) SetPinCount(pinCount int) {
	this.pinCount = pinCount
}

func (this *Page) IsDirty() bool {
	return this.isDirty
}

func (this *Page) SetDirty(isDirty bool) {
	this.isDirty = isDirty
}

//func (this *Page) GetRemainSize() int16 {
//	return pagere - this.pageHeadPos + 1
//}

//func (this *Page) GetHeaderPos() int16 {
//	return this.pageHeadPos
//}
//
//func (this *Page) SetHeaderPosByOffset(value int16) {
//	this.pageHeadPos += value
//	this.isDirty = true
//}
//
//func (this *Page) SetHeaderPos(value int16) {
//	this.pageHeadPos = value
//	this.isDirty = true
//}

//func (this *Page) GetTailPos() int16 {
//	return this.pageTailPos
//}
//
//func (this *Page) SetTailPos(value int16) {
//	this.pageTailPos = value
//	this.isDirty = true
//}
