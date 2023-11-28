package structType

import (
	"GoSQL/src/msg"
)

type Page struct {
	pageId     msg.PageId // 这个也可以用作判断页有效性
	nextPageID msg.PageId
	freeSpace  msg.FreeSpaceTypeInTable
	pinCount   int  // 不用写进内存
	isDirty    bool // 不用写进内存
	data       []byte
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
}

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
