package structType

import (
	"GoSQL/src/msg"
)

type Page struct {
	pageId      msg.PageId
	nextPageID  msg.PageId
	pinCount    int
	pageHeadPos uint16 //指向头部已存数据最后一个
	pageTailPos uint16 //指向尾部已存数据最前一个
	isDirty     bool
	data        []byte
}

func (this *Page) GetData() []byte {
	slice := this.data[:]
	return slice
}

func (this *Page) SetData(values []byte) {
	this.data = values
}

func (this *Page) GetPageId() msg.PageId {
	return this.pageId
}

func (this *Page) SetPageId(id msg.PageId) {
	this.pageId = id
}

func (this *Page) GetNextPageId() msg.PageId {
	return this.nextPageID
}

func (this *Page) SetNextPageId(id msg.PageId) {
	this.nextPageID = id
}

func (this *Page) GetPinCount() int {
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

func (this *Page) GetRemainSize() uint16 {
	return this.pageTailPos - this.pageHeadPos + 1
}

func (this *Page) GetHeaderPos() uint16 {
	return this.pageHeadPos
}

func (this *Page) SetHeaderPosByOffset(value uint16) {
	this.pageHeadPos += value
}

func (this *Page) SetHeaderPos(value uint16) {
	this.pageHeadPos = value
}

func (this *Page) GetTailPos() uint16 {
	return this.pageTailPos
}

func (this *Page) SetTailPos(value uint16) {
	this.pageTailPos = value
}
