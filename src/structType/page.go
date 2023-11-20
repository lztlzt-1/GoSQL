package structType

import (
	"GoSQL/src/msg"
	"GoSQL/src/utils"
)

type Page struct {
	pageId      msg.PageId // 这个也可以用作判断页有效性
	nextPageID  msg.PageId
	freeSpace   msg.FreeSpaceTypeInTable
	pinCount    int
	pageHeadPos int16 //指向头部已存数据最后一个
	pageTailPos int16 //指向尾部已存数据最前一个
	isDirty     bool
	data        []byte
}

func (this *Page) GetFreeSpace() msg.FreeSpaceTypeInTable {
	return this.freeSpace
}

func (this *Page) SetFreeSpace(values msg.FreeSpaceTypeInTable) {
	this.freeSpace = values
}

func (this *Page) GetData() []byte {
	slice := this.data[:]
	return slice
}

func (this *Page) SetData(values []byte) {
	this.data = values
}

func (this *Page) AddData(values []byte) {
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

func (this *Page) GetRemainSize() int16 {
	return this.pageTailPos - this.pageHeadPos + 1
}

func (this *Page) GetHeaderPos() int16 {
	return this.pageHeadPos
}

func (this *Page) SetHeaderPosByOffset(value int16) {
	this.pageHeadPos += value
}

func (this *Page) SetHeaderPos(value int16) {
	this.pageHeadPos = value
}

func (this *Page) GetTailPos() int16 {
	return this.pageTailPos
}

func (this *Page) SetTailPos(value int16) {
	this.pageTailPos = value
}

// InsertDataToFreeSpace 在这里查找空余位置并判断
func (this *Page) InsertDataToFreeSpace(bytes []byte) (int, error) {
	index := this.GetFreeSpace()
	if int(index)+len(bytes) >= msg.PageRemainSize {
		return -2, nil
	}
	nextFreeSpace := utils.Bytes2Int16(this.data[index : index+2])
	if nextFreeSpace != 0 {
		this.SetFreeSpace(msg.FreeSpaceTypeInTable(nextFreeSpace))
	} else {
		this.SetFreeSpace(msg.FreeSpaceTypeInTable(int(index) + len(bytes)))
	}
	_, err := utils.InsertAndReplaceAtIndex(this.data, int(index), bytes)
	if err != nil {
		return -1, err
	}
	return msg.Success, nil
}
