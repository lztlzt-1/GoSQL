package storage

import (
	"GoSQL/src/msg"
	"GoSQL/src/utils"
	"errors"
)

type Page struct {
	pageId      msg.PageId
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

func (this *Page) GetPageId() msg.PageId {
	return this.pageId
}

func (this *Page) GetPinCount() int {
	return this.pinCount
}

func (this *Page) IsDirty() bool {
	return this.isDirty
}

func (this *Page) GetRemainSize() uint16 {
	return this.pageTailPos - this.pageHeadPos + 1
}

func (this *Page) GetHeaderPos() uint16 {
	return this.pageHeadPos
}

func (this *Page) SetHeaderPos(value uint16) {
	this.pageHeadPos += value
}

func (this *Page) GetTailPos() uint16 {
	return this.pageTailPos
}

func (this *Page) Insert(value []byte) error {
	if int(this.GetRemainSize()) < len(value) {
		return errors.New("error: index out of range while inserting")
	}
	var err error
	insertPos := int(this.GetTailPos()) - len(value) + 1
	// 插入插槽，最前面1b=1表示数据有效
	this.data, err = utils.InsertAndReplaceAtIndex[byte](this.data, 0, utils.Uint162Bytes(uint16((1<<15)+insertPos)))
	if err != nil {
		return err
	}
	this.SetHeaderPos(2)
	// 插入数据
	this.data, err = utils.InsertAndReplaceAtIndex[byte](this.data, insertPos, value)
	if err != nil {
		return err
	}
	return nil
}

//func (this *Page) Query(value []byte) error {
//	//return
//}
