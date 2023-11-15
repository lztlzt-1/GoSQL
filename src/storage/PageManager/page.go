package PageManager

import (
	"GoSQL/src/msg"
	"GoSQL/src/storage/DiskManager"
	"GoSQL/src/utils"
	"errors"
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

// Deprecated: InsertTuple slotted Page方法，内存需要过多，暂时弃用
func (this *Page) InsertTuple(value []byte) error {
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

// 用于将超过单页大小的数据分段
func (this *Page) insertMultipleData(value []byte) error {
	remainSize := this.GetRemainSize()
	err := this.InsertDataAndToDisk(value[:remainSize])
	value = value[remainSize:]
	if err != nil {
		return err
	}
	var newPage *Page
	if this.nextPageID == -1 {
		newPage = this.CreateNextPage()
	} else {
		*newPage, err = DiskManager.GlobalDiskManager.GetPageById(this.nextPageID)
		if err != nil {
			return err
		}
	}
	// 利用递归进行增加page和插入数据操作
	err = newPage.InsertDataAndToDisk(value)
	if err != nil {
		return err
	} // 直接写入disk中
	//err = this.ToDisk()
	if err != nil {
		return err
	}

	return nil
}

func (this *Page) ToDisk() error {
	_, err := DiskManager.GlobalDiskManager.WritePage(this.GetPageId(), this)
	if err != nil {
		return err
	}
	return nil
}

// InsertDataAndToDisk 这里会自动写入页之后写到disk中
func (this *Page) InsertDataAndToDisk(value []byte) error {
	// 如果数据长度大于容量，则调用insertMultipleData进行保存
	if int(this.GetRemainSize()) < len(value) {
		err := this.insertMultipleData(value)
		if err != nil {
			return err
		}
	} else {
		// 不然就直接保存
		var err error
		this.data, err = utils.InsertAndReplaceAtIndex[byte](this.data, int(this.pageHeadPos), value)
		if err != nil {
			return err
		}
		this.pageHeadPos += uint16(len(value))
		err = this.ToDisk()
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *Page) CreateNextPage() *Page {
	newPage := GlobalPageManager.NewPage()
	this.nextPageID = newPage.GetPageId()
	return newPage
}

func (this *Page) GetNextID() msg.PageId {
	return this.nextPageID
}
