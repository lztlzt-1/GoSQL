package pageMgr

import (
	"GoSQL/src/msg"
	"GoSQL/src/storage/diskMgr"
	"GoSQL/src/structType"
	"GoSQL/src/utils"
	"errors"
)

type PageManager struct {
	GetNewPageId func() msg.PageId
	initPage     *InitPage
}

var GlobalPageManager PageManager

func NewPageManager(initState msg.PageId, page *InitPage) error {
	GlobalPageManager = PageManager{
		GetNewPageId: NewPageId(initState),
		initPage:     page,
	}
	return nil
}

// NewPageId 获取一个新的pageId
func NewPageId(initState msg.PageId) func() msg.PageId {
	generatePageId := func(state any) any {
		cur := state.(msg.PageId)
		cur = cur + 1
		return cur
	}
	pageGenerator := utils.LazyGenerator(generatePageId, initState)
	return func() msg.PageId {
		initState = pageGenerator().(msg.PageId)
		return initState
	}
}

// NewPage 生成一个新页,返回指针
func (this *PageManager) NewPage() *structType.Page {
	pageId := this.GetNewPageId()
	page := new(structType.Page)
	page.SetPageId(pageId)
	page.SetPinCount(0)
	page.SetDirty(false)
	page.SetTailPos(msg.PageRemainSize - 1)
	page.SetHeaderPos(0)
	page.SetNextPageId(-1)
	this.initPage.SetInitPageID(pageId)
	//page.pageSize = 0
	page.SetData(make([]byte, msg.PageRemainSize))
	return page
}

func (this *PageManager) NewPageWithID(id msg.PageId) *structType.Page {
	pageId := id
	page := new(structType.Page)
	page.SetPageId(pageId)
	page.SetPinCount(0)
	page.SetDirty(false)
	page.SetTailPos(msg.PageRemainSize - 1)
	page.SetHeaderPos(0)
	this.initPage.SetInitPageID(pageId)
	//page.pageSize = 0
	page.SetData(make([]byte, msg.PageRemainSize))
	return page
}

// Deprecated: InsertTuple slotted Page方法，内存需要过多，暂时弃用
func (this *PageManager) InsertTuple(page structType.Page, value []byte) error {
	if int(page.GetRemainSize()) < len(value) {
		return errors.New("error: index out of range while inserting")
	}
	var err error
	insertPos := int(page.GetTailPos()) - len(value) + 1
	// 插入插槽，最前面1b=1表示数据有效
	data, err := utils.InsertAndReplaceAtIndex[byte](page.GetData(), 0, utils.Uint162Bytes(uint16((1<<15)+insertPos)))
	if err != nil {
		return err
	}
	page.SetData(data)
	page.SetHeaderPos(2)
	// 插入数据
	data, err = utils.InsertAndReplaceAtIndex[byte](page.GetData(), insertPos, value)
	if err != nil {
		return err
	}
	page.SetData(data)
	return nil
}

// 用于将超过单页大小的数据分段
func (this *PageManager) insertMultipleData(page structType.Page, value []byte, tupleSize int, GlobalDiskManager diskMgr.DiskManager) error {
	insertSize := msg.PageRemainSize / tupleSize * tupleSize
	var newPage *structType.Page
	if len(value) > insertSize { // 当前插入的数据长度大于页中可存储大小，需要链接上新页
		if page.GetNextPageId() == -1 {
			newPage = this.CreateNextPage(page)
		} else {
			var err error
			pageValue, err := diskMgr.GlobalDiskManager.GetPageById(page.GetNextPageId())
			if err != nil {
				return err
			}
			newPage = &pageValue
		}
		page.SetNextPageId(newPage.GetPageId())
	}
	err := this.InsertDataAndToDisk(page, value[:insertSize], tupleSize, GlobalDiskManager)
	value = value[insertSize:]
	if err != nil {
		return err
	}
	// 利用递归进行增加page和插入数据操作
	err = this.InsertDataAndToDisk(*newPage, value, tupleSize, GlobalDiskManager)
	if err != nil {
		return err
	} // 直接写入disk中
	//err = this.ToDisk()
	if err != nil {
		return err
	}

	return nil
}

func (this *PageManager) ToDisk(page structType.Page, GlobalDiskManager diskMgr.DiskManager) error {
	_, err := GlobalDiskManager.WritePage(page.GetPageId(), &page)
	if err != nil {
		return err
	}
	return nil
}

// InsertDataAndToDisk 这里会自动写入页之后写到disk中,这里写的是一个完整的页
func (this *PageManager) InsertDataAndToDisk(page structType.Page, value []byte, tupleSize int, GlobalDiskManager diskMgr.DiskManager) error {
	// 如果数据长度大于容量，则调用insertMultipleData进行保存
	if int(msg.PageRemainSize)/tupleSize*tupleSize < len(value) {
		err := this.insertMultipleData(page, value, tupleSize, GlobalDiskManager)
		if err != nil {
			return err
		}
	} else {
		// 不然就直接保存
		var err error
		data, err := utils.InsertAndReplaceAtIndex[byte](page.GetData(), 0, value)
		if err != nil {
			return err
		}
		page.SetData(data)
		page.SetHeaderPosByOffset(uint16(len(value)))
		err = this.ToDisk(page, GlobalDiskManager)
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *PageManager) CreateNextPage(page structType.Page) *structType.Page {
	newPage := this.NewPage()
	page.SetNextPageId(newPage.GetPageId())
	return newPage
}
