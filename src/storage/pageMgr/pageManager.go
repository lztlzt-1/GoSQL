package pageMgr

import (
	"GoSQL/src/msg"
	"GoSQL/src/storage/diskMgr"
	"GoSQL/src/structType"
	"GoSQL/src/utils"
	"errors"
)

type PageManager struct {
	initPage   *diskMgr.InitPage
	lastPageID msg.PageId
}

func NewPageManager(initState msg.PageId, page *diskMgr.InitPage) (*PageManager, error) {
	GlobalPageManager := PageManager{
		initPage: page,
	}
	return &GlobalPageManager, nil
}

func (this *PageManager) GetInitPage() *diskMgr.InitPage {
	return this.initPage
}

func (this *PageManager) GetLastPageID() msg.PageId {
	return this.lastPageID
}

func (this *PageManager) SetLastPageID(id msg.PageId) {
	this.lastPageID = id
}

// NewPage 生成一个新页,返回指针
func (this *PageManager) NewPage(diskManager *diskMgr.DiskManager) *structType.Page {
	pageId := diskManager.GetNewPageId()
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

// InsertMultipleDataForTable 用于将整个表插入内存中，new后或者修改表结构使用
func (this *PageManager) InsertMultipleDataForTable(page *structType.Page, value []byte, headSize int, recordSize int, GlobalDiskManager *diskMgr.DiskManager) error {
	recordSize++ // 对于插入来说每一个记录多了1B的flag，这个flag不写到record对象里，所以只在这里+1
	if (len(value)-headSize)%recordSize != 0 {
		return errors.New("headSize error")
	}
	if msg.PageRemainSize >= len(value) {
		err := this.insertDataAndToDisk(page, value, GlobalDiskManager)
		if err != nil {
			return err
		}
		return nil
	}
	//todo: 先预分配好所有页
	//pageSum := headSize/msg.PageRemainSize + 1
	//pageSum += 1
	//pageSum += (len(value)-headSize)/msg.PageRemainSize + 1
	//if page.GetPageId() != -1 {
	//	pageSum--
	//}
	//var nextPages []msg.PageId
	//for i := 0; i < pageSum; i++ {
	//	nextPages = append(nextPages, GlobalDiskManager.GetNewPageId())
	//}
	//先处理表头
	head := make([]byte, 0, msg.PageRemainSize)
	head = append(head, value[:headSize]...)
	value = value[headSize:]
	//头数据1页放不下，或者放完数据后不能再放下一个record
	for len(head) > msg.PageRemainSize || msg.PageRemainSize-len(head) < recordSize {
		// 处理头数据，因为头数据一般不常变动，所以直接当做字节流处理，读的时候一起读掉，当修改表结构时会重新写入所有
		// 这里总共需要分配len(head)/msg.PageRemainSize页
		err := this.insertDataAndToDisk(page, head[:msg.PageRemainSize], GlobalDiskManager)
		if err != nil {
			return err
		}
		head = head[:msg.PageRemainSize]
		nextPage, err := this.GetNextPage(page, GlobalDiskManager)
		if err != nil {
			return err
		}
		page = nextPage
	}
	// 处理到这table的头已经可以在1页中放下了，需要处理头数据+一些record数据的情况，此时必然可以放下至少一个record
	// 这里总共需要分配1页
	remainSize := msg.PageRemainSize - len(head)
	recordNum := remainSize / recordSize
	head = append(head, value[:recordNum*recordSize]...)
	value = value[recordNum*recordSize:]
	err := this.insertDataAndToDisk(page, head, GlobalDiskManager)
	if err != nil {
		return err
	}
	nextPage, err := this.GetNextPage(page, GlobalDiskManager)
	if err != nil {
		return err
	}
	page = nextPage
	sizeInOnePage := msg.PageRemainSize / recordSize
	//已经处理完包括头文件的所有数据，下面的数据只有record
	for {
		// 这里总共⌈len(value)/(sizeInOnePage*recordSize)⌉个page，其中len(value)=len(value)-headSize
		if len(value) <= msg.PageRemainSize {
			err := this.insertDataAndToDisk(page, value, GlobalDiskManager)
			if err != nil {
				return err
			}
			return nil
		}
		err := this.insertDataAndToDisk(page, value[:sizeInOnePage*recordSize], GlobalDiskManager)
		if err != nil {
			return err
		}
		value = value[sizeInOnePage*recordSize:]
		nextPage, err := this.GetNextPage(page, GlobalDiskManager)
		if err != nil {
			return err
		}
		page = nextPage
	}
}

func (this *PageManager) GetNextPage(page *structType.Page, diskManager *diskMgr.DiskManager) (*structType.Page, error) {
	var newPage *structType.Page
	if page.GetNextPageId() == -1 {
		newPage = this.CreateNextPage(page, diskManager)
	} else {
		var err error
		pageValue, err := diskManager.GetPageById(page.GetNextPageId())
		if err != nil {
			return nil, err
		}
		newPage = pageValue
	}
	nextID := newPage.GetPageId()
	page.SetNextPageId(nextID)
	return newPage, nil
}

func (this *PageManager) ToDisk(page *structType.Page, GlobalDiskManager *diskMgr.DiskManager) error {
	_, err := GlobalDiskManager.WritePage(page.GetPageId(), page)
	if err != nil {
		return err
	}
	return nil
}

// InsertDataAndToDisk 这里会自动写入页之后写到disk中,需要提供目标页，这里写的是一个完整的页
func (this *PageManager) insertDataAndToDisk(page *structType.Page, value []byte, GlobalDiskManager *diskMgr.DiskManager) error {
	// 如果数据长度大于容量，则调用insertMultipleData进行保存
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
	return nil
}

func (this *PageManager) CreateNextPage(page *structType.Page, diskManager *diskMgr.DiskManager) *structType.Page {
	newPage := this.NewPage(diskManager)
	page.SetNextPageId(newPage.GetPageId())
	newPage.SetDirty(false)
	newPage.SetPinCount(page.GetPinCount())
	return newPage
}
