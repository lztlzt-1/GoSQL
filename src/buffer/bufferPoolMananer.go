package buffer

import (
	"GoSQL/src/algorithm/replacer"
	"GoSQL/src/msg"
	"GoSQL/src/storage/diskMgr"
	"GoSQL/src/structType"
	"GoSQL/src/utils"
	"errors"
	"fmt"
)

type BufferPoolManager struct {
	pages       []*structType.Page
	replacer_   replacer.LruKReplacer
	pageTable   BufferPageTable
	diskManager *diskMgr.DiskManager
}

func NewBufferPoolManager(bufferSize int, diskManager *diskMgr.DiskManager) *BufferPoolManager {
	pages := make([]*structType.Page, 0, bufferSize)
	replacer := replacer.NewLruKReplacer(msg.ReplacerSize(bufferSize), msg.BufferCapacityLruTime)
	table := NewPageTable()
	bufferPoolManager := BufferPoolManager{
		pages:       pages,
		replacer_:   replacer,
		pageTable:   table,
		diskManager: diskManager,
	}
	return &bufferPoolManager
}

// InsertPage 在缓冲中插入页，如果缓冲满了则会和磁盘交换
func (this *BufferPoolManager) insertPage(page *structType.Page) int {
	if this.pageTable.Query(page.GetPageId()) != nil {
		//如果已经在缓冲区存在就加一次计数
		this.replacer_.Insert(page.GetPageId())
		return msg.Success
	}
	if len(this.pages) != cap(this.pages) {
		idx := len(this.pages)
		this.pages = append(this.pages, page)
		this.pageTable.InsertRecord(page.GetPageId(), msg.FrameId(idx))
		this.replacer_.Insert(page.GetPageId())
		return msg.Success
	}
	var id msg.PageId
	if this.replacer_.Evict(&id) == msg.Success {
		pair := this.pageTable.Query(id)
		err := this.swapPage(pair.Second.(msg.FrameId), page)
		if err != nil {
			return msg.NotFoundEvictable
		}
	}
	return msg.Success
}

// Pin 这里需要原子操作
func (this *BufferPoolManager) pin(page *structType.Page) {
	this.insertPage(page)
	page.Pin()
	this.replacer_.SetEvictFlag(page.GetPageId(), false)
}

func (this *BufferPoolManager) unPin(page *structType.Page) {
	pinNum := page.UnPin()
	if pinNum < 0 {
		pinNum = 0
		page.SetPinCount(pinNum)
	}
	if pinNum == 0 {
		this.replacer_.SetEvictFlag(page.GetPageId(), true)
	}
}

func (this *BufferPoolManager) queryPageByID(pageID msg.PageId) *structType.Page {
	pair := this.pageTable.Query(pageID)
	if pair == nil {
		return nil
	}
	this.replacer_.Insert(pageID)
	frameID := pair.Second.(msg.FrameId)
	return this.pages[frameID]
}

func (this *BufferPoolManager) QueryPage(pageID msg.PageId) *structType.Page {
	page := this.queryPageByID(pageID)
	if page == nil {
		var err error
		page, err = this.diskManager.GetPageById(pageID)
		this.insertPage(page)
		if err != nil {
			return nil
		}
	}
	return page
}

func (this *BufferPoolManager) GetPageById(pageID msg.PageId) (*structType.Page, error) {
	//在缓存中找
	for i := 0; i < len(this.pages); i++ {
		if this.pages[i].GetPageId() == pageID {
			page := this.pages[i]
			this.replacer_.Insert(pageID)
			return page, nil
		}
	}
	//从磁盘中找
	page, err := this.diskManager.GetPageById(pageID)
	if err != nil {
		return nil, err
	}
	//替换,查找时不会修改数据，所以dirty是false
	this.insertPage(page)
	return page, nil
}

// RefreshAll 将缓冲区中所有脏页记录保存到磁盘,不会刷新缓存
func (this *BufferPoolManager) RefreshAll() error {
	pairs := this.pageTable.hash.GetAllBuckets()
	for _, item := range pairs {
		frameId := item.Second.(msg.FrameId)
		pageID := item.First.(msg.PageId)
		if this.pages[frameId].IsDirty() {
			_, err := this.diskManager.WritePage(pageID, this.pages[frameId])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// 将一个页写入disk,将另一个页写入buffer，这里swap对应frameID
func (this *BufferPoolManager) swapPage(frameId msg.FrameId, newPage *structType.Page) error {
	oldPage := this.pages[frameId]
	_, err := this.diskManager.WritePage(oldPage.GetPageId(), oldPage)
	if err != nil {
		s := fmt.Sprint("can not swap the page {", oldPage.GetPageId(), "}")
		return errors.New(s)
	}
	this.pages[frameId] = newPage
	this.replacer_.Insert(newPage.GetPageId())
	this.pageTable.DeleteRecord(oldPage.GetPageId())
	this.pageTable.InsertRecord(newPage.GetPageId(), frameId)
	return nil
}

func (this *BufferPoolManager) dumpPage(frameId msg.FrameId) error {
	oldPage := this.pages[frameId]
	_, err := this.diskManager.WritePage(oldPage.GetPageId(), oldPage)
	if err != nil {
		s := fmt.Sprint("can not swap the page {", oldPage.GetPageId(), "}")
		return errors.New(s)
	}
	this.pages[frameId] = nil
	this.replacer_.Remove(oldPage.GetPageId())
	this.pageTable.DeleteRecord(oldPage.GetPageId())
	return nil
}

//////////////////////////////////////////////////////////////////////////////////////////
///////////////////////////////       对页的操作都有缓存实现接口      ////////////////////////

func (this *BufferPoolManager) SetNextPageID(page *structType.Page, pageID msg.PageId) {
	this.pin(page)
	page.SetNextPageId(pageID)
	this.unPin(page)
}

func (this *BufferPoolManager) GetNextPageID(page *structType.Page) msg.PageId {
	this.insertPage(page)
	pageID := page.GetNextPageId()
	return pageID
}

func (this *BufferPoolManager) GetFreeSpace(page *structType.Page) msg.FreeSpaceTypeInTable {
	this.insertPage(page)
	return page.GetFreeSpace()
}

func (this *BufferPoolManager) SetFreeSpace(page *structType.Page, values msg.FreeSpaceTypeInTable) {
	this.pin(page)
	page.SetFreeSpace(values)
	this.unPin(page)
}

func (this *BufferPoolManager) GetData(page *structType.Page) []byte {
	this.insertPage(page)
	return page.GetData()
}

func (this *BufferPoolManager) SetData(page *structType.Page, values []byte) {
	this.pin(page)
	page.SetData(values)
	this.unPin(page)
}

func (this *BufferPoolManager) GetPageId(page *structType.Page) msg.PageId {
	this.insertPage(page)
	return page.GetPageId()
}

func (this *BufferPoolManager) SetPageId(page *structType.Page, id msg.PageId) {
	this.pin(page)
	page.SetPageId(id)
	this.unPin(page)
}

func (this *BufferPoolManager) GetPinCount(page *structType.Page) int {
	this.insertPage(page)
	return page.GetPinCount()
}

func (this *BufferPoolManager) SetPinCount(page *structType.Page, pinCount int) {
	this.pin(page)
	page.SetPinCount(pinCount)
	this.unPin(page)
}

func (this *BufferPoolManager) IsDirty(page *structType.Page) bool {
	//this.InsertPage(page)
	return page.IsDirty()
}

func (this *BufferPoolManager) SetDirty(page *structType.Page, isDirty bool) {
	page.SetDirty(isDirty)
}

// InsertDataToFreeSpace 在这里查找空余位置并判断
func (this *BufferPoolManager) InsertDataToFreeSpace(page *structType.Page, bytes []byte) (int, error) {
	index := this.GetFreeSpace(page)
	for int(index)+len(bytes) >= msg.PageRemainSize {
		//说明这页没有空余空间
		nextPageID := this.GetNextPageID(page)
		var err error
		page, err = this.GetPageById(nextPageID)
		if err != nil {
			return 0, err
		}
		index = this.GetFreeSpace(page)
	}
	data := this.GetData(page)
	nextFreeSpace := utils.Bytes2Int16(data[index+1 : index+3]) // 前面有1Bflag
	if nextFreeSpace != 0 {
		this.SetFreeSpace(page, msg.FreeSpaceTypeInTable(nextFreeSpace))
	} else {
		this.SetFreeSpace(page, msg.FreeSpaceTypeInTable(int(index)+len(bytes)))
	}
	var err error
	data, err = utils.InsertAndReplaceAtIndex(data, int(index), bytes)
	if err != nil {
		return 0, err
	}
	this.SetData(page, data)
	return msg.Success, nil
}
