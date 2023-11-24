package buffer

import (
	"GoSQL/src/algorithm/replacer"
	"GoSQL/src/msg"
	"GoSQL/src/storage/diskMgr"
	"GoSQL/src/structType"
	"errors"
	"fmt"
)

type BufferPoolManager struct {
	pages     []*structType.Page
	replacer_ replacer.LruKReplacer
	pageTable BufferPageTable
}

func NewBufferPoolManager(bufferSize int) *BufferPoolManager {
	pages := make([]*structType.Page, 0, bufferSize)
	replacer := replacer.NewLruKReplacer(msg.ReplacerSize(bufferSize), msg.BufferCapacityLruTime)
	table := NewPageTable()
	bufferPoolManager := BufferPoolManager{
		pages:     pages,
		replacer_: replacer,
		pageTable: table,
	}
	return &bufferPoolManager
}

// InsertPage 在缓冲中插入页，如果缓冲满了则会和磁盘交换
func (this *BufferPoolManager) InsertPage(page *structType.Page, diskManager *diskMgr.DiskManager) int {
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
		err := this.swapPage(pair.Second.(msg.FrameId), page, diskManager)
		if err != nil {
			return msg.NotFoundEvictable
		}
	}
	return msg.Success
}

// Pin 这里需要原子操作
func (this *BufferPoolManager) Pin(page *structType.Page) {
	page.Pin()
	this.replacer_.SetEvictFlag(page.GetPageId(), false)
}

func (this *BufferPoolManager) UnPin(page *structType.Page) {
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

func (this *BufferPoolManager) QueryPage(pageID msg.PageId, diskManager *diskMgr.DiskManager) *structType.Page {
	page := this.queryPageByID(pageID)
	if page == nil {
		var err error
		page, err = diskManager.GetPageById(pageID)
		this.InsertPage(page, diskManager)
		if err != nil {
			return nil
		}
	}
	return page
}

func (this *BufferPoolManager) GetPageById(pageID msg.PageId, diskManager *diskMgr.DiskManager) (*structType.Page, error) {
	//在缓存中找
	for i := 0; i < len(this.pages); i++ {
		if this.pages[i].GetPageId() == pageID {
			page := this.pages[i]
			this.replacer_.Insert(pageID)
			return page, nil
		}
	}
	//从磁盘中找
	page, err := diskManager.GetPageById(pageID)
	if err != nil {
		return nil, err
	}
	//替换,查找时不会修改数据，所以dirty是false
	this.InsertPage(page, diskManager)
	return page, nil
}

// RefreshAll 将缓冲区中所有脏页记录保存到磁盘,不会刷新缓存
func (this *BufferPoolManager) RefreshAll(diskManager *diskMgr.DiskManager) error {
	pairs := this.pageTable.hash.GetAllBuckets()
	for _, item := range pairs {
		frameId := item.Second.(msg.FrameId)
		pageID := item.First.(msg.PageId)
		if this.pages[frameId].IsDirty() {
			_, err := diskManager.WritePage(pageID, this.pages[frameId])
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// 将一个页写入disk,将另一个页写入buffer，这里swap对应frameID
func (this *BufferPoolManager) swapPage(frameId msg.FrameId, newPage *structType.Page, GlobalDiskManager *diskMgr.DiskManager) error {
	oldPage := this.pages[frameId]
	_, err := GlobalDiskManager.WritePage(oldPage.GetPageId(), oldPage)
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

func (this *BufferPoolManager) dumpPage(frameId msg.FrameId, diskManager *diskMgr.DiskManager) error {
	oldPage := this.pages[frameId]
	_, err := diskManager.WritePage(oldPage.GetPageId(), oldPage)
	if err != nil {
		s := fmt.Sprint("can not swap the page {", oldPage.GetPageId(), "}")
		return errors.New(s)
	}
	this.pages[frameId] = nil
	this.replacer_.Remove(oldPage.GetPageId())
	this.pageTable.DeleteRecord(oldPage.GetPageId())
	return nil
}
