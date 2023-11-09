package buffer

import (
	"GoSQL/src/algorithm/ExtendibleHash"
	"GoSQL/src/dataTypes"
	"GoSQL/src/msg"
	"GoSQL/src/storage"
	"errors"
	"fmt"
)

type bufferPoolManager struct {
	pages       []storage.Page
	replacer_   ExtendibleHash.LruKReplacer
	pageTable   storage.PageTable
	diskManager *storage.DiskManager
}

type PageTable struct {
	hash ExtendibleHash.ExtendibleHash
}

func NewBufferPoolManager(bufferSize int) bufferPoolManager {
	pages := make([]storage.Page, 0, bufferSize)
	replacer := ExtendibleHash.NewLruKReplacer(msg.ReplacerSize(bufferSize), msg.CapacityLruTime)
	table := storage.NewPageTable()
	diskManager, _ := storage.NewDiskManager("test.db")
	return bufferPoolManager{
		pages:       pages,
		replacer_:   replacer,
		pageTable:   table,
		diskManager: diskManager,
	}
}

func (this *bufferPoolManager) Insert(page *storage.Page) int {
	if len(this.pages) != cap(this.pages) {
		idx := len(this.pages)
		this.pages = append(this.pages, *page)
		this.pageTable.InsertRecord(msg.FrameId(idx), page.GetPageId())
		this.replacer_.Insert(msg.FrameId(idx))
		return msg.Success
	}
	var id msg.FrameId
	if this.replacer_.Evict(&id) == msg.Success {
		pair := this.pageTable.Query(id)
		err := this.swapPage(pair.First.(msg.FrameId), page)
		if err != nil {
			return msg.NotFoundEvictable
		}
	}
	return msg.Success
}

func (this *bufferPoolManager) swapPage(frameId msg.FrameId, newPage *storage.Page) error {
	oldPage := this.pages[frameId]
	_, err := this.diskManager.WritePage(dataTypes.PageId(oldPage.GetPageId()), &oldPage)
	if err != nil {
		s := fmt.Sprint("can not swap the page {", oldPage.GetPageId(), "}")
		return errors.New(s)
	}
	this.pages[frameId] = *newPage
	this.replacer_.Insert(frameId)
	this.pageTable.UpdateRecord(frameId, newPage.GetPageId())
	return nil
}
