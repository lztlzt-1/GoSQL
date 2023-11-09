package buffer

import (
	"GoSQL/src/algorithm/ExtendibleHash"
	"GoSQL/src/algorithm/replacer"
	"GoSQL/src/dataTypes"
	"GoSQL/src/msg"
	"GoSQL/src/storage"
	"errors"
	"fmt"
)

type bufferPoolManager struct {
	pages       []storage.Page
	replacer_   replacer.LruKReplacer
	pageTable   storage.PageTable
	diskManager *storage.DiskManager
}

type PageTable struct {
	hash ExtendibleHash.ExtendibleHash
}

func NewBufferPoolManager(bufferSize int) bufferPoolManager {
	pages := make([]storage.Page, 0, bufferSize)
	replacer := replacer.NewLruKReplacer(msg.ReplacerSize(bufferSize), msg.CapacityLruTime)
	table := storage.NewPageTable()
	diskManager, _ := storage.NewDiskManager("test.db")
	return bufferPoolManager{
		pages:       pages,
		replacer_:   replacer,
		pageTable:   table,
		diskManager: diskManager,
	}
}

func (this *bufferPoolManager) InsertPage(page *storage.Page) int {
	if len(this.pages) != cap(this.pages) {
		idx := len(this.pages)
		this.pages = append(this.pages, *page)
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

func (this *bufferPoolManager) swapPage(frameId msg.FrameId, newPage *storage.Page) error {
	oldPage := this.pages[frameId]
	_, err := this.diskManager.WritePage(dataTypes.PageId(oldPage.GetPageId()), &oldPage)
	if err != nil {
		s := fmt.Sprint("can not swap the page {", oldPage.GetPageId(), "}")
		return errors.New(s)
	}
	this.pages[frameId] = *newPage
	this.replacer_.Insert(newPage.GetPageId())
	this.pageTable.DeleteRecord(oldPage.GetPageId())
	this.pageTable.InsertRecord(newPage.GetPageId(), frameId)
	return nil
}
