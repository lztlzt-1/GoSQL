package buffer

import (
	"GoSQL/src/algorithm/replacer"
	"GoSQL/src/msg"
	"GoSQL/src/storage/DiskManager"
	"GoSQL/src/storage/PageManager"
	"errors"
	"fmt"
)

type BufferPoolManager struct {
	pages     []PageManager.Page
	replacer_ replacer.LruKReplacer
	pageTable BufferPageTable
}

var GlobalBufferPoolManager BufferPoolManager

func NewBufferPoolManager(bufferSize int) error {
	pages := make([]PageManager.Page, 0, bufferSize)
	replacer := replacer.NewLruKReplacer(msg.ReplacerSize(bufferSize), msg.CapacityLruTime)
	table := NewPageTable()
	GlobalBufferPoolManager = BufferPoolManager{
		pages:     pages,
		replacer_: replacer,
		pageTable: table,
	}
	return nil
}

func (this *BufferPoolManager) InsertPage(page *PageManager.Page) int {
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

func (this *BufferPoolManager) swapPage(frameId msg.FrameId, newPage *PageManager.Page) error {
	oldPage := this.pages[frameId]
	_, err := DiskManager.GlobalDiskManager.WritePage(msg.PageId(oldPage.GetPageId()), &oldPage)
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
