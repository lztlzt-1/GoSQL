package storage

import (
	"GoSQL/src/msg"
	"GoSQL/src/utils"
)

type PageManager struct {
	GetNewPageId func() msg.PageId
	initPage     *InitPage
}

func NewPageManager(initState msg.PageId, page *InitPage) PageManager {
	this := PageManager{
		GetNewPageId: NewPageId(initState),
		initPage:     page,
	}

	return this
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
func (this *PageManager) NewPage() *Page {
	pageId := this.GetNewPageId()
	page := new(Page)
	page.pageId = pageId
	page.pinCount = 0
	page.isDirty = false
	page.pageTailPos = msg.PageRemainSize - 1
	page.pageHeadPos = 0
	this.initPage.SetInitPageID(pageId)
	//page.pageSize = 0
	page.data = make([]byte, msg.PageRemainSize)
	return page
}

func (this *PageManager) NewPageWithID(id msg.PageId) *Page {
	pageId := id
	page := new(Page)
	page.pageId = pageId
	page.pinCount = 0
	page.isDirty = false
	page.pageTailPos = msg.PageRemainSize - 1
	page.pageHeadPos = 0
	this.initPage.SetInitPageID(pageId)
	//page.pageSize = 0
	page.data = make([]byte, msg.PageRemainSize)
	return page
}
