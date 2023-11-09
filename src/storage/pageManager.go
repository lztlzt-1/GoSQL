package storage

import (
	"GoSQL/src/dataTypes"
	"GoSQL/src/msg"
	"GoSQL/src/utils"
)

type pageManager struct {
	GetNewPageId func() msg.PageId
}

func NewPageManager() pageManager {
	this := pageManager{
		GetNewPageId: NewPageId(),
	}
	return this
}

// NewPageId 获取一个新的pageId
func NewPageId() func() msg.PageId {
	initState := msg.PageId(0)
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
func (this *pageManager) NewPage() *Page {
	pageId := this.GetNewPageId()
	page := new(Page)
	page.pageId = pageId
	page.pinCount = 0
	page.isDirty = false
	page.pageTailPos = dataTypes.PageRemainSize - 1
	page.pageHeadPos = 0
	//page.pageSize = 0
	page.data = make([]byte, dataTypes.PageRemainSize)
	return page
}
