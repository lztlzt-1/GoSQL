package utils

import "GoSQL/src/msg"

var GetNewPageId func() msg.PageId

// NewPageId 获取一个新的pageId
func NewPageId(initState msg.PageId) func() msg.PageId {
	generatePageId := func(state any) any {
		cur := state.(msg.PageId)
		cur = cur + 1
		return cur
	}
	pageGenerator := LazyGenerator(generatePageId, initState)
	return func() msg.PageId {
		initState = pageGenerator().(msg.PageId)
		return initState
	}
}
