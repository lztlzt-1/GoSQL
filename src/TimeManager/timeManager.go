package TimeManager

import (
	"GoSQL/src/utils"
)

type TimeManager struct {
	GetNewTime func() int
}

func NewTimeManager() TimeManager {
	this := TimeManager{
		GetNewTime: newTimeId(),
	}
	return this
}

// NewTimeId 获取一个新的pageId
func newTimeId() func() int {
	initState := 0
	generatePageId := func(state any) any {
		cur := state.(int)
		cur = cur + 1
		return cur
	}
	pageGenerator := utils.LazyGenerator(generatePageId, initState)
	return func() int {
		initState = pageGenerator().(int)
		return initState
	}
}

// NewTime 生成一个新页,返回指针
func (this *TimeManager) NewTime() int {
	return this.GetNewTime()
}
