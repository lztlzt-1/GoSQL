package buffer

import (
	"GoSQL/src/algorithm/ExtendibleHash"
	"GoSQL/src/msg"
	"GoSQL/src/utils"
)

type BufferPageTable struct {
	hash ExtendibleHash.ExtendibleHash
}

func (this *BufferPageTable) Query(id msg.PageId) *utils.Pair {
	return this.hash.Query(id)
}

func NewPageTable() BufferPageTable {
	return BufferPageTable{hash: ExtendibleHash.NewExtendibleHash(msg.CapacityBucket)}
}

// InsertRecord 向页表中插入一个记录（frameId,pageId),用在buffer里面的
func (this *BufferPageTable) InsertRecord(pageId msg.PageId, frameId msg.FrameId) int {
	return this.hash.Insert(pageId, frameId)
}

func (this *BufferPageTable) UpdateRecord(pageId msg.PageId, frameId msg.FrameId) int {
	return this.hash.Update(pageId, frameId)
}

func (this *BufferPageTable) DeleteRecord(pageId msg.PageId) int {
	return this.hash.Delete(pageId)
}
