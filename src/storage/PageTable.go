package storage

import (
	"GoSQL/src/algorithm/ExtendibleHash"
	"GoSQL/src/msg"
	"GoSQL/src/utils"
)

type PageTable struct {
	hash ExtendibleHash.ExtendibleHash
}

func (this *PageTable) Query(id msg.PageId) *utils.Pair {
	return this.hash.Query(id)
}

func NewPageTable() PageTable {
	return PageTable{hash: ExtendibleHash.NewExtendibleHash(msg.CapacityBucket)}
}

// InsertRecord 向页表中插入一个记录（frameId,pageId)
func (this *PageTable) InsertRecord(pageId msg.PageId, frameId msg.FrameId) int {
	return this.hash.Insert(pageId, frameId)
}

func (this *PageTable) UpdateRecord(pageId msg.PageId, frameId msg.FrameId) int {
	return this.hash.Update(pageId, frameId)
}

func (this *PageTable) DeleteRecord(pageId msg.PageId) int {
	return this.hash.Delete(pageId)
}

func (this *PageTable) WriteData(data []byte) {

}

func (this *PageTable) GetEmptyPos() int {

	return 1
}
