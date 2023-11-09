package storage

import (
	"GoSQL/src/algorithm/ExtendibleHash"
	"GoSQL/src/msg"
	"GoSQL/src/utils"
)

type PageTable struct {
	hash ExtendibleHash.ExtendibleHash
}

func (this *PageTable) Query(id msg.FrameId) *utils.Pair {
	return this.hash.Query(id)
}

func NewPageTable() PageTable {
	return PageTable{hash: ExtendibleHash.NewExtendibleHash(msg.CapacityBucket)}
}

// InsertRecord 向页表中插入一个记录（frameId,pageId)
func (this *PageTable) InsertRecord(frameId msg.FrameId, pageId msg.PageId) int {
	return this.hash.Insert(frameId, pageId)
}

func (this *PageTable) UpdateRecord(frameId msg.FrameId, pageId msg.PageId) int {
	return this.hash.Update(frameId, pageId)
}

func (this *PageTable) WriteData(data []byte) {

}

func (this *PageTable) GetEmptyPos() int {

	return 1
}
