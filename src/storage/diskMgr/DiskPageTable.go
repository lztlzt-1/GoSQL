package diskMgr

import (
	"GoSQL/src/algorithm/ExtendibleHash"
	"GoSQL/src/msg"
	"GoSQL/src/utils"
	"fmt"
)

type DiskPageTable struct {
	hash ExtendibleHash.ExtendibleHash
}

func (this *DiskPageTable) Query(id msg.PageId) *utils.Pair {
	return this.hash.Query(id)
}

func NewPageTable() DiskPageTable {
	return DiskPageTable{hash: ExtendibleHash.NewExtendibleHash(msg.CapacityBucket)}
}

// InsertTable 向页表中插入一个表格
func (this *DiskPageTable) InsertTable(pageId msg.PageId, frameId msg.FrameId) int {
	return this.hash.Insert(pageId, frameId)
}

func (this *DiskPageTable) UpdateTable(pageId msg.PageId, frameId msg.FrameId) int {
	return this.hash.Update(pageId, frameId)
}

func (this *DiskPageTable) DeleteTable(pageId msg.PageId) int {
	return this.hash.Delete(pageId)
}

func (this *DiskPageTable) ToDisk() error {
	records := this.hash.GetAllBuckets()
	fmt.Println(records)
	return nil
}

//func (this *DiskPageTable) LoadFromDisk(pageId msg.PageId) int {
//
//}
