package diskMgr

import (
	"GoSQL/src/algorithm/ExtendibleHash"
	"GoSQL/src/msg"
	"fmt"
)

type DiskPageTable struct {
	hash ExtendibleHash.ExtendibleHash
}

func NewDiskPageTable(size uint8) DiskPageTable {
	return DiskPageTable{
		hash: ExtendibleHash.NewExtendibleHash(size),
	}
}

func (this *DiskPageTable) Query(name string) msg.PageId {
	value := this.hash.Query(name)
	if value == nil {
		return -1
	}
	return value.Second.(msg.PageId)
}

func NewPageTable() DiskPageTable {
	return DiskPageTable{hash: ExtendibleHash.NewExtendibleHash(msg.PageTableCapacityBucket)}
}

// InsertTable 向页表中插入一个表格
func (this *DiskPageTable) InsertTable(pageName string, pageID msg.PageId) int {
	return this.hash.Insert(pageName, pageID)
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
