package ExtendibleHash

import (
	"GoSQL/src/utils"
)

type bucket struct {
	size  uint8
	depth uint8
	list_ []utils.Pair
}

func NewBucket(size uint8, depth uint8) bucket {
	var list []utils.Pair
	bucket_ := bucket{
		size:  size,
		depth: depth,
		list_: list,
	}
	return bucket_
}

func (this *bucket) GetSize() uint8 {
	return uint8(len(this.list_))
}

func (this *bucket) GetTotalSize() uint8 {
	return this.size
}
func (this *bucket) GetDepth() uint8 {
	return this.depth
}

func (this *bucket) IncreaseDepth() {
	this.depth++
}

func (this *bucket) IsFull() bool {
	if this.GetSize() == this.GetTotalSize() {
		return true
	}
	return false
}

func (this *bucket) Insert(key any, value any) int {
	for _, item := range this.list_ {
		if item.First == key {
			item.Second = value
			return 0
		}
	}
	if this.IsFull() {
		return bucketIsFull
	}
	this.list_ = append(this.list_, utils.Pair{First: key, Second: value})
	return success
}

func (this *bucket) Delete(key any) int {
	for index, item := range this.list_ {
		if item.First == key {
			this.list_ = utils.DeleteElement[utils.Pair](this.list_, index)
			return success
		}
	}
	return notFind
}

func (this *bucket) GetItem(key any) any {
	for _, item := range this.list_ {
		if item.First == key {
			return item.Second
		}
	}
	return notFind
}

func (this *bucket) GetItems() any {
	return this.list_
}
