package ExtendibleHash

import (
	"GoSQL/src/utils"
)

type extendibleHash struct {
	buckets     []*bucket
	globalDepth uint8
	bucketSize  uint8
}

func NewExtendibleHash(size uint8) extendibleHash {
	buckets := make([]*bucket, 0)
	bucket1 := NewBucket(size, 0)
	buckets = append(buckets, &bucket1)
	return extendibleHash{
		buckets:     buckets,
		globalDepth: 0,
		bucketSize:  size,
	}
}

func (this *extendibleHash) GetGlobalDepth() uint8 {
	return this.globalDepth
}

func (this *extendibleHash) GetLocalDepth(index int) uint8 {
	return this.buckets[index].GetDepth()
}

func (this *extendibleHash) GetBucketNum() int {
	return len(this.buckets)
}

func (this *extendibleHash) redistribute(bucket_ *bucket) {
	originMask := 1<<bucket_.GetDepth() - 1
	originKey := utils.GetHashValueSHA256ToInt(bucket_.list_[0].First) & originMask
	bucket_.IncreaseDepth()
	depth := bucket_.GetDepth()
	newBucket0 := NewBucket(this.bucketSize, depth)
	newBucket1 := NewBucket(this.bucketSize, depth)
	for _, pointer := range bucket_.list_ {
		key := utils.GetHashValueSHA256ToInt(pointer.First)
		if (key>>(depth-1))&1 == 0 {
			newBucket0.Insert(pointer.First, pointer.Second)
		} else {
			newBucket1.Insert(pointer.First, pointer.Second)
		}
	}

	mask := 1<<bucket_.GetDepth() - 1
	for i := 0; i < this.GetBucketNum(); i++ {
		if i&originMask == originKey {
			if i&mask == originKey {
				this.buckets[i] = &newBucket0
			} else {
				this.buckets[i] = &newBucket1
			}
		}
	}
}

func (this *extendibleHash) Insert(key any, value any) int {

	for {
		index := this.indexOf(key)
		if !this.buckets[index].IsFull() {
			this.buckets[index].Insert(key, value)
			return success
		}
		if this.GetGlobalDepth() != this.GetLocalDepth(index) {
			this.redistribute(this.buckets[index])
		} else {
			this.globalDepth++
			sz := this.GetBucketNum()
			for i := 0; i < sz; i++ {
				this.buckets = append(this.buckets, this.buckets[i])
			}
		}
	}
}

func (this *extendibleHash) indexOf(key any) int {
	hash := utils.GetHashValueSHA256ToInt(key)
	mask := 1<<this.GetGlobalDepth() - 1
	return hash & mask
}
