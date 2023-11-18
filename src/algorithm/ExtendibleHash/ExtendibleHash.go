package ExtendibleHash

import (
	"GoSQL/src/msg"
	"GoSQL/src/utils"
)

type ExtendibleHash struct {
	buckets     []*bucket
	globalDepth uint8
	bucketSize  uint8 // 指bucket的最大容量
}

func NewExtendibleHash(size uint8) ExtendibleHash {
	buckets := make([]*bucket, 0)
	bucket1 := NewBucket(size, 0)
	buckets = append(buckets, &bucket1)
	return ExtendibleHash{
		buckets:     buckets,
		globalDepth: 0,
		bucketSize:  size,
	}
}

func (this *ExtendibleHash) GetAllBuckets() []utils.Pair {
	var records []utils.Pair
	visited := make(map[*bucket]bool)
	for _, bucketAddr := range this.buckets {
		if _, ok := visited[bucketAddr]; !ok {
			records = append(records, bucketAddr.GetAllItems()...)
		}
		visited[bucketAddr] = true
	}
	return records
}

func (this *ExtendibleHash) GetGlobalDepth() uint8 {
	return this.globalDepth
}

func (this *ExtendibleHash) GetLocalDepth(index int) uint8 {
	return this.buckets[index].GetDepth()
}

func (this *ExtendibleHash) GetBucketNum() int {
	return len(this.buckets)
}

func (this *ExtendibleHash) redistribute(bucket_ *bucket) {
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

func (this *ExtendibleHash) Insert(key any, value any) int {
	for {
		index := this.indexOf(key)
		if !this.buckets[index].IsFull() {
			this.buckets[index].Insert(key, value)
			return msg.Success
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

func (this *ExtendibleHash) indexOf(key any) int {
	hash := utils.GetHashValueSHA256ToInt(key)
	mask := 1<<this.GetGlobalDepth() - 1
	return hash & mask
}

// Query hash中查询键值key的pair
func (this *ExtendibleHash) Query(key any) *utils.Pair {
	idx := this.indexOf(key)
	bucket1 := this.buckets[idx]
	return bucket1.Query(key)
}

func (this *ExtendibleHash) Delete(key any) int {
	idx := this.indexOf(utils.GetHashValueSHA256ToInt(key))
	bucket1 := this.buckets[idx]
	if bucket1.Delete(key) == msg.Success {
		return msg.Success
	}
	return msg.NotFound
}

func (this *ExtendibleHash) Update(key any, value any) int {
	idx := this.indexOf(utils.GetHashValueSHA256ToInt(key))
	bucket1 := this.buckets[idx]
	if bucket1.Update(key, value) == msg.Success {
		return msg.Success
	}
	return msg.NotFound
}
