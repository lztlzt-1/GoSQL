package replacer

import (
	"GoSQL/src/TimeManager"
	"GoSQL/src/algorithm/Queue"
	"GoSQL/src/msg"
	"log"
)

type frameInfo struct {
	evictable  *bool
	insertTime *Queue.Queue[int]
}

type LruKReplacer struct {
	hash_         map[msg.PageId]frameInfo
	capacity      msg.ReplacerSize
	k             uint8
	evictableSize msg.ReplacerSize
	timeGenerator TimeManager.TimeManager
}

func NewLruKReplacer(capacity msg.ReplacerSize, k uint8) LruKReplacer {
	mp := make(map[msg.PageId]frameInfo)
	for k, _ := range mp {
		*mp[k].evictable = true
	}
	ti := TimeManager.NewTimeManager()
	return LruKReplacer{
		hash_:         mp,
		capacity:      capacity,
		k:             k,
		evictableSize: msg.ReplacerSize(0),
		timeGenerator: ti,
	}
}

func (this *LruKReplacer) GetEvictFlag(pageID msg.PageId) bool {
	return *this.hash_[pageID].evictable
}

func (this *LruKReplacer) SetEvictFlag(pageID msg.PageId, flag bool) {
	*this.hash_[pageID].evictable = flag
}

func (this *LruKReplacer) getSize() msg.ReplacerSize {
	return msg.ReplacerSize(len(this.hash_))
}

func (this *frameInfo) getSize() msg.ReplacerSize {
	return msg.ReplacerSize(this.insertTime.GetLength())
}

func (this *LruKReplacer) Insert(id msg.PageId) int {
	if _, ok := this.hash_[id]; !ok {
		if this.getSize() == this.capacity {
			//淘汰，并插入
			var id msg.PageId = -1
			state := this.Evict(&id)
			if state != msg.Success {
				return msg.CannotBeEvict
			}
		}
		if this.getSize() == this.capacity {
			return msg.NotFoundEvictable
		}
		queue := Queue.NewQueue[int]()
		nowTime := this.timeGenerator.GetNewTime()
		queue.Push(nowTime)
		evict := true
		this.evictableSize++
		f := frameInfo{evictable: &evict, insertTime: &queue}
		this.hash_[id] = f
		return msg.Success
	}
	if this.hash_[id].insertTime.GetLength() == int(this.k) {
		this.hash_[id].insertTime.Pop()
	}
	this.hash_[id].insertTime.Push(this.timeGenerator.GetNewTime())
	return msg.Success
}

func isLess(d1 frameInfo, d2 frameInfo, k uint8) bool {
	if d1.getSize() == msg.ReplacerSize(k) && d2.getSize() < msg.ReplacerSize(k) {
		return false
	}
	if d1.getSize() < msg.ReplacerSize(k) && d2.getSize() == msg.ReplacerSize(k) {
		return true
	}
	return d1.insertTime.GetData()[0] < d2.insertTime.GetData()[0]
}

func (this *LruKReplacer) Evict(id *msg.PageId) int {
	p := msg.PageId(-1)
	for key, value := range this.hash_ {
		if (p == -1 || (isLess(value, this.hash_[p], this.k))) && *value.evictable == true {
			p = key
		}
	}
	if p != msg.PageId(-1) {
		*id = p
		delete(this.hash_, p)
		this.evictableSize--
		return msg.Success
	}
	return msg.NotFoundEvictable
}

func (this *LruKReplacer) SetEvict(id msg.PageId, flag bool) int {
	if _, ok := this.hash_[id]; !ok {
		return msg.NotFound
	}
	if *this.hash_[id].evictable == true && flag == false {
		this.evictableSize--
	}
	if *this.hash_[id].evictable == false && flag == true {
		this.evictableSize++
	}
	*this.hash_[id].evictable = flag
	return msg.Success
}

func (this *LruKReplacer) Remove(id msg.PageId) int {
	if _, ok := this.hash_[id]; !ok {
		return msg.NotFound
	}
	if *this.hash_[id].evictable == true {
		log.Printf("warning: evict a page that not excepted to be evicted")
	}
	delete(this.hash_, id)
	return msg.Success
}
