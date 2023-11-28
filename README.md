# 基本结构：

* ## 数据库支持插入数据类型：
  
  * int
  * float
  * bool
  * string
  * int64(long)
  * float64(double)
* 范围：
* 面向总共存储1TB的数据，可扩展，但直接增加到TB以上可能会有问题
* 每个记录至少6B，方便进行内存回收。

# 功能实现

## 与磁盘进行交互：

### diskManager

​	以页为单位对内存中的数据进行存储，现在默认1页4K，提供GetData，WritePage，ReadPage三个接口

### DiskPageTable

```go
type DiskPageTable struct {
	hash ExtendibleHash.ExtendibleHash
}
```

对于每一个表在磁盘的位置进行存储，需要进行持久化

```go
func (this *DiskPageTable) ToDisk(pageId msg.PageId) int {
	直接将所有记录写进disk，额外存储下bucketSize
}
```

```go
func (this *DiskPageTable) LoadFromDisk(pageId msg.PageId) int {
	对于每一个记录直接进行insert，由于表格比起记录会少得多，所以这样写对效率没有特别大影响
}
```



## 内存中管理

### page

​	基本存储单位，存储记录，结构体定义如下

```go
type Page struct {
	pageId      msg.PageId // 这个也可以用作判断页有效性
	nextPageID  msg.PageId
	freeSpace   msg.FreeSpaceTypeInTable
	pinCount    int
	pageHeadPos int16 //指向头部已存数据最后一个
	pageTailPos int16 //指向尾部已存数据最前一个
	isDirty     bool
	data        []byte
}
```

~~使用slotted page作为数据管理方式~~（内存消耗过大，弃用）

~~NextPageID和HeadPageID是用于读取等操作时不必将所有页读入使用的，每次读取1个页，若没有目标记录再去读取下一个~~

freeSpace指向第一个空闲槽位（不一定在页表中是第一个空闲区域）

当执行插入等操作时，由两种情况

* freeSpace有值，那么直接使用这个值，它就是下一个空闲槽位，freeSpace修改成这个地址，并使用这个槽位
* freeSpace没有值，说明这个槽位本身就没有被使用过，使用这个槽位，并将freeSpace向后移动+recordsize

### pageTable

​	功能：对页进行管理，使用扩展hash作为存储数据结构，放在内存里面，管理磁盘的页，常驻内存中。存储格式是“记录编号（5B）+内存地址（8B）”，内存地址-1则表示还没有数据，数据库暂时面向TB级数据，即2^40，所以用5B

### replacer

​	进行置换算法，结构体定义如下

```go
type Lru_K_Replacer struct {
	hash_    map[frameId]frameInfo
	capacity replacerSize
	k        uint8
	size     replacerSize
    timeGenerator TimeManager.TimeManager
}
```



疑问：go里面没有静态变量，这样会让编码有一定复杂度，比如timeGenerator用于生成时间序列，没有静态变量所以需要在结构体里加个类，可能会让开销增大？

### bufferPool

```go
type bufferPoolManager struct {
	pages       []storage.Page
	replacer_   replacer.LruKReplacer
	pageTable   storage.PageTable
	diskManager *storage.DiskManager
}
```

* replacer负责调度淘汰策略，通过pageId进行
* pageTable负责将页表翻译成bufferPool对应位置

## 表管理

### table 表

```go
type Column struct {
	Name    string
	ItsType string
}

type Table struct {
	PageId     msg.PageId // 这个不用存进disk里，表示这个表的起始页位置
	Name       string     // 最多TableNameLength长度
	Length     int        // todo: 可能能利用这个懒读取
	ColumnSize int
	RecordSize int
	FreeSpace  msg.FreeSpaceTypeInTable
	Column     []Column
	Records    []structType.Record
	NextPageID msg.PageId // 这个不用存进disk里，页的头里面包含了，表示这个表下一页
	HeadPageID msg.PageId // 这个不用存进disk里，表示这个表的页所构成的链表的头
	//StartPageID msg.PageId // 这个不用存进disk里，表示这个表的页所构成的链表的头
}
```

#### insert/create表

* 正常情况

  * 在创建表的同时直接将表写入内存
  * 在插入数据判断时判断到已经满一页则进行插入页

* 当一条记录的长度已经超过1页（操作时应当尽量避免设计出这种表）

  * 整个记录变成一个页码，指向overflow page，overflow page有页头，数据单纯的流数据

使用dirtyPageList管理，当换页则写入之前脏页，每次insert则将当前页放入脏页

为了节省内存对于每个table只保留curpage的记录，当换页则删除之前的记录，只保留cruPage里的record

####   query表：

记录当前id为startID，从curPage开始向后寻找所有记录，找到nextID=-1结束，返回第一个有记录的表中，根据偏移量找到记录开始位置，一直寻找到startID页

#### update表

按照query的方法查找到最终记录，但最终记录是三元组Triplet（record修改后的数值，页码，偏移量）

#### delete表

1. 判断本页的freeSpace是否有值
   * 无值：freeSpace指向该偏移，这个记录的空间置为-1
   * 有值：这个空闲地址记录freeSpace信息，freeSpace指向这个空间

## 缓冲管理

```go
type BufferPoolManager struct {
	pages     []*structType.Page
	replacer_ replacer.LruKReplacer
	pageTable BufferPageTable
}
```

缓冲替换策略是在table读入新的一页则将它在缓冲区访问次数+1

<u>***在引入缓存后，所有上层对页的操作均由缓存实现接口***</u>

使用缓存时不要先存下之前的页，因为可能淘汰后重新读入导致地址不同无法同步，正确的操作是记下页码
