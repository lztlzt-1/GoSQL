### 基本结构：

* 数据库支持插入数据类型：
  * int
  * float
  * bool
  * string
  * int64(long)
  * float64(double)

* 范围：
* 总共存储1TB的数据，最多170个表

### 功能实现

#### 与磁盘进行交互：

##### diskManager

​	以页为单位对内存中的数据进行存储，现在默认1页4K，提供GetData，WritePage，ReadPage三个接口

##### DiskPageTable

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



#### 内存中管理

##### page：

​	基本存储单位，存储记录，结构体定义如下

```go
type Table struct {
	PageId     msg.PageId // 这个不用存进disk里，表示这个表的起始页位置
	Name       string     // 最多TableNameLength长度
	Length     int        // todo: 可能能利用这个懒读取
	ColumnSize int
	RecordSize int
	Column     []Column
	Records    []structType.Record
	NextPageID msg.PageId // 这个不用存进disk里，表示这个表的下一页
	HeadPageID msg.PageId // 这个不用存进disk里，表示这个表的页所构成的链表的头
}
```

~~使用slotted page作为数据管理方式~~（内存消耗过大，弃用）

NextPageID和HeadPageID是用于读取等操作时不必将所有页读入使用的，每次读取1个页，若没有目标记录再去读取下一个

##### pageTable

​	功能：对页进行管理，使用扩展hash作为存储数据结构，放在内存里面，管理磁盘的页，常驻内存中。存储格式是“记录编号（5B）+内存地址（8B）”，内存地址-1则表示还没有数据，数据库暂时面向TB级数据，即2^40，所以用5B

##### replacer

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

##### bufferPool

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

#### 表管理

##### table 表

