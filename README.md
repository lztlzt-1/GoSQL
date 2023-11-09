#### 与磁盘进行交互：

##### diskManager

​	以页为单位对内存中的数据进行存储，现在默认1页4K，提供GetData，WritePage，ReadPage三个接口

#### 内存中管理

page：

​	基本存储单位，存储记录，结构体定义如下

```go
type Page struct {
	pageId      int
	pinCount    int
	pageHeadPos uint16 //指向头部已存数据最后一个
	pageTailPos uint16 //指向尾部已存数据最前一个
	isDirty     bool
	data        []byte
}
```

使用slotted page作为数据管理方式

pageTable

​	功能：对页进行管理，使用扩展hash作为存储数据结构，放在内存里面，管理磁盘的页，常驻内存中。存储格式是“记录编号（5B）+内存地址（8B）”，内存地址-1则表示还没有数据



buffer

replacer

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
