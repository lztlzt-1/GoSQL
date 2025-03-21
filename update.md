# 更新日志

## 11.15

* 在存在表的情况下LoadTableByName会EOF

  * 修改pageHeadSize解决
* 向磁盘插入旧表会重新开辟空间

  * 修改持久化算法解决
* 向磁盘插入旧表会重新开辟空间
  * 检查算法没写好，通过检查重复表可避免这个情况
* *** 在数据库异常终止状态下可能会将磁盘所有数据丢失
* loadTable只能load一页
  * 对于一个table增加一个HasNext，当没有查到指定数据时进行继续查找
  * 第一次new一个table就直接写入大量数据，会出现内存被占满的情况，所以策略是RecordSize到达一定数量时直接使用写操作写到磁盘或缓存，之后就和直接从磁盘里读取数据一样
  * 从磁盘读数据时，每次只会遍历一个页，通过NextPageID读取下一页，当RecordSize到达一定数量时则写操作写到磁盘或缓存，读到NextPageID=-1则重新回到HeadPageID，不做成环状链表是因为环状链表无法确定是不是已经遍历完所有页了，并且维护起来要么需要单向链表变成双向链表，要么需要O(n)的遍历寻找上一节点，开销过大。
  * ~~由于读取操作开销过大，所以增加一个StartPageID，避免重复读取某个页~~。可以直接作为参数传入方法，不必占用结构体内存
  * 但上述方法存在一个问题，就是无法确定起始那个页位置
    * 增加一个页表，存放每一个table和它的起始pageID,总共20+4B长度，1页可以存170个表

## 11.16

* ***page和disk耦合性很强无法分包，之后尝试找到一种解耦合方法

# 11.19

***后续可以考虑将空闲页做成优先队列并且自动回收末尾的内存
