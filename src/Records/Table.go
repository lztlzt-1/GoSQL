package Records

import (
	"GoSQL/src/buffer"
	"GoSQL/src/msg"
	"GoSQL/src/storage/diskMgr"
	"GoSQL/src/storage/pageMgr"
	"GoSQL/src/structType"
	"GoSQL/src/utils"
	"errors"
	"io"
	"log"
	"strconv"
	"strings"
)

type Column struct {
	Name    string
	ItsType string
}

type Table struct {
	PageId        msg.PageId // 这个不用存进disk里，表示这个表的起始页位置
	RecordStartID msg.PageId // 这个不用存进disk，表示记录开始是在哪个页的
	StartOff      int        // 这个不用存进disk，表示记录开始那个页第一个记录的偏移量
	CurPage       *structType.Page
	Name          string // 最多TableNameLength长度
	Length        int    // todo: 可能能利用这个懒读取
	ColumnSize    int
	RecordSize    int
	//FreeSpacePointInPage uint16 // 指示下一个存入的在页的数据区中的地址
	//FreeSpace            msg.FreeSpaceTypeInTable
	Column  []Column
	Records []structType.Record
	//StartPageID msg.PageId // 这个不用存进disk里，表示这个表的页所构成的链表的头
}

// NewTable 创建一个新的表，名字是name，str表示“变量名1 变量名1类型 变量名2 变量名2类型”，tableList中存放它的地址,创建表会直接写入到磁盘
func NewTable(name string, str string, tableList *[]*Table, pageManager *pageMgr.PageManager, bufferManager *buffer.BufferPoolManager, diskManager *diskMgr.DiskManager) (*Table, error) {
	pageId, _ := diskManager.FindPageIdByName(name)
	if pageId != -1 {
		return nil, errors.New("the table is already exist")
	}
	list := strings.Fields(str)
	if len(list)&1 != 0 {
		return nil, errors.New("invalid string, please check")
	}
	var column []Column
	recordSize := 0
	for i := 0; i < len(list); i++ {
		name := list[i]
		if len(name) > msg.TableNameLength {
			return nil, errors.New("table name is too large")
		}
		itsType := list[i+1]
		i++
		if utils.JudgeSize(itsType) == msg.ErrorType {
			return nil, errors.New("invalid data type, please check")
		}
		size := utils.JudgeSize(itsType)
		if size == -1 {
			return nil, errors.New("invalid data type, please check")
		}
		recordSize += size
		column = append(column, Column{Name: name, ItsType: itsType})
	}
	//newID := utils.GetNewPageId()

	//err = GlobalDiskManager.InsertTableToTablePage(name, newID)
	//if err != nil {
	//	return nil, err
	//}
	table := Table{PageId: -1, Name: name, ColumnSize: len(column), Column: column, Length: 0, RecordSize: recordSize}
	*tableList = append(*tableList, &table)
	err := table.ToDiskForNewTable(pageManager, bufferManager, diskManager)
	if err != nil {
		return nil, err
	}
	err = diskManager.DumpPageTable()
	if err != nil {
		log.Fatal("error while insert table page")
	}
	return &table, nil
}

func LoadTableByName(name string, bufferManager *buffer.BufferPoolManager, diskManager *diskMgr.DiskManager, tableList *[]*Table) (*Table, error) {
	table := Table{}
	pageId, err := diskManager.FindPageIdByName(name)
	table.PageId = pageId
	if err != nil {
		return nil, err
	}
	page, err := bufferManager.GetPageById(pageId)
	if err != nil {
		return nil, err
	}
	err = table.LoadDataFromPage(page, bufferManager)
	if err != nil {
		return nil, err
	}
	*tableList = append(*tableList, &table)
	return &table, nil
}

//
//func (this *Table)WritePageForInsert(diskManager diskMgr.DiskManager)  {
//	if this.CurPageID==-1{
//		this.CurPageID=this.PageId
//	}
//	page, err := diskManager.GetPageById(this.CurPageID)
//	if err != nil {
//		return
//	}
//	for i:=0;i<len(this.Records);i++{
//		page.
//	}
//
//}

func (this *Table) InsertBigRecord(str string, bufferManager *buffer.BufferPoolManager, diskManager *diskMgr.DiskManager) error {
	// 处理大对象的操作，直接存一个指向数据页表的地址
	if this.RecordSize+1 < msg.PageRemainSize {
		err := this.Insert(str, diskManager, bufferManager)
		if err != nil {
			return err
		}
		return nil
	}
	//bufferManager.Pin(this.CurPage) // 表示在完成插入之前curPage一直会在缓冲中
	ID := diskManager.GetNewPageId()
	bytes := utils.Int2Bytes(int(ID)) // 记录只有一个pageID和1B有效位
	bytes = append(bytes, utils.Bool2Bytes(true)...)
	if msg.PageRemainSize-int(bufferManager.GetFreeSpace(this.CurPage)) < msg.IntSize+1 {

		if bufferManager.GetNextPageID(this.CurPage) == -1 {
			ID := diskManager.GetNewPageId()
			bufferManager.SetNextPageID(this.CurPage, ID)
			//this.CurPage.SetNextPageId(ID)
			//bufferManager.InsertPage(this.CurPage)
		}
		var err error
		//bufferManager.UnPin(this.CurPage)
		this.CurPage, err = bufferManager.GetPageById(bufferManager.GetNextPageID(this.CurPage))
		//bufferManager.Pin(this.CurPage)
		if err != nil {
			return err
		}
	}
	_, err := bufferManager.InsertDataToFreeSpace(this.CurPage, bytes)
	//bufferManager.InsertPage(this.CurPage, true, diskManager)
	if err != nil {
		return err
	}
	// todo: 插入记录
	items := strings.Fields(str)
	if len(items) != len(this.Column) {
		return errors.New("error: While inserting into Table, count is not same")
	}
	record := structType.Record{}
	bytes = make([]byte, 0)
	//将所有传入的值转化成对应value，并检查错误
	for i := 0; i < len(items); i++ {
		switch this.Column[i].ItsType {
		case "int":
			d, err := strconv.Atoi(items[i])
			if err != nil {
				return err
			}
			record.Value = append(record.Value, d)
			bytes = append(bytes, utils.Int2Bytes(d)...)
		case "bool":
			if items[i] == "true" {
				record.Value = append(record.Value, true)
				bytes = append(bytes, utils.Bool2Bytes(true)...)
			} else if items[i] == "false" {
				record.Value = append(record.Value, false)
				bytes = append(bytes, utils.Bool2Bytes(false)...)
			} else {
				return errors.New("error: While inserting into Table, expect true or false")
			}
		case "string":
			if len(items[i]) > msg.StringSize {
				log.Printf("failed insert: the string is too large")
				return nil
			}
			record.Value = append(record.Value, items[i])
			strBytes := []byte(items[i])
			strBytes = utils.FixSliceLength(strBytes, msg.StringSize)
			bytes = append(bytes, strBytes...)
		}
	}
	page, err := bufferManager.GetPageById(ID)
	if err != nil {
		return err
	}
	for len(bytes) > msg.PageRemainSize {
		bufferManager.SetData(page, bytes[:msg.PageRemainSize])
		ID := diskManager.GetNewPageId()
		bufferManager.SetNextPageID(page, ID)
		//bufferManager.InsertPage(page)
		page, err = bufferManager.GetPageById(ID)
		if err != nil {
			return err
		}
		bytes = bytes[msg.PageRemainSize:]
	}
	bufferManager.SetData(page, bytes)
	//page.SetData(bytes)
	//bufferManager.UnPin(page)
	//bufferManager.UnPin(this.CurPage)
	//bufferManager.InsertPage(page,  diskManager)
	return nil
}

// Insert 记录的插入操作，str表示“变量1的值 变量2的值...”
func (this *Table) Insert(str string, diskManager *diskMgr.DiskManager, bufferManager *buffer.BufferPoolManager) error {
	if this.RecordSize+1 >= msg.PageRemainSize {
		err := this.InsertBigRecord(str, bufferManager, diskManager)
		if err != nil {
			return err
		}
		return nil
	}
	// 如果插入数据后超过1页，则将之前的写入
	//bufferManager.InsertPage(this.CurPage)
	//bufferManager.Pin(this.CurPage)
	for msg.PageRemainSize-int(bufferManager.GetFreeSpace(this.CurPage)) < this.RecordSize+1 { // 有1B的标志位
		// 直接使用页进行写入
		if bufferManager.GetNextPageID(this.CurPage) == -1 {
			//先将nextPage设置好
			ID := diskManager.GetNewPageId()
			bufferManager.SetNextPageID(this.CurPage, ID)
			//this.CurPage.SetNextPageId(ID)
			//bufferManager.InsertPage(this.CurPage)
		}
		var err error
		//bufferManager.UnPin(this.CurPage)
		this.CurPage, err = bufferManager.GetPageById(bufferManager.GetNextPageID(this.CurPage))
		//bufferManager.Pin(this.CurPage)
		if err != nil {
			return err
		}
		this.Records = this.Records[:0] // 换页后直接清空记录
	}
	items := strings.Fields(str)
	if len(items) != len(this.Column) {
		return errors.New("error: While inserting into Table, count is not same")
	}
	record := structType.Record{}
	bytes := make([]byte, 0)
	//将所有传入的值转化成对应value，并检查错误
	bytes = append(bytes, utils.Bool2Bytes(true)...) // 1B标志位
	for i := 0; i < len(items); i++ {
		switch this.Column[i].ItsType {
		case "int":
			d, err := strconv.Atoi(items[i])
			if err != nil {
				return err
			}
			record.Value = append(record.Value, d)
			bytes = append(bytes, utils.Int2Bytes(d)...)
		case "bool":
			if items[i] == "true" {
				record.Value = append(record.Value, true)
				bytes = append(bytes, utils.Bool2Bytes(true)...)
			} else if items[i] == "false" {
				record.Value = append(record.Value, false)
				bytes = append(bytes, utils.Bool2Bytes(false)...)
			} else {
				return errors.New("error: While inserting into Table, expect true or false")
			}
		case "string":
			if len(items[i]) > msg.StringSize {
				log.Printf("failed insert: the string is too large")
				return nil
			}
			record.Value = append(record.Value, items[i])
			strBytes := []byte(items[i])
			strBytes = utils.FixSliceLength(strBytes, msg.StringSize)
			bytes = append(bytes, strBytes...)
		}
	}
	this.Length++
	_, err := bufferManager.InsertDataToFreeSpace(this.CurPage, bytes)
	if err != nil {
		return err
	}
	//bufferManager.InsertPage(this.CurPage, true, diskManager)
	this.Records = append(this.Records, record)
	//diskManager.InsertDirtyPageToList(this.CurPage)
	//bufferManager.UnPin(this.CurPage)
	return nil
}

// 私有函数，用作查询列的某一项对应的下标
func (this *Table) queryidx(key string) (int, error) {
	for i := 0; i < len(this.Column); i++ {
		if this.Column[i].Name == key {
			return i, nil
		}
	}
	return 0, errors.New("error: invalid column name")
}

// Query 这个查询属于比较底层的，所以可以通过前面的步骤过滤到提供两个list,表示每一个key对应的value是数组里的值则拿出
func (this *Table) Query(key []string, value []any, bufferManager *buffer.BufferPoolManager) ([]structType.Record, error) {
	var queryRecords []structType.Record
	//bufferManager.Pin(this.CurPage)
	idx, err := this.queryidx(key[0]) // column[idx]表示要查询的记录值
	if err != nil {
		return nil, err
	}
	//保存当前页的ID，遍历每一个页，当遍历到末尾则通过table的pageID转到第1页，知道遍历到ID等于当前ID，
	// 从头找到第一个记录record的页
	for bufferManager.GetPageId(this.CurPage) != this.RecordStartID {
		//bufferManager.UnPin(this.CurPage)
		nextID := bufferManager.GetNextPageID(this.CurPage)
		if nextID == -1 {
			nextID = this.PageId
		}
		this.CurPage, err = bufferManager.GetPageById(nextID)
		//bufferManager.Pin(this.CurPage)
		if err != nil {
			return nil, err
		}
	} //遍历完当前页，结束遍历
	//startID := bufferManager.GetPageId(this.CurPage)
	for {
		pos := 0
		if bufferManager.GetPageId(this.CurPage) == this.RecordStartID {
			pos = this.StartOff
		}
		bytes := bufferManager.GetData(this.CurPage)
		for len(bytes) >= pos+this.RecordSize+1 {
			flag := utils.Bytes2Bool(bytes[pos : pos+1])
			pos++
			if flag == false {
				pos += this.RecordSize
				continue
			}
			// 从页中提取出一条记录
			thisRecord := structType.Record{}
			for i := 0; i < this.ColumnSize; i++ {
				size := utils.GetTypeSize(this.Column[i].ItsType)
				if size == msg.ErrorType {
					return nil, errors.New("record type unknown")
				}
				value := utils.Bytes2Any(bytes[pos:pos+size], this.Column[i].ItsType)
				thisRecord.Value = append(thisRecord.Value, value)
				pos += size
			}
			if thisRecord.Value[idx] == value[0] {
				queryRecords = append(queryRecords, thisRecord)
			}
		}
		if bufferManager.GetNextPageID(this.CurPage) == -1 {
			//bufferManager.UnPin(this.CurPage)
			this.CurPage, err = bufferManager.GetPageById(this.PageId)
			//bufferManager.Pin(this.CurPage)
			if err != nil {
				return nil, err
			}
			break
		} else {
			//bufferManager.UnPin(this.CurPage)
			this.CurPage, err = bufferManager.GetPageById(bufferManager.GetNextPageID(this.CurPage))
			//bufferManager.Pin(this.CurPage)
			if err != nil {
				return nil, err
			}
		}
	}
	// 找完了第一个关键词的所有信息，之后只需要对找到了这些记录进行筛选
	for j := 1; j < len(value); j++ {
		n := len(queryRecords)
		var localRecords []structType.Record
		idx, err := this.queryidx(key[j]) // column[idx]表示要查询的记录值
		if err != nil {
			return nil, err
		}
		for i := 0; i < n; i++ {
			if queryRecords[i].Value[idx] == value[j] {
				localRecords = append(localRecords, queryRecords[i])
			}
		}
		queryRecords = localRecords
	}
	return queryRecords, nil
}

// Update 这个查询属于比较底层的，所以可以通过前面的步骤过滤到提供两个list
func (this *Table) Update(key []string, value []any, updateKey []string, updateValue []any, bufferManager *buffer.BufferPoolManager) error {
	var queryRecords []utils.Triplet
	//bufferManager.Pin(this.CurPage)
	idx, err := this.queryidx(key[0]) // column[idx]表示要查询的记录值
	if err != nil {
		return err
	}
	//保存当前页的ID，遍历每一个页，当遍历到末尾则通过table的pageID转到第1页，知道遍历到ID等于当前ID，
	// 从头找到第一个记录record的页
	for bufferManager.GetPageId(this.CurPage) != this.RecordStartID {
		//bufferManager.UnPin(this.CurPage)
		nextID := bufferManager.GetNextPageID(this.CurPage)
		if nextID == -1 {
			nextID = this.PageId
		}
		this.CurPage, err = bufferManager.GetPageById(nextID)
		//bufferManager.Pin(this.CurPage)
		if err != nil {
			return err
		}
	} //遍历完当前页，结束遍历
	//startID := bufferManager.GetPageId(this.CurPage)
	for {
		pos := 0
		if bufferManager.GetPageId(this.CurPage) == this.RecordStartID {
			pos = this.StartOff
		}
		bytes := bufferManager.GetData(this.CurPage)
		for len(bytes) >= pos+this.RecordSize+1 {
			flag := utils.Bytes2Bool(bytes[pos : pos+1])
			startPos := pos
			pos++
			if flag == false {
				pos += this.RecordSize
				continue
			}
			// 从页中提取出一条记录
			thisRecord := structType.Record{}
			for i := 0; i < this.ColumnSize; i++ {
				size := utils.GetTypeSize(this.Column[i].ItsType)
				if size == msg.ErrorType {
					return errors.New("record type unknown")
				}
				value := utils.Bytes2Any(bytes[pos:pos+size], this.Column[i].ItsType)
				thisRecord.Value = append(thisRecord.Value, value)
				pos += size
			}
			if thisRecord.Value[idx] == value[0] {
				tb := utils.Triplet{
					First:  thisRecord,
					Second: this.CurPage.GetPageId(),
					Third:  startPos,
				}
				queryRecords = append(queryRecords, tb)
			}
		}
		if bufferManager.GetNextPageID(this.CurPage) == -1 {
			//bufferManager.UnPin(this.CurPage)
			this.CurPage, err = bufferManager.GetPageById(this.PageId)
			//bufferManager.Pin(this.CurPage)
			if err != nil {
				return err
			}
			break
		} else {
			//bufferManager.UnPin(this.CurPage)
			this.CurPage, err = bufferManager.GetPageById(bufferManager.GetNextPageID(this.CurPage))
			//bufferManager.Pin(this.CurPage)
			if err != nil {
				return err
			}
		}
	}
	// 找完了第一个关键词的所有信息，之后只需要对找到了这些记录进行筛选
	for j := 1; j < len(value); j++ {
		n := len(queryRecords)
		var localRecords []utils.Triplet
		idx, err := this.queryidx(key[j]) // column[idx]表示要查询的记录值
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if queryRecords[i].First.(structType.Record).Value[idx] == value[j] {
				localRecords = append(localRecords, queryRecords[i])
			}
		}
		queryRecords = localRecords
	}
	//完成查找，接下来进行修改
	var idxs []int
	for _, item := range updateKey {
		idx, err := this.queryidx(item)
		if err != nil {
			return err
		}
		idxs = append(idxs, idx)
	}
	for i := 0; i < len(queryRecords); i++ {
		for j, idx := range idxs {
			queryRecords[i].First.(structType.Record).Value[idx] = updateValue[j]
		}
		bytes := make([]byte, 0, this.RecordSize)
		bytes = append(bytes, 1)
		for j := 0; j < this.ColumnSize; j++ {
			//size:=utils.GetTypeSize(this.Column[j].ItsType)
			value := utils.Any2BytesForPage(queryRecords[i].First.(structType.Record).Value[j])
			bytes = append(bytes, value...)
		}
		page, err := bufferManager.GetPageById(queryRecords[i].Second.(msg.PageId))
		if err != nil {
			return err
		}
		data := bufferManager.GetData(page)
		data, err = utils.InsertAndReplaceAtIndex(data, queryRecords[i].Third.(int), bytes)
		if err != nil {
			return err
		}
		log.Printf("%p", queryRecords[i].Second)
		page, err = bufferManager.GetPageById(queryRecords[i].Second.(msg.PageId))
		if err != nil {
			return err
		}
		bufferManager.SetData(page, data)
	}
	return nil
}

// Delete 这个查询属于比较底层的，所以可以通过前面的步骤过滤到提供两个list
func (this *Table) Delete(key []string, value []any, bufferManager *buffer.BufferPoolManager) error {
	var queryRecords []utils.Triplet
	//bufferManager.Pin(this.CurPage)
	idx, err := this.queryidx(key[0]) // column[idx]表示要查询的记录值
	if err != nil {
		return err
	}
	//保存当前页的ID，遍历每一个页，当遍历到末尾则通过table的pageID转到第1页，知道遍历到ID等于当前ID，
	// 从头找到第一个记录record的页
	for bufferManager.GetPageId(this.CurPage) != this.RecordStartID {
		//bufferManager.UnPin(this.CurPage)
		nextID := bufferManager.GetNextPageID(this.CurPage)
		if nextID == -1 {
			nextID = this.PageId
		}
		this.CurPage, err = bufferManager.GetPageById(nextID)
		//bufferManager.Pin(this.CurPage)
		if err != nil {
			return err
		}
	} //遍历完当前页，结束遍历
	//startID := bufferManager.GetPageId(this.CurPage)
	for {
		pos := 0
		if bufferManager.GetPageId(this.CurPage) == this.RecordStartID {
			pos = this.StartOff
		}
		bytes := bufferManager.GetData(this.CurPage)
		for len(bytes) >= pos+this.RecordSize+1 {
			flag := utils.Bytes2Bool(bytes[pos : pos+1])
			startPos := pos
			pos++
			if flag == false {
				pos += this.RecordSize
				continue
			}
			// 从页中提取出一条记录
			thisRecord := structType.Record{}
			for i := 0; i < this.ColumnSize; i++ {
				size := utils.GetTypeSize(this.Column[i].ItsType)
				if size == msg.ErrorType {
					return errors.New("record type unknown")
				}
				value := utils.Bytes2Any(bytes[pos:pos+size], this.Column[i].ItsType)
				thisRecord.Value = append(thisRecord.Value, value)
				pos += size
			}
			if thisRecord.Value[idx] == value[0] {
				tb := utils.Triplet{
					First:  thisRecord,
					Second: this.CurPage.GetPageId(),
					Third:  startPos,
				}
				queryRecords = append(queryRecords, tb)
			}
		}
		if bufferManager.GetNextPageID(this.CurPage) == -1 {
			//bufferManager.UnPin(this.CurPage)
			this.CurPage, err = bufferManager.GetPageById(this.PageId)
			//bufferManager.Pin(this.CurPage)
			if err != nil {
				return err
			}
			break
		} else {
			//bufferManager.UnPin(this.CurPage)
			this.CurPage, err = bufferManager.GetPageById(bufferManager.GetNextPageID(this.CurPage))
			//bufferManager.Pin(this.CurPage)
			if err != nil {
				return err
			}
		}
	}
	// 找完了第一个关键词的所有信息，之后只需要对找到了这些记录进行筛选
	for j := 1; j < len(value); j++ {
		n := len(queryRecords)
		var localRecords []utils.Triplet
		idx, err := this.queryidx(key[j]) // column[idx]表示要查询的记录值
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if queryRecords[i].First.(structType.Record).Value[idx] == value[j] {
				localRecords = append(localRecords, queryRecords[i])
			}
		}
		queryRecords = localRecords
	}
	//完成查找，接下来进行修改
	for i := 0; i < len(queryRecords); i++ {
		pageID := queryRecords[i].Second.(msg.PageId)
		offset := queryRecords[i].Third.(int)
		page, err := bufferManager.GetPageById(pageID)
		if err != nil {
			return err
		}
		bytes := make([]byte, 0, this.RecordSize+1)
		bytes = append(bytes, 0)
		freeSpace := bufferManager.GetFreeSpace(page)
		bytes = append(bytes, utils.Int162Bytes(int16(freeSpace))...)
		freeSpace = msg.FreeSpaceTypeInTable(offset)
		bytes = utils.FixSliceLength(bytes, this.RecordSize+1)
		data := bufferManager.GetData(page)
		data, err = utils.InsertAndReplaceAtIndex(data, int(offset), bytes)
		if err != nil {
			return err
		}
		this.Length--
		bufferManager.SetData(page, data)
		bufferManager.SetFreeSpace(page, msg.FreeSpaceTypeInTable(offset))
	}
	return nil
}

func (this *Table) ToDiskForNewTable(pageManager *pageMgr.PageManager, bufferManager *buffer.BufferPoolManager, diskManager *diskMgr.DiskManager) error {
	//var pages []storage.Page
	var bytes []byte
	name := make([]byte, 0, msg.TableNameLength)
	name = append(name, []byte(this.Name)...)
	name = utils.FixSliceLength(name, msg.TableNameLength)
	bytes = append(bytes, name...)
	temp := utils.Int2Bytes(this.Length)
	bytes = append(bytes, temp...)
	temp = utils.Int2Bytes(this.ColumnSize)
	bytes = append(bytes, temp...)
	temp = utils.Int2Bytes(this.RecordSize)
	bytes = append(bytes, temp...)
	//freeSpaceOff := 0
	//temp = utils.Int162Bytes(0)
	//bytes = append(bytes, temp...)
	for i := 0; i < this.ColumnSize; i++ {
		columnBytes := make([]byte, 0, msg.RecordNameLength+msg.RecordTypeSize)
		columnBytes = append(columnBytes, []byte(this.Column[i].Name)...)
		columnBytes = utils.FixSliceLength(columnBytes, msg.RecordNameLength)
		columnBytes = append(columnBytes, []byte(this.Column[i].ItsType)...)
		columnBytes = utils.FixSliceLength(columnBytes, msg.RecordNameLength+msg.RecordTypeSize)
		bytes = append(bytes, columnBytes...)
	}
	//freeSpaceOff = len(bytes)
	//_, err := utils.InsertAndReplaceAtIndex(bytes, msg.TableNameLength+3*msg.IntSize, utils.Int162Bytes(int16(freeSpaceOff)))
	//if err != nil {
	//	return err
	//}
	//for i := 0; i < len(this.Records); i++ {
	//	recordBytes := make([]byte, 0, this.RecordSize+1)
	//	recordBytes = append(recordBytes, utils.Bool2Bytes(true)...) // 多1B的标志位
	//	for j := 0; j < len(this.Records[i].Value); j++ {
	//		recordBytes = append(recordBytes, utils.Any2BytesForPage(this.Records[i].Value[j])...)
	//	}
	//	bytes = append(bytes, recordBytes...)
	//}
	var page *structType.Page
	var err error
	//ID=-1表示还没有收到页，那么就分配一个
	if this.PageId == -1 {
		page = pageManager.NewPage(diskManager)
		err := diskManager.InsertTableToTablePage(this.Name, bufferManager.GetPageId(page))
		this.PageId = bufferManager.GetPageId(page)
		this.CurPage = page
		if err != nil {
			return err
		}
	} else {
		page, err = bufferManager.GetPageById(this.PageId)
		if err != nil {
			if err == io.EOF {
				// 说明当前table所属的page没有被初始化
				page = pageManager.NewPageWithID(this.PageId)
				this.PageId = bufferManager.GetPageId(page)
				this.CurPage = page
			} else {
				return err
			}
		}
	}
	//对于每个表中的column，和表头一起处理，可以节省空间
	recordStartID, off, err := pageManager.InsertMultipleDataForNewTable(page, bytes, msg.TableNameLength+3*msg.IntSize+this.ColumnSize*(msg.TableNameLength+msg.RecordTypeSize), this.RecordSize, diskManager)
	if err != nil {
		return err
	}
	this.RecordStartID = msg.PageId(recordStartID)
	this.StartOff = off
	//bufferManager.SetFreeSpace(this.CurPage, msg.FreeSpaceTypeInTable(off))
	//this.FreeSpacePointInPage = offset
	//_, err = storage.GlobalDiskManager.WritePage(page.GetPageId(), page)
	//bufferManager.InsertPage(this.CurPage)
	return nil
}

// LoadDataFromPage 将数据从page解析到表里
func (this *Table) LoadDataFromPage(page *structType.Page, bufferManager *buffer.BufferPoolManager) error {
	//bufferManager.InsertPage(page)
	bytes := bufferManager.GetData(page)
	name := bytes[:msg.TableNameLength]
	name = utils.RemoveTrailingNullBytes(name)
	length := bytes[msg.TableNameLength : msg.TableNameLength+msg.IntSize]
	columnSize := bytes[msg.TableNameLength+msg.IntSize : msg.TableNameLength+2*msg.IntSize]
	recordSize := bytes[msg.TableNameLength+2*msg.IntSize : msg.TableNameLength+3*msg.IntSize]
	this.Name = string(name)
	this.Length = utils.Bytes2Int(length)
	this.ColumnSize = utils.Bytes2Int(columnSize)
	this.RecordSize = utils.Bytes2Int(recordSize)
	this.CurPage = page
	// 加载进包括column在内的所有table头信息
	for i := 0; i < (msg.TableNameLength+3*msg.IntSize+this.ColumnSize*(msg.RecordNameLength+msg.RecordTypeSize))/msg.PageRemainSize; i++ {
		var err error
		page, err = bufferManager.GetPageById(bufferManager.GetNextPageID(page))
		if err != nil {
			return err
		}
		bytes = append(bytes, bufferManager.GetData(page)...)
	}
	pos := msg.TableNameLength + 3*msg.IntSize
	var columns []Column
	for i := 0; i < this.ColumnSize; i++ {
		name := string(utils.RemoveTrailingNullBytes(bytes[pos : pos+msg.RecordNameLength]))
		itsType := string(utils.RemoveTrailingNullBytes(bytes[pos+msg.RecordNameLength : pos+msg.RecordNameLength+msg.RecordTypeSize]))
		columns = append(columns, Column{
			Name:    name,
			ItsType: itsType,
		})
		pos += msg.RecordNameLength + msg.RecordTypeSize
	}
	this.Column = columns
	var records []structType.Record
	this.CurPage = page
	this.RecordStartID = bufferManager.GetPageId(page)
	this.StartOff = pos % msg.PageRemainSize
	for i := 0; i < this.Length; i++ {
		var record structType.Record
		if msg.PageRemainSize-pos < this.RecordSize {
			break
		}
		for j := 0; j < this.ColumnSize; j++ {
			size := utils.JudgeSize(this.Column[j].ItsType)
			value := utils.Bytes2Any(bytes[pos:pos+size], this.Column[j].ItsType)
			record.Value = append(record.Value, value)
			pos += size
		}
		flag := utils.Bytes2Bool(bytes[pos : pos+1]) // 有1B的有效位
		if flag == false {
			continue
		}
		records = append(records, record)
		pos++
	}
	this.Records = records
	//if page.GetNextPageId() != -1 {
	//	this.NextPageID = page.GetNextPageId()
	//}

	return nil
}

// SaveTableHead 当table执行操作后头文件会有相应的修改，在写入记录页不会写入，所以在系统最后或者关闭表时写入,由于之后关闭表，所以这部分不用写入buffer
func (this *Table) SaveTableHead(diskManager *diskMgr.DiskManager) error {
	page, err := diskManager.GetPageById(this.PageId)
	if err != nil {
		return err
	}
	bytes := page.GetData()
	bytes, err = utils.InsertAndReplaceAtIndex(bytes, msg.TableNameLength, utils.Int2Bytes(this.Length))
	if err != nil {
		return err
	}
	page.SetData(bytes)
	_, err = diskManager.WritePage(page.GetPageId(), page)
	if err != nil {
		return err
	}
	return nil
}
