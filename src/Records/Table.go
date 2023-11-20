package Records

import (
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
	PageId     msg.PageId // 这个不用存进disk里，表示这个表的起始页位置
	CurPage    *structType.Page
	Name       string // 最多TableNameLength长度
	Length     int    // todo: 可能能利用这个懒读取
	ColumnSize int
	RecordSize int
	//FreeSpacePointInPage uint16 // 指示下一个存入的在页的数据区中的地址
	//FreeSpace            msg.FreeSpaceTypeInTable
	Column  []Column
	Records []structType.Record
	//StartPageID msg.PageId // 这个不用存进disk里，表示这个表的页所构成的链表的头
}

// NewTable 创建一个新的表，名字是name，str表示“变量名1 变量名1类型 变量名2 变量名2类型”，tableList中存放它的地址
func NewTable(name string, str string, tableList *[]*Table, pageManager *pageMgr.PageManager, diskManager *diskMgr.DiskManager) (*Table, error) {
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
	err := table.ToDiskForNewTable(diskManager, pageManager)
	if err != nil {
		return nil, err
	}
	return &table, nil
}

func LoadTableByName(name string, diskManager *diskMgr.DiskManager, tableList *[]*Table) (*Table, error) {
	table := Table{}
	pageId, err := diskManager.FindPageIdByName(name)
	table.PageId = pageId
	if err != nil {
		return nil, err
	}
	page, err := diskManager.GetPageById(pageId)
	if err != nil {
		return nil, err
	}
	err = table.LoadDataFromPage(page, diskManager)
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

// Insert 记录的插入操作，str表示“变量1的值 变量2的值...”
func (this *Table) Insert(str string, diskManager *diskMgr.DiskManager) error {
	// 如果插入数据后超过1页，则将之前的写入
	if msg.PageRemainSize-int(this.CurPage.GetFreeSpace()) < this.RecordSize+1 { // 有1B的标志位
		// 直接使用页进行写入
		_, err := diskManager.WritePage(this.CurPage.GetPageId(), this.CurPage)
		if err != nil {
			return err
		}
		if this.CurPage.GetNextPageId() == -1 {
			ID := diskManager.GetNewPageId()
			this.CurPage.SetNextPageId(ID)
		}
		this.CurPage, err = diskManager.GetPageById(this.CurPage.GetNextPageId())
		if err != nil {
			return err
		}
	}
	items := strings.Fields(str)
	if len(items) != len(this.Column) {
		return errors.New("error: While inserting into Table, count is not same")
	}
	record := structType.Record{}
	bytes := make([]byte, 0)
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
	bytes = append(bytes, utils.Bool2Bytes(true)...) // 1B标志位
	_, err := this.CurPage.InsertDataToFreeSpace(bytes)
	if err != nil {
		return err
	}
	this.Records = append(this.Records, record)
	this.Length++
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
func (this *Table) Query(key []string, value []any, diskManager *diskMgr.DiskManager, startPageIDs ...msg.PageId) ([]structType.Record, error) {
	// 模拟默认参数
	//var startPageID msg.PageId
	//if len(startPageIDs) == 0 {
	//	startPageID = this.PageId
	//} else if len(startPageIDs) == 1 {
	//	startPageID = startPageIDs[0]
	//} else {
	//	log.Fatal("parameter is invalid")
	//}
	n := len(this.Records) // 总共查询记录的个数
	var queryRecords []structType.Record
	idx, err := this.queryidx(key[0]) // column[idx]表示要查询的记录值
	if err != nil {
		return nil, err
	}
	for i := 0; i < n; i++ {
		if this.Records[i].Value[idx] == value[0] {
			queryRecords = append(queryRecords, this.Records[i])
		}
	}
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
	// 递归读取下一个页的数据
	if this.CurPage.GetNextPageId() != -1 {
		newPage, err2 := diskManager.GetPageById(this.CurPage.GetNextPageId())
		if err2 != nil {
			return nil, err2
		}
		err := this.LoadDataFromPage(newPage, diskManager)
		if err != nil {
			return nil, err
		}

	}
	return queryRecords, nil
}

// Update 这个查询属于比较底层的，所以可以通过前面的步骤过滤到提供两个list
func (this *Table) Update(key []string, value []any, updateKey []string, updateValue []any) error {
	n := len(this.Records) // 总共查询记录的个数
	var queryPos []int
	idx, err := this.queryidx(key[0]) // column[idx]表示要查询的记录值
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if this.Records[i].Value[idx] == value[0] {
			queryPos = append(queryPos, i)
		}
	}
	for j := 1; j < len(value); j++ {
		n := len(queryPos)
		var localPos []int
		idx, err := this.queryidx(key[j]) // column[idx]表示要查询的记录值
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if this.Records[queryPos[i]].Value[idx] == value[j] {
				localPos = append(localPos, queryPos[i])
			}
		}
		queryPos = localPos
	}
	n = len(queryPos)
	for j := 0; j < len(updateValue); j++ {
		queryidx, err := this.queryidx(updateKey[j])
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			this.Records[queryPos[i]].Value[queryidx] = updateValue[j]
		}
	}
	return nil
}

// Delete 这个查询属于比较底层的，所以可以通过前面的步骤过滤到提供两个list
func (this *Table) Delete(keys []string, values []any) error {
	if len(keys) != len(values) {
		return errors.New("error: key and value slices must have the same length")
	}

	for i := 0; i < len(this.Records); i++ {
		record := this.Records[i]
		match := true
		for j := 0; j < len(keys); j++ {
			key := keys[j]
			value := values[j]
			// 查找键在列中的索引
			index, err := this.queryidx(key)
			if err != nil {
				return err
			}
			// 检查记录中的值是否匹配
			if record.Value[index] != value {
				match = false
				break
			}
		}
		if match {
			this.Records = append(this.Records[:i], this.Records[i+1:]...)
			this.Length--
			i--
		}
	}
	return nil
}

func (this *Table) ToDiskForNewTable(diskManager *diskMgr.DiskManager, GlobalPageManager *pageMgr.PageManager) error {
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
	freeSpaceOff := 0
	temp = utils.Int162Bytes(0)
	bytes = append(bytes, temp...)
	for i := 0; i < this.ColumnSize; i++ {
		columnBytes := make([]byte, 0, msg.RecordNameLength+msg.RecordTypeSize)
		columnBytes = append(columnBytes, []byte(this.Column[i].Name)...)
		columnBytes = utils.FixSliceLength(columnBytes, msg.RecordNameLength)
		columnBytes = append(columnBytes, []byte(this.Column[i].ItsType)...)
		columnBytes = utils.FixSliceLength(columnBytes, msg.RecordNameLength+msg.RecordTypeSize)
		bytes = append(bytes, columnBytes...)
	}
	freeSpaceOff = len(bytes)
	_, err := utils.InsertAndReplaceAtIndex(bytes, msg.TableNameLength+3*msg.IntSize, utils.Int162Bytes(int16(freeSpaceOff)))
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
	//ID=-1表示还没有收到页，那么就分配一个
	if this.PageId == -1 {
		page = GlobalPageManager.NewPage(diskManager)
		err := diskManager.InsertTableToTablePage(this.Name, page.GetPageId())
		if err != nil {
			return err
		}
	} else {
		page, err = diskManager.GetPageById(this.PageId)
		if err != nil {
			if err == io.EOF {
				// 说明当前table所属的page没有被初始化
				page = GlobalPageManager.NewPageWithID(this.PageId)
			} else {
				return err
			}
		}
	}
	//对于每个表中的column，和表头一起处理，可以节省空间
	_, err = GlobalPageManager.InsertMultipleDataForNewTable(page, bytes, msg.TableNameLength+3*msg.IntSize+2+this.ColumnSize*(msg.TableNameLength+msg.RecordTypeSize), this.RecordSize, diskManager)
	if err != nil {
		return err
	}
	//this.FreeSpacePointInPage = offset
	//_, err = storage.GlobalDiskManager.WritePage(page.GetPageId(), page)

	return nil
}

// LoadDataFromPage 将数据从page解析到表里
func (this *Table) LoadDataFromPage(page *structType.Page, diskManager *diskMgr.DiskManager) error {
	bytes := page.GetData()
	name := bytes[:msg.TableNameLength]
	name = utils.RemoveTrailingNullBytes(name)
	length := bytes[msg.TableNameLength : msg.TableNameLength+msg.IntSize]
	columnSize := bytes[msg.TableNameLength+msg.IntSize : msg.TableNameLength+2*msg.IntSize]
	recordSize := bytes[msg.TableNameLength+2*msg.IntSize : msg.TableNameLength+3*msg.IntSize]
	this.Name = string(name)
	this.Length = utils.Bytes2Int(length)
	this.ColumnSize = utils.Bytes2Int(columnSize)
	this.RecordSize = utils.Bytes2Int(recordSize)
	for i := 0; i < (msg.TableNameLength+3*msg.IntSize+msg.FreeSpaceSizeInTable+this.ColumnSize*(msg.RecordNameLength+msg.RecordTypeSize))/msg.PageRemainSize; i++ {
		var err error
		page, err = diskManager.GetPageById(page.GetNextPageId())
		if err != nil {
			return err
		}
		bytes = append(bytes, page.GetData()...)
	}
	pos := msg.TableNameLength + 3*msg.IntSize + msg.FreeSpaceSizeInTable
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
		records = append(records, record)
	}
	this.Records = records
	//if page.GetNextPageId() != -1 {
	//	this.NextPageID = page.GetNextPageId()
	//}

	return nil
}
