package diskMgr

import (
	"GoSQL/src/msg"
	"GoSQL/src/structType"
	"GoSQL/src/utils"
	"errors"
	"log"
	"os"
)

type DiskManager struct {
	fp            *os.File // 存储DiskManager指向的文件
	diskPageTable DiskPageTable
	getNewPageId  func() msg.PageId
	freePageList  []msg.PageId
}

// NewDiskManager 全局只需要一个diskManager！扫描disk的文件并提取必要信息
func NewDiskManager(filePath string) (*DiskManager, error) {
	if !utils.FileExists(filePath) {
		initDBFile(filePath) // 进行初始化创建db文件
	}
	file, err := os.OpenFile(filePath, os.O_RDWR, 0660)
	if err != nil {
		return nil, err
	}
	GlobalDiskManager := DiskManager{
		fp:            file,
		diskPageTable: NewDiskPageTable(msg.DiskBucketSize),
		freePageList:  make([]msg.PageId, 0),
	}
	err = GlobalDiskManager.loadPageTable(msg.PageTableStart)
	if err != nil {
		return nil, err
	}
	return &GlobalDiskManager, nil
}

func (this *DiskManager) GetFreePageID() msg.PageId {
	id := msg.PageId(-1)
	if len(this.freePageList) > 0 {
		id = this.freePageList[0]
		this.freePageList = this.freePageList[1:]
	}
	return id
}

func (this *DiskManager) SetFreePageID(params ...msg.PageId) {
	for i := 0; i < len(params); i++ {
		this.freePageList = append(this.freePageList, params[i])
	}
}

// NewPageId 在磁盘中分配一个空闲的pageID
func (this *DiskManager) newPageId(initState msg.PageId) func() msg.PageId {
	generatePageId := func(state any) any {
		cur := state.(msg.PageId)
		cur = cur + 1
		return cur
	}
	pageGenerator := utils.LazyGenerator(generatePageId, initState)
	return func() msg.PageId {
		initState = pageGenerator().(msg.PageId)
		return initState
	}
}

func (this *DiskManager) GetNewPageId() msg.PageId {
	id := this.getNewPageId()
	// 只应允许从这里分配磁盘的一个页，所以直接添加末尾
	err := this.MallocNewPageToEnd()
	if err != nil {
		return 0
	}
	return id
}

// MallocNewPageToEnd 移动到文件末尾并且添加一个空页
func (this *DiskManager) MallocNewPageToEnd() error {
	_, err := this.fp.Seek(0, 2)
	if err != nil {
		return err
	}
	bytes := make([]byte, msg.PageSize)
	_, err = this.fp.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

// 读页表，页表每条记录是20B的名字+4B的ID（偏移量）+1B标志位
func (this *DiskManager) loadPageTable(id msg.PageId) error {
	offset := id * msg.PageSize
	bytes, err := this.GetData(int64(offset), msg.PageSize)
	if err != nil {
		return err
	}
	_ = utils.Any2BytesForPage(bytes[:msg.FreeSpaceSizeInPageTable]) //前两个字节是指向当前页的空闲地址，读取时不需要
	pos := msg.FreeSpaceSizeInPageTable                              //1表示标志位
	for i := 0; i < msg.PageSize/(msg.TableNameLength+msg.PageIDSize+1); i++ {
		flag := utils.Bytes2Bool(bytes[pos : pos+1])
		pos += 1
		tableName := string(utils.RemoveTrailingNullBytes(bytes[pos : pos+msg.TableNameLength]))
		pos += msg.TableNameLength
		pageID := utils.Bytes2Int(bytes[pos : pos+msg.PageIDSize])
		pos += msg.PageIDSize
		if flag == true {
			this.diskPageTable.InsertTable(tableName, msg.PageId(pageID))
		}
	}
	nextPageID := utils.Bytes2Int(bytes[msg.PageSize-4:])
	if nextPageID != -1 {
		err := this.loadPageTable(msg.PageId(nextPageID))
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *DiskManager) InsertTableToTablePage(name string, id msg.PageId) error {
	this.diskPageTable.InsertTable(name, id)
	return nil
}

// DumpPageTable 页表格式：2B第一个空闲位置偏移量，中间是n个有效位1b+tableName20B+pageID4B，末尾是4B下一页pageID
func (this *DiskManager) DumpPageTable() error {
	buckets := this.diskPageTable.hash.GetAllBuckets()
	length := len(buckets)
	sizeInOnePage := (msg.PageSize - msg.FreeSpaceSizeInPageTable - msg.PageIDSize) / (msg.TableNameLength + msg.PageIDSize + 1)
	bytes := make([]byte, 0, msg.PageSize)
	bytes = append(bytes, utils.Int162Bytes(-1)...) // pageTable会统一修改，所以这个-1已经没有意义了
	tablePage := msg.PageId(1)
	for i := 0; i < length; i++ {
		if i%sizeInOnePage == 0 && i != 0 {
			_, err := this.fp.Seek(int64(tablePage)*msg.PageSize, 0)
			if err != nil {
				return err
			}
			blankSize := msg.PageSize - msg.PageIDSize - len(bytes)
			bytes = append(bytes, make([]byte, blankSize)...)
			_, err = this.fp.Write(bytes)
			if err != nil {
				return err
			}
			tablePage = this.getNewPageId()
			_, err = this.fp.Write(utils.Int2Bytes(int(tablePage)))
			if err != nil {
				return err
			}
			bytes = bytes[:0]
			bytes = append(bytes, utils.Int162Bytes(-1)...)
		}
		name := []byte(buckets[i].First.(string))
		name = utils.FixSliceLength(name, msg.TableNameLength)
		bytes = append(bytes, utils.Bool2Bytes(true)...) //标志位
		bytes = append(bytes, name...)
		bytes = append(bytes, utils.Int2Bytes(int(buckets[i].Second.(msg.PageId)))...)
	}
	_, err := this.fp.Seek(int64(tablePage)*msg.PageSize, 0)
	if err != nil {
		return err
	}
	blankSize := msg.PageSize - msg.PageIDSize - len(bytes)
	bytes = append(bytes, make([]byte, blankSize)...)
	_, err = this.fp.Write(bytes)
	if err != nil {
		return err
	}
	tablePage = -1
	_, err = this.fp.Write(utils.Int2Bytes(int(tablePage)))
	if err != nil {
		return err
	}
	// 将initPage重新设置
	return nil
}

// GetData 从偏移量offset的地址下读取length长度数据
func (this *DiskManager) GetData(offset int64, length int) ([]byte, error) {
	_, err := this.fp.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	data := make([]byte, length)
	_, err = this.fp.Read(data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (this *DiskManager) DumpInitPage() error {
	bytes := make([]byte, msg.PageSize)
	_, err := this.fp.Seek(0, 0)
	if err != nil {
		return err
	}
	_, err = this.fp.Read(bytes)
	if err != nil {
		return err
	}
	nextID := this.getNewPageId()
	bytes, err = utils.InsertAndReplaceAtIndex(bytes, msg.MagicSize, utils.Int2Bytes(int(nextID)-1))
	if err != nil {
		return err
	}
	_, err = this.fp.Seek(0, 0)
	if err != nil {
		return err
	}
	_, err = this.fp.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

func initDBFile(filePath string) {
	file, err := os.OpenFile(filePath, os.O_CREATE, 0666)
	if err != nil {
		log.Fatal(err)
	}
	_, err = file.Write([]byte("MagicGoSQL"))
	if err != nil {
		log.Fatal(err)
	}
	_, err = file.Write(utils.Int2Bytes(msg.PageTableStart))
	if err != nil {
		log.Fatal(err)
	}
	bytes := make([]byte, 2*msg.PageSize-msg.MagicSize-msg.IntSize-msg.IntSize)
	bytes = append(bytes, utils.Int2Bytes(-1)...)
	_, err = file.Write(bytes)
	if err != nil {
		log.Fatal(err)
	}
	err = file.Close()
	if err != nil {
		return
	}
	return
}

// WritePage 向磁盘的文件中写入一个页,如果超过文件大小则会在最末尾加
func (this *DiskManager) WritePage(pageId msg.PageId, page *structType.Page) (int, error) {
	offset := pageId * msg.PageSize
	_, err := this.fp.Seek(int64(offset), 0)
	if err != nil {
		return msg.Success, err
	}
	data := make([]byte, msg.PageSize)
	// 按照页的每个属性进行写入
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 0, utils.Int2Bytes(int(page.GetPageId())))
	if err != nil {
		return -1, err
	}
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 4, utils.Int2Bytes(int(page.GetNextPageId())))
	if err != nil {
		return -1, err
	}
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 8, utils.Int162Bytes(int16(page.GetFreeSpace())))
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 10, page.GetData())
	if err != nil {
		return -1, err
	}
	_, err = this.fp.Write(data)
	if err != nil {
		return -1, err
	}
	log.Println(msg.SuccessWritePage(int(pageId)))
	err = this.fp.Sync()
	if err != nil {
		return msg.Success, err
	}
	return len(page.GetData()), nil
	// 可能需要刷新同步数据到磁盘
}

// WriteData 对db文件写入，暂时只有initPage需要
func (this *DiskManager) WriteData(bytes []byte) error {
	_, err := this.fp.Seek(0, 0)
	if err != nil {
		return err
	}
	_, err = this.fp.Write(bytes)
	if err != nil {
		return err
	}
	return nil
}

// ReadPage 从页中的pageId表示偏移量中读出一页,这里的页有页头，一些特殊页无法读取
func (this *DiskManager) ReadPage(pageId msg.PageId) (structType.Page, error) {
	offset := pageId * msg.PageSize
	_, err := this.fp.Seek(int64(offset), 0)
	page := structType.Page{}
	if err != nil {
		return page, err
	}
	data := make([]byte, 4096)
	_, err = this.fp.Read(data)
	if err != nil {
		return page, err
	}
	var readData []byte
	readData, err = utils.ReadBytesFromPosition(data, 0, 4)
	page.SetPageId(msg.PageId(utils.Bytes2Int(readData)))
	readData, err = utils.ReadBytesFromPosition(data, 4, 4)
	page.SetNextPageId(msg.PageId(utils.Bytes2Int(readData)))
	readData, err = utils.ReadBytesFromPosition(data, 8, 2)
	page.SetFreeSpace(msg.FreeSpaceTypeInTable(utils.Bytes2Int16(readData)))
	values, err := utils.ReadBytesFromPosition(data, 10, msg.PageRemainSize)
	page.SetData(values)
	log.Println(msg.SuccessWritePage(int(pageId)))
	return page, nil
}

func (this *DiskManager) FindPageIdByName(name string) (msg.PageId, error) {
	ID := this.diskPageTable.Query(name)
	if ID != -1 {
		return ID, nil
	}
	return -1, errors.New("the table is not exist")
}

func (this *DiskManager) GetPageById(pageid msg.PageId) (*structType.Page, error) {
	_, err := this.fp.Seek(int64(pageid*msg.PageSize), 0)
	if err != nil {
		return nil, err
	}
	pageBytes := make([]byte, msg.PageSize)
	_, err = this.fp.Read(pageBytes)
	if err != nil {
		return nil, err
	}
	id := msg.PageId(utils.Bytes2Int(pageBytes[:4]))
	nextID := utils.Bytes2Int(pageBytes[4:8])
	if nextID == 0 {
		nextID = -1
	}
	freeSpace := utils.Bytes2Int16(pageBytes[8:10])
	bytes := pageBytes[10:]
	if id == 0 {
		id = pageid
		nextID = -1
	}
	page := structType.Page{}
	page.SetPageId(msg.PageId(id))
	page.SetNextPageId(msg.PageId(nextID))
	page.SetFreeSpace(msg.FreeSpaceTypeInTable(freeSpace))
	page.SetPinCount(0)
	page.SetData(bytes)
	page.SetDirty(false)
	return &page, nil
}

// Deprecated: Use GetData instead.
func (this *DiskManager) Read(slice *[]byte) error {
	_, err := this.fp.Read(*slice)
	if err != nil {
		return err
	}
	return nil
}

func WriteLog(logData []byte, size int) {

}

func ReadLog(logData []byte, size int, offset int) {

}

func GetNumFlushes() {

}

func GetFlushState() {

}

func GetNumWrites() {

}

func SetFlushLogFuture() {

}

func HasFlushLogFuture() {

}

func GetFileSize() {

}

// ////////////////////////////////////////////
// 以下是初始化信息
type InitPage struct {
	magic      string
	initPageID msg.PageId
}

func GetInitPage(diskManager *DiskManager) *InitPage {
	this := InitPage{}
	magic, err := diskManager.GetData(0, msg.MagicSize)
	if err != nil || string(magic) != "MagicGoSQL" {
		log.Fatal(errors.New("error: it's not a GoSQL file"))
	}
	this.magic = string(magic)
	initIDBytes, err := diskManager.GetData(msg.MagicSize, msg.IntSize)
	if err != nil {
		log.Fatal(errors.New("error: it's not a GoSQL file"))
	}
	id := utils.Bytes2Int(initIDBytes)
	this.initPageID = msg.PageId(id)
	diskManager.getNewPageId = diskManager.newPageId(this.GetInitPageID())
	return &this
}

func (this *InitPage) SetInitPageToDisk(GlobalDiskManager *DiskManager) error {
	bytes := make([]byte, 0, msg.PageSize)
	bytes = append(bytes, []byte(this.magic)...)
	bytes = append(bytes, utils.Int2Bytes(int(this.initPageID))...)
	bytes = utils.FixSliceLength(bytes, msg.PageSize)
	err := GlobalDiskManager.WriteData(bytes)
	if err != nil {
		return err
	}
	return nil
}

func (this *InitPage) GetInitPageID() msg.PageId {
	return this.initPageID
}

func (this *InitPage) SetInitPageID(pageID msg.PageId) {
	this.initPageID = pageID
}
