package DiskManager

import (
	"GoSQL/src/msg"
	"GoSQL/src/storage/PageManager"
	"GoSQL/src/utils"
	"errors"
	"io"
	"log"
	"os"
	"strings"
)

var GlobalDiskManager DiskManager

type DiskManager struct {
	fp *os.File // 存储DiskManager指向的文件
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
	_, err = file.Write(utils.Int2Bytes(0))
	if err != nil {
		log.Fatal(err)
	}
	file.Close()
	return
}

func NewDiskManager(filePath string) error {
	if !utils.FileExists(filePath) {
		initDBFile(filePath)
	}
	file, err := os.OpenFile(filePath, os.O_RDWR, 0660)
	if err != nil {
		return errors.New(msg.Nofile(filePath))
	}
	GlobalDiskManager = DiskManager{
		fp: file,
	}
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

func Shutdown() {

}

// WritePage 向磁盘的文件中写入一个页,如果超过文件大小则会在最末尾加
func (this *DiskManager) WritePage(pageId msg.PageId, page *PageManager.Page) (int, error) {
	offset := pageId * msg.PageSize
	_, err := this.fp.Seek(int64(offset), 0)
	if err != nil {
		return msg.Success, err
	}
	data := make([]byte, msg.PageSize)
	// 按照页的每个属性进行写入
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 0, utils.Int2Bytes(int(page.GetPageId())))
	if err != nil {
		return msg.Success, err
	}
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 4, utils.Int2Bytes(int(page.GetNextID())))
	if err != nil {
		return msg.Success, err
	}
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 8, utils.Int2Bytes(page.GetPinCount()))
	if err != nil {
		return msg.Success, err
	}
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 12, utils.Uint162Bytes(page.GetHeaderPos()))
	if err != nil {
		return msg.Success, err
	}
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 14, utils.Uint162Bytes(page.GetTailPos()))
	if err != nil {
		return msg.Success, err
	}
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 16, utils.Bool2Bytes(page.IsDirty()))
	if err != nil {
		return msg.Success, err
	}
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 17, page.GetData())
	if err != nil {
		return msg.Success, err
	}
	_, err = this.fp.Write(data)
	if err != nil {
		return msg.Success, err
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

// ReadPage 从页中的pageId表示偏移量中读出一页
func (this *DiskManager) ReadPage(pageId msg.PageId) (PageManager.Page, error) {
	offset := pageId * msg.PageSize
	_, err := this.fp.Seek(int64(offset), 0)
	page := PageManager.Page{}
	if err != nil {
		return page, err
	}
	//data := make([]byte, dataTypes.PageSize)
	data := make([]byte, 4096)
	_, err = this.fp.Read(data)
	if err != nil {
		return page, err
	}
	var readData []byte
	readData, err = utils.ReadBytesFromPosition(data, 0, 4)
	page.SetPageId(msg.PageId(utils.Bytes2Int(readData)))
	readData, err = utils.ReadBytesFromPosition(data, 4, 4)
	page.SetPinCount(utils.Bytes2Int(readData))
	readData, err = utils.ReadBytesFromPosition(data, 8, 2)
	page.SetHeaderPos(utils.Bytes2Uint16(readData))
	readData, err = utils.ReadBytesFromPosition(data, 10, 2)
	page.SetTailPos(utils.Bytes2Uint16(readData))
	readData, err = utils.ReadBytesFromPosition(data, 12, 1)
	page.SetDirty(utils.Bytes2Bool(readData))
	values, err := utils.ReadBytesFromPosition(data, 13, msg.PageRemainSize)
	page.SetData(values)
	log.Println(msg.SuccessWritePage(int(pageId)))
	return page, nil
}

func (this *DiskManager) FindPageIdByName(name string) (msg.PageId, error) {
	readPos := msg.PageHeadSize
	for {
		_, err := this.fp.Seek(int64(readPos), 0)
		if err != nil {
			return 0, err
		}
		readName := make([]byte, 10)
		_, err = this.fp.Read(readName)
		if err != nil {
			return 0, err
		}
		readName = utils.RemoveTrailingNullBytes(readName)
		if strings.Compare(name, string(readName)) == 0 {
			_, err := this.fp.Seek(-msg.PageHeadSize-10, io.SeekCurrent)
			if err != nil {
				return 0, err
			}
			bytes := make([]byte, msg.IntSize)
			_, err = this.fp.Read(bytes)
			if err != nil {
				return 0, err
			}
			return msg.PageId(utils.Bytes2Int(bytes)), nil
		}
		readPos += msg.PageSize
	}
}

func (this *DiskManager) GetPageById(pageid msg.PageId) (PageManager.Page, error) {
	_, err := this.fp.Seek(int64(pageid*msg.PageSize), 0)
	if err != nil {
		return PageManager.Page{}, err
	}
	pageBytes := make([]byte, msg.PageSize)
	_, err = this.fp.Read(pageBytes)
	if err != nil {
		return PageManager.Page{}, err
	}
	id := utils.Bytes2Int(pageBytes[:4])
	count := utils.Bytes2Int(pageBytes[4:8])
	headPos := utils.Bytes2Uint16(pageBytes[8:10])
	tailPos := utils.Bytes2Uint16(pageBytes[10:12])
	isDirty := utils.Bytes2Bool(pageBytes[12:13])
	bytes := pageBytes[13:]
	page := PageManager.Page{}
	page.SetPageId(msg.PageId(id))
	page.SetPinCount(count)
	page.SetHeaderPos(headPos)
	page.SetTailPos(tailPos)
	page.SetDirty(isDirty)
	page.SetData(bytes)
	return page, nil
}

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
