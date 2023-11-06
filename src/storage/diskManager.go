package storage

import (
	"GoSQL/src/dataTypes"
	"GoSQL/src/msg"
	"GoSQL/src/utils"
	"errors"
	"log"
	"os"
)

type diskManager struct {
	fp *os.File // 存储DiskManager指向的文件
}

func NewDiskManager(filePath string) (*diskManager, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0660)
	if err != nil {
		return nil, errors.New(msg.Nofile(filePath))
	}
	return &diskManager{
		fp: file,
	}, nil
}

// GetData 从偏移量offset的地址下读取length长度数据
func (this *diskManager) GetData(offset int64, length int) ([]byte, error) {
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
func (this *diskManager) WritePage(pageId dataTypes.PageId, page *Page) (int, error) {
	offset := pageId * dataTypes.PageSize
	_, err := this.fp.Seek(int64(offset), 0)
	if err != nil {
		return 0, err
	}
	data := make([]byte, dataTypes.PageSize)
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 0, utils.Int2Bytes(page.pageId))
	if err != nil {
		return 0, err
	}
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 4, utils.Int2Bytes(page.pinCount))
	if err != nil {
		return 0, err
	}
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 8, utils.Uint162Bytes(page.pageHeadPos))
	if err != nil {
		return 0, err
	}
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 10, utils.Uint162Bytes(page.pageTailPos))
	if err != nil {
		return 0, err
	}
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 12, utils.Bool2Bytes(page.isDirty))
	if err != nil {
		return 0, err
	}
	data, err = utils.InsertAndReplaceAtIndex[byte](data, 13, page.data)
	if err != nil {
		return 0, err
	}
	_, err = this.fp.Write(data)
	if err != nil {
		return 0, err
	}
	log.Println(msg.SuccessWritePage(int(pageId)))
	err = this.fp.Sync()
	if err != nil {
		return 0, err
	}
	return len(page.data), nil
	// 可能需要刷新同步数据到磁盘
}

// ReadPage 从页中的pageId表示偏移量中读出一页
func (this *diskManager) ReadPage(pageId dataTypes.PageId) (Page, error) {
	offset := pageId * dataTypes.PageSize
	_, err := this.fp.Seek(int64(offset), 0)
	page := Page{}
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
	page.pageId = utils.Bytes2Int(readData)
	readData, err = utils.ReadBytesFromPosition(data, 4, 4)
	page.pinCount = utils.Bytes2Int(readData)
	readData, err = utils.ReadBytesFromPosition(data, 8, 2)
	page.pageHeadPos = utils.BytesToUint16(readData)
	readData, err = utils.ReadBytesFromPosition(data, 10, 2)
	page.pageTailPos = utils.BytesToUint16(readData)
	readData, err = utils.ReadBytesFromPosition(data, 12, 1)
	page.isDirty = utils.Bytes2Bool(readData)
	page.data, err = utils.ReadBytesFromPosition(data, 13, dataTypes.PageRemainSize)
	log.Println(msg.SuccessWritePage(int(pageId)))
	return page, nil
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
