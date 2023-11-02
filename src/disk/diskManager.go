package disk

import (
	"GoSQL/src/dataTypes"
	"GoSQL/src/msg"
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
func (this *diskManager) WritePage(pageId dataTypes.PageId, pageData []byte) (int, error) {
	if len(pageData) > dataTypes.PageSize {
		return 0, errors.New("error: Page is too large")
	}
	offset := pageId * dataTypes.PageSize
	_, err := this.fp.Seek(int64(offset), 0)
	if err != nil {
		return 0, err
	}
	_, err = this.fp.Write(pageData)
	if err != nil {
		return 0, err
	}
	log.Println(msg.SuccessWritePage(int(pageId)))
	err = this.fp.Sync()
	if err != nil {
		return 0, err
	}
	return len(pageData), nil
	// 可能需要刷新同步数据到磁盘
}

func (this *diskManager) ReadPage(pageId dataTypes.PageId, pageData []byte) {
	offset := pageId * dataTypes.PageSize
	_, err := this.fp.Seek(int64(offset), 0)
	if err != nil {
		return 0, err
	}
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
