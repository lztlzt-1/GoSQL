package pageMgr

import (
	"GoSQL/src/msg"
	"GoSQL/src/storage/diskMgr"
	"GoSQL/src/utils"
	"errors"
	"log"
)

// InitPage 起始页，记录了一些初始化的重要参数，这里修改的话diskManager的创建函数也需要相应修改
type InitPage struct {
	magic      string
	initPageID msg.PageId
}

func GetInitPage() InitPage {
	this := InitPage{}
	magic, err := diskMgr.GlobalDiskManager.GetData(0, msg.MagicSize)
	if err != nil || string(magic) != "MagicGoSQL" {
		log.Fatal(errors.New("error: it's not a GoSQL file"))
	}
	this.magic = string(magic)
	initIDBytes, err := diskMgr.GlobalDiskManager.GetData(msg.MagicSize, msg.IntSize)
	if err != nil {
		return InitPage{}
	}
	if err != nil {
		log.Fatal(errors.New("error: it's not a GoSQL file"))
	}
	id := utils.Bytes2Int(initIDBytes)
	this.initPageID = msg.PageId(id)
	return this
}

func (this *InitPage) SetInitPageToDisk() error {
	bytes := make([]byte, 0, msg.PageSize)
	bytes = append(bytes, []byte(this.magic)...)
	bytes = append(bytes, utils.Int2Bytes(int(this.initPageID))...)
	bytes = utils.FixSliceLength(bytes, msg.PageSize)
	err := diskMgr.GlobalDiskManager.WriteData(bytes)
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
