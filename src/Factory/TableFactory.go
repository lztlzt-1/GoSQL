package Factory

import (
	"GoSQL/src/Records"
	"GoSQL/src/storage/diskMgr"
)

func LoadTableByName(name string, diskManager *diskMgr.DiskManager, tableList *[]*Records.Table) (*Records.Table, error) {
	table := Records.Table{}
	pageId, err := diskManager.FindPageIdByName(name)
	table.PageId = pageId
	if err != nil {
		return nil, err
	}
	page, err := diskManager.GetPageById(pageId)
	if err != nil {
		return nil, err
	}
	err = table.LoadDataFromPage(page)
	if err != nil {
		return nil, err
	}
	*tableList = append(*tableList, &table)
	return &table, nil
}
