package storage

type PageTable struct {
	page Page
}

func (this *PageTable) InsertRecord(key []byte, value []byte) {

}

func (this *PageTable) WriteData(data []byte) {

}

func (this *PageTable) GetEmptyPos() int {

	return 1
}
