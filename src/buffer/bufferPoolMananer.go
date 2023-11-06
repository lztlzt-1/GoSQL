package buffer

import "GoSQL/src/storage"

type bufferPoolManager struct {
	pages []storage.Page
}

func NewBufferPoolManager() {

}
