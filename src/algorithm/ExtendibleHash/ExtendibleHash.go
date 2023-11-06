package ExtendibleHash

type extendibleHash[K int | byte] struct {
	buckets     []uint64
	globalDepth uint32
	bucketSize  uint8
}

func NewExtendibleHash[K int | byte | string, V int | byte | string]() {
	//extendibleHash[]{}
}
