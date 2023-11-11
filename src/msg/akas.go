package msg

const (
	Success = iota
	BucketIsFull
	NotFound
	AlreadyExist
	NotFoundEvictable
	CannotBeEvict
)

const (
	intType = iota
	floatType
	stringType
	boolType
)

type FrameId int
type PageId int

type TimeType int64
type ReplacerSize uint8

const CapacityLru = 7
const CapacityLruTime = 3
const CapacityBucket = 8

const TableNameLength = 10
const RecordNameLength = 20
