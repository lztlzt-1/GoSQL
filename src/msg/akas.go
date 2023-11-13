package msg

const PageSize = 4096
const PageRemainSize = 4083
const TupleSlotSize = 2
const (
	IntSize    = 4
	BoolSize   = 1
	FloatSize  = 4
	StringSize = 255
	pageIdSize = 4
	ErrorType  = -1
)
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
const CapacityLru = 7
const CapacityLruTime = 3
const CapacityBucket = 8
const TableNameLength = 20
const RecordNameLength = 20
const RecordTypeSize = 10
const MaxStringLength = 255
const PageHeadSize = 13
const MagicSize = 10

type FrameId int
type PageId int
type TimeType int64
type ReplacerSize uint8
