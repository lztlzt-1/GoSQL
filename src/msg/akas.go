package msg

const PageSize = 4096
const PageRemainSize = 4079
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
const DiskBucketSize = 8
const PageHeadSize = 17
const MagicSize = 10
const TableHeadSize = 40

type FrameId int
type PageId int

const PageIDSize = 4

type TimeType int64
type ReplacerSize uint8

const PageTableStart = 1
