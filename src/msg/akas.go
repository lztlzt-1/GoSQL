package msg

const PageSize = 4096
const PageRemainSize = 4086
const TupleSlotSize = 2
const (
	IntSize    = 4
	BoolSize   = 1
	FloatSize  = 4
	StringSize = 255
	DoubleSize = 8
	LongSize   = 8
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
const BufferCapacityLruTime = 3
const BufferBucketSize = 2
const PageTableCapacityBucket = 8
const TableNameLength = 20
const RecordNameLength = 20
const RecordTypeSize = 10
const MaxStringLength = 255
const DiskBucketSize = 2
const PageHeadSize = 10
const MagicSize = 10
const TableHeadSize = 32
const FreeSpaceSizeInPageTable = 2
const PageIDSize = 4
const PageTableStart = 1

const FreeSpaceSizeInTable = 2

type FreeSpaceTypeInTable int16
type FreeSpaceTypeInPageTable int16
type FrameId int
type PageId int
type TimeType int64
type ReplacerSize uint8
