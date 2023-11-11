package Records

import (
	"GoSQL/src/dataTypes"
	"GoSQL/src/msg"
	"GoSQL/src/structType"
	"GoSQL/src/utils"
	"errors"
	"strconv"
	"strings"
)

type Column struct {
	Name    string // 最多RecordNameLength长度
	ItsType string
}

type Table struct {
	Name       string // 最多TableNameLength长度
	Length     int
	ColumnSize int
	RecordSize int
	Column     []Column
	Records    []structType.Record
}

func NewTable(name string, str string) (*Table, error) {
	list := strings.Split(str, " ")
	if len(list)&1 != 0 {
		return nil, errors.New("invalid string, please check")
	}
	var column []Column
	recordSize := 0
	for i := 0; i < len(list); i++ {
		name := list[i]
		if len(name) > msg.TableNameLength {
			return nil, errors.New("table name is too large")
		}
		itsType := list[i+1]
		i++
		if utils.JudgeSize(itsType) == dataTypes.ErrorType {
			return nil, errors.New("invalid data type, please check")
		}
		recordSize += utils.JudgeSize(itsType)
		column = append(column, Column{Name: name, ItsType: itsType})
	}
	return &Table{Name: name, ColumnSize: len(column), Column: column, Length: 0, RecordSize: recordSize}, nil
}

func (this *Table) Insert(str string) error {
	items := strings.Split(str, " ")
	if len(items) != len(this.Column) {
		return errors.New("error: While inserting into Table, count is not same")
	}
	record := structType.Record{}
	//将所有传入的值转化成对应value，并检查错误
	for i := 0; i < len(items); i++ {
		switch this.Column[i].ItsType {
		case "int":
			d, err := strconv.Atoi(items[i])
			if err != nil {
				return err
			}
			record.Value = append(record.Value, d)
		case "bool":
			if items[i] == "true" {
				record.Value = append(record.Value, true)
			} else if items[i] == "false" {
				record.Value = append(record.Value, false)
			} else {
				return errors.New("error: While inserting into Table, expect true or false")
			}
		case "string":
			record.Value = append(record.Value, items[i])
		}
	}
	this.Records = append(this.Records, record)
	this.Length++
	return nil
}

func (this *Table) queryidx(key string) (int, error) {
	for i := 0; i < len(this.Column); i++ {
		if this.Column[i].Name == key {
			return i, nil
		}
	}
	return 0, errors.New("error: invalid column name")
}

// Query 这个查询属于比较底层的，所以可以通过前面的步骤过滤到提供两个list,表示每一个key对应的value是数组里的值则拿出
func (this *Table) Query(key []string, value []any) ([]structType.Record, error) {
	n := len(this.Records) // 总共查询记录的个数
	var queryRecords []structType.Record
	idx, err := this.queryidx(key[0]) // column[idx]表示要查询的记录值
	if err != nil {
		return nil, err
	}
	for i := 0; i < n; i++ {
		if this.Records[i].Value[idx] == value[0] {
			queryRecords = append(queryRecords, this.Records[i])
		}
	}
	for j := 1; j < len(value); j++ {
		n := len(queryRecords)
		var localRecords []structType.Record
		idx, err := this.queryidx(key[j]) // column[idx]表示要查询的记录值
		if err != nil {
			return nil, err
		}
		for i := 0; i < n; i++ {
			if queryRecords[i].Value[idx] == value[j] {
				localRecords = append(localRecords, queryRecords[i])
			}
		}
		queryRecords = localRecords
	}
	return queryRecords, nil
}

// Update 这个查询属于比较底层的，所以可以通过前面的步骤过滤到提供两个list
func (this *Table) Update(key []string, value []any, updateKey []string, updateValue []any) error {
	n := len(this.Records) // 总共查询记录的个数
	var queryPos []int
	idx, err := this.queryidx(key[0]) // column[idx]表示要查询的记录值
	if err != nil {
		return err
	}
	for i := 0; i < n; i++ {
		if this.Records[i].Value[idx] == value[0] {
			queryPos = append(queryPos, i)
		}
	}
	for j := 1; j < len(value); j++ {
		n := len(queryPos)
		var localPos []int
		idx, err := this.queryidx(key[j]) // column[idx]表示要查询的记录值
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			if this.Records[queryPos[i]].Value[idx] == value[j] {
				localPos = append(localPos, queryPos[i])
			}
		}
		queryPos = localPos
	}
	n = len(queryPos)
	for j := 0; j < len(updateValue); j++ {
		queryidx, err := this.queryidx(updateKey[j])
		if err != nil {
			return err
		}
		for i := 0; i < n; i++ {
			this.Records[queryPos[i]].Value[queryidx] = updateValue[j]
		}
	}
	return nil
}

// Delete 这个查询属于比较底层的，所以可以通过前面的步骤过滤到提供两个list
func (this *Table) Delete(keys []string, values []any) error {
	if len(keys) != len(values) {
		return errors.New("error: key and value slices must have the same length")
	}

	for i := 0; i < len(this.Records); i++ {
		record := this.Records[i]
		match := true
		for j := 0; j < len(keys); j++ {
			key := keys[j]
			value := values[j]
			// 查找键在列中的索引
			index, err := this.queryidx(key)
			if err != nil {
				return err
			}
			// 检查记录中的值是否匹配
			if record.Value[index] != value {
				match = false
				break
			}
		}
		if match {
			this.Records = append(this.Records[:i], this.Records[i+1:]...)
			this.Length--
			i--
		}
	}
	return nil
}

func (this *Table) ToDisk() error {
	//var pages []storage.Page
	var bytes []byte
	name := make([]byte, 0, 10)
	name = append(name, []byte(this.Name)...)
	name = utils.FixSliceLength(name, 10).([]byte)
	bytes = append(bytes, name...)
	temp := utils.Int2Bytes(this.Length)
	bytes = append(bytes, temp...)
	temp = utils.Int2Bytes(this.ColumnSize)
	bytes = append(bytes, temp...)
	temp = utils.Int2Bytes(this.RecordSize)
	bytes = append(bytes, temp...)
	for i := 0; i < this.ColumnSize; i++ {
		columeBytes := make([]byte, 0, 30)
		columeBytes = append(columeBytes, []byte(this.Column[i].Name)...)
		utils.FixSliceLength(columeBytes, 20)
		columeBytes = append(columeBytes, []byte(this.Column[i].ItsType)...)
		utils.FixSliceLength(columeBytes, 30)
		bytes = append(bytes, columeBytes...)
	}
	for i := 0; i < this.Length; i++ {
		recordBytes := make([]byte, 0, this.RecordSize)
		re
	}
	print(bytes)
	return nil
}
